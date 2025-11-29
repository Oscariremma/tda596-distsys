package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	defaultWorkerPort      = "1235"
	defaultCoordinatorAddr = "127.0.0.1:1234"
	tempWorkDir            = "."
)

// getWorkerPort returns the worker port from env or default.
func getWorkerPort() string {
	if port := os.Getenv("MR_WORKER_PORT"); port != "" {
		return port
	}
	return defaultWorkerPort
}

// getCoordinatorAddr returns the coordinator address from env or default.
func getCoordinatorAddr() string {
	if addr := os.Getenv("MR_COORDINATOR_ADDR"); addr != "" {
		return addr
	}
	return defaultCoordinatorAddr
}

// ByKey implements sort.Interface for sorting KeyValue slices by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// KeyValue represents a key-value pair emitted by Map functions.
type KeyValue struct {
	Key   string
	Value string
}

// ihash returns a hash for the given key, used for partitioning keys to reducers.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// WorkerInstance represents a worker node in the MapReduce system.
type WorkerInstance struct {
	WorkerId     uint32
	ReducerCount uint32
	Coordinator  CoordinatorRpc
	localRpc     *WorkerRpc
	mapf         func(string, string) []KeyValue
	reducef      func(string, []string) string
}

// NewWorkerInstance creates and registers a new worker with the coordinator.
func NewWorkerInstance(coordinatorAddr string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) (*WorkerInstance, error) {
	coordinatorRpc, err := ConnectToCoordinator(coordinatorAddr)
	if err != nil {
		return nil, err
	}

	localRpc, err := startWorkerRpcServer()
	if err != nil {
		return nil, err
	}

	var registerWorkerReply RegisterWorkerReply
	err = coordinatorRpc.RegisterWorker(RegisterWorkerArgs{
		LocalRpcPort: localRpc.port,
	}, &registerWorkerReply)
	if err != nil {
		return nil, err
	}

	localRpc.localWorkerId = registerWorkerReply.WorkerId

	return &WorkerInstance{
		WorkerId:     registerWorkerReply.WorkerId,
		Coordinator:  coordinatorRpc,
		localRpc:     localRpc,
		mapf:         mapf,
		reducef:      reducef,
		ReducerCount: registerWorkerReply.ReducerCount,
	}, nil
}

// Worker is the main entry point for a MapReduce worker.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	instance, err := NewWorkerInstance(getCoordinatorAddr(), mapf, reducef)
	if err != nil {
		log.Fatalf("Failed to create WorkerInstance: %v", err)
	}

	log.Printf("Worker %d started", instance.WorkerId)
	instance.ClearOldWorkerFiles()

	for {
		var reply GetWorkTaskReply
		err = instance.Coordinator.GetWorkTask(&reply, instance.WorkerId)
		if err != nil {
			log.Fatalf("Failed to get work task: %v", err)
		}
		switch reply.Type {
		case ExitTask:
			log.Printf("Worker %d exiting", instance.WorkerId)
			return
		case TaskWait:
			time.Sleep(reply.WaitTask.SleepTime)
		case TaskMap:
			instance.ProcessMapTask(reply.MapTask)
		case TaskReduce:
			instance.ProcessReduceTask(reply.ReduceTask)
		default:
			log.Fatalf("Unknown task type %v", reply.Type)
		}
	}
}

// ProcessMapTask executes a map task and reports results to the coordinator.
func (wi *WorkerInstance) ProcessMapTask(task *MapTask) {
	mapResult := wi.mapf(task.FileName, task.Content)
	wi.saveMapResult(mapResult)

	var reportMapTaskReply ReportMapTaskReply
	err := wi.Coordinator.ReportMapTask(ReportMapTaskArgs{
		TaskId:   task.TaskId,
		WorkerId: wi.WorkerId,
	}, &reportMapTaskReply)
	if err != nil {
		log.Fatalf("Failed to report map task: %v", err)
	}
}

// saveMapResult partitions map results by reducer and writes them to temp files.
func (wi *WorkerInstance) saveMapResult(mapRes []KeyValue) {
	files := make([]*os.File, wi.ReducerCount)

	for reducer := 0; reducer < int(wi.ReducerCount); reducer++ {
		fileName := fmt.Sprintf("worker-%d-map-result-for-reducer-%d.tmp", wi.WorkerId, reducer)
		file, err := os.OpenFile(filepath.Join(tempWorkDir, fileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("Failed to create map result file %s: %v", fileName, err)
		}
		files[reducer] = file
	}

	for _, kv := range mapRes {
		reducer := ihash(kv.Key) % int(wi.ReducerCount)
		if _, err := fmt.Fprintf(files[reducer], "%s\t%s\n", kv.Key, kv.Value); err != nil {
			log.Fatalf("Failed to write to map result file for reducer %d: %v", reducer, err)
		}
	}

	for _, file := range files {
		if err := file.Close(); err != nil {
			log.Fatalf("Failed to close map result file: %v", err)
		}
	}
}

// ProcessReduceTask fetches data from other workers and executes a reduce task.
func (wi *WorkerInstance) ProcessReduceTask(task *ReduceTask) {
	data := wi.fetchReduceData(task)
	if data == nil {
		return // Error already reported
	}

	localData, err := readMapResultForReducer(wi.WorkerId, task.ReducerId)
	if err != nil {
		log.Printf("Failed to read local map result for reducer %d: %v", task.ReducerId, err)
		wi.reportFailedReduce(task.ReducerId)
		return
	}

	data = append(data, localData...)
	sort.Sort(ByKey(data))
	reducedData := wi.executeReduce(data)

	var reportReduceTaskReply ReportReduceTaskReply
	err = wi.Coordinator.ReportReduceTask(ReportReduceTaskArgs{
		Id:        task.ReducerId,
		Success:   true,
		KeyValues: reducedData,
		WorkerId:  wi.WorkerId,
	}, &reportReduceTaskReply)
	if err != nil {
		log.Fatalf("Failed to report reduce task: %v", err)
	}
}

// fetchReduceData fetches intermediate data from other workers for a reduce task.
// Returns nil if there was an error (already reported to coordinator).
func (wi *WorkerInstance) fetchReduceData(task *ReduceTask) []KeyValue {
	data := make([]KeyValue, 0)

	for _, source := range task.DataSources {
		otherWorkerRpc, err := ConnectToOtherWorker(source.Endpoint)
		if err != nil {
			wi.reportFailedReduce(task.ReducerId)
			return nil
		}

		var fetchDataReply FetchDataForReducerReply
		err = otherWorkerRpc.FetchDataForReducer(FetchDataForReducerArgs{
			ReducerId:        task.ReducerId,
			ExpectedWorkerId: source.WorkerId,
		}, &fetchDataReply)
		if err != nil {
			wi.reportFailedReduce(task.ReducerId)
			return nil
		}

		data = append(data, fetchDataReply.KeyValues...)
	}

	return data
}

// executeReduce applies the reduce function to grouped key-value pairs.
func (wi *WorkerInstance) executeReduce(data []KeyValue) []KeyValue {
	reducedData := make([]KeyValue, 0)

	i := 0
	for i < len(data) {
		// Find all values for the same key
		j := i + 1
		for j < len(data) && data[j].Key == data[i].Key {
			j++
		}

		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, data[k].Value)
		}

		output := wi.reducef(data[i].Key, values)
		reducedData = append(reducedData, KeyValue{Key: data[i].Key, Value: output})
		i = j
	}

	return reducedData
}

// reportFailedReduce notifies the coordinator that a reduce task failed.
func (wi *WorkerInstance) reportFailedReduce(reducerId uint32) {
	var reportReduceTaskReply ReportReduceTaskReply
	err := wi.Coordinator.ReportReduceTask(ReportReduceTaskArgs{
		Id:        reducerId,
		WorkerId:  wi.WorkerId,
		Success:   false,
		KeyValues: nil,
	}, &reportReduceTaskReply)
	if err != nil {
		log.Fatalf("Failed to report failed reduce task: %v", err)
	}
}

// ClearOldWorkerFiles removes stale temp files from previous worker runs.
func (wi *WorkerInstance) ClearOldWorkerFiles() error {
	dirContent, err := os.ReadDir(tempWorkDir)
	if err != nil {
		return err
	}

	workerFileRegex, err := regexp.Compile(fmt.Sprintf("worker-%d-map-result-for-reducer-\\d+\\.tmp", wi.WorkerId))
	if err != nil {
		return err
	}

	for _, file := range dirContent {
		if file.IsDir() {
			continue
		}
		if workerFileRegex.MatchString(file.Name()) {
			if err := os.Remove(filepath.Join(tempWorkDir, file.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// RpcClient wraps an RPC client connection.
type RpcClient struct {
	conn *rpc.Client
}

// connectToRpc establishes an RPC connection to the given address.
func connectToRpc(addr string) (RpcClient, error) {
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return RpcClient{}, fmt.Errorf("rpc.Dial err: %w", err)
	}
	return RpcClient{conn: c}, nil
}

// Close closes the RPC client connection.
func (c *RpcClient) Close() error {
	return c.conn.Close()
}

// CoordinatorRpc provides RPC methods to communicate with the coordinator.
type CoordinatorRpc struct {
	rpcClient *RpcClient
}

// RegisterWorker registers this worker with the coordinator.
func (c *CoordinatorRpc) RegisterWorker(args RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	return c.rpcClient.conn.Call("ConnCoordinator.RegisterWorker", args, reply)
}

// GetWorkTask requests a new task from the coordinator.
func (c *CoordinatorRpc) GetWorkTask(reply *GetWorkTaskReply, workerId uint32) error {
	args := GetWorkTaskArgs{WorkerId: workerId}
	return c.rpcClient.conn.Call("ConnCoordinator.GetWorkTask", args, reply)
}

// ReportMapTask reports completion of a map task to the coordinator.
func (c *CoordinatorRpc) ReportMapTask(args ReportMapTaskArgs, reply *ReportMapTaskReply) error {
	return c.rpcClient.conn.Call("ConnCoordinator.ReportMapTask", args, reply)
}

// ReportReduceTask reports completion of a reduce task to the coordinator.
func (c *CoordinatorRpc) ReportReduceTask(args ReportReduceTaskArgs, reply *ReportReduceTaskReply) error {
	return c.rpcClient.conn.Call("ConnCoordinator.ReportReduceTask", args, reply)
}

// ConnectToCoordinator establishes an RPC connection to the coordinator.
func ConnectToCoordinator(addr string) (CoordinatorRpc, error) {
	rpcClient, err := connectToRpc(addr)
	if err != nil {
		return CoordinatorRpc{}, err
	}
	return CoordinatorRpc{rpcClient: &rpcClient}, nil
}

// OtherWorkerRpc provides RPC methods to communicate with other workers.
type OtherWorkerRpc struct {
	rpcClient *RpcClient
}

// FetchDataForReducer fetches intermediate data for a reduce task from another worker.
func (c *OtherWorkerRpc) FetchDataForReducer(args FetchDataForReducerArgs, reply *FetchDataForReducerReply) error {
	return c.rpcClient.conn.Call("WorkerRpc.FetchDataForReducer", args, reply)
}

// ConnectToOtherWorker establishes an RPC connection to another worker.
func ConnectToOtherWorker(addr string) (OtherWorkerRpc, error) {
	rpcClient, err := connectToRpc(addr)
	if err != nil {
		return OtherWorkerRpc{}, err
	}
	return OtherWorkerRpc{rpcClient: &rpcClient}, nil
}

// WorkerRpc handles RPC requests from other workers.
type WorkerRpc struct {
	port          string
	localWorkerId uint32
}

// startWorkerRpcServer starts the RPC server for worker-to-worker communication.
func startWorkerRpcServer() (*WorkerRpc, error) {
	workerRpc := &WorkerRpc{}

	port := getWorkerPort()
	l, err := net.Listen("tcp", ":"+port)
	if err != nil {
		l, err = net.Listen("tcp", ":0")
		if err != nil {
			return nil, fmt.Errorf("WorkerRpc net.Listen err: %w", err)
		}
	}

	workerRpc.port = fmt.Sprintf("%d", l.Addr().(*net.TCPAddr).Port)

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				continue
			}

			rpcServer := rpc.NewServer()
			rpcServer.Register(workerRpc)
			go rpcServer.ServeConn(conn)
		}
	}()

	return workerRpc, nil
}

// FetchDataForReducer handles RPC requests from other workers for intermediate data.
func (c *WorkerRpc) FetchDataForReducer(args FetchDataForReducerArgs, reply *FetchDataForReducerReply) error {
	if args.ExpectedWorkerId != c.localWorkerId {
		return fmt.Errorf("worker %d received request intended for worker %d", c.localWorkerId, args.ExpectedWorkerId)
	}

	kvs, err := readMapResultForReducer(c.localWorkerId, args.ReducerId)
	if err != nil {
		return err
	}
	reply.KeyValues = kvs
	return nil
}

// PingWorker responds to health check pings from the coordinator.
func (c *WorkerRpc) PingWorker(args PingWorkerArgs, reply *PingWorkerReply) error {
	reply.WorkerId = c.localWorkerId
	return nil
}

// readMapResultForReducer reads intermediate map results from a temp file for a specific reducer.
func readMapResultForReducer(workerId, reducerId uint32) ([]KeyValue, error) {
	fileName := fmt.Sprintf("worker-%d-map-result-for-reducer-%d.tmp", workerId, reducerId)

	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return []KeyValue{}, nil
	}

	data, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	kvs := make([]KeyValue, 0)
	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "\t", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid line format in file %s: %s", fileName, line)
		}
		kvs = append(kvs, KeyValue{Key: parts[0], Value: parts[1]})
	}
	return kvs, nil
}
