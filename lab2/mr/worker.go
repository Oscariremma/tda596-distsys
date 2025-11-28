package mr

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "sort"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

const PREFFERED_DEFAULT_PORT = "1235"
const TEMP_WORK_DIR = "."

type WorkerInstance struct {
	WorkerId     uint32
	ReducerCount uint32
	Coordinator  CoordinatorRpc
	localRpc     *WorkerRpc
	mapf         func(string, string) []KeyValue
	reducef      func(string, []string) string
}

func NewWorkerInstance(coordinatorAddr string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) (*WorkerInstance, error) {
	coordinatorRpc, err := ConnectToCoordinator(coordinatorAddr)
	if err != nil {
		return nil, err
	}

	localRpc, err := StartWorkerRpcServer()
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	instance, err := NewWorkerInstance("127.0.0.1:1234", mapf, reducef)
	if err != nil {
		log.Fatalf("Failed to create WorkerInstance: %v", err)
	}

	instance.ClearOldWorkerFiles()

	for {
		var reply GetWorkTaskReply
		err = instance.Coordinator.GetWorkTask(&reply, instance.WorkerId)
		if err != nil {
			log.Fatalf("Failed to get work task: %v", err)
		}
		switch reply.Type {
		case ExitTask:
			println("Exiting task")
			return
		case TaskWait:
			fmt.Printf("Waiting for %d seconds\n", reply.WaitTask.SleepTime/time.Second)
			time.Sleep(reply.WaitTask.SleepTime)
			break
		case TaskMap:
			fmt.Printf("Processing map task %d\n", reply.MapTask.TaskId)
			instance.ProcessMapTask(reply.MapTask)
			break
		case TaskReduce:
			fmt.Printf("Processing reduce task %d\n", reply.ReduceTask.ReducerId)
			instance.ProcessReduceTask(reply.ReduceTask)
			break
		default:
			log.Fatalf("Unknown task type %v", reply.Type)
		}
	}
}

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

func (wi *WorkerInstance) saveMapResult(mapRes []KeyValue) {
	files := make([]*os.File, wi.ReducerCount)

	for reducer := 0; reducer < int(wi.ReducerCount); reducer++ {
		fileName := fmt.Sprintf("worker-%d-map-result-for-reducer-%d.tmp", wi.WorkerId, reducer)
		file, err := os.OpenFile(filepath.Join(TEMP_WORK_DIR, fileName), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("Failed to create map result file %s: %v", fileName, err)
		}
		files[reducer] = file
	}

	for _, kv := range mapRes {
		reducer := ihash(kv.Key) % int(wi.ReducerCount)
		_, err := fmt.Fprintf(files[reducer], "%s\t%s\n", kv.Key, kv.Value)
		if err != nil {
			log.Fatalf("Failed to write to map result file for reducer %d: %v", reducer, err)
		}
	}

	for _, file := range files {
		err := file.Close()
		if err != nil {
			log.Fatalf("Failed to close map result file: %v", err)
		}
	}
}

func (wi *WorkerInstance) ProcessReduceTask(task *ReduceTask) {

	println("Reducer", task.ReducerId, "fetching data from other workers")
	otherWorkerRpcs := make([]OtherWorkerRpc, 0)

	for _, addr := range task.DataSources {
		otherWorkerRpc, err := ConnectToOtherWorker(addr)
		if err != nil {
			log.Printf("Failed to connect to other worker at %s: %v", addr, err)
			wi.ReportFailedReduce(task.ReducerId)
			return
		}
		otherWorkerRpcs = append(otherWorkerRpcs, otherWorkerRpc)
	}

	data := make([]KeyValue, 0)

	for _, otherWorkerRpc := range otherWorkerRpcs {
		var fetchDataReply FetchDataForReducerReply
		err := otherWorkerRpc.FetchDataForReducer(FetchDataForReducerArgs{
			ReducerId: task.ReducerId,
		}, &fetchDataReply)
		if err != nil {
			log.Printf("Failed to fetch data for reducer from other worker: %v", err)
			wi.ReportFailedReduce(task.ReducerId)
			return
		}
		data = append(data, fetchDataReply.KeyValues...)
	}

	localData, err := readMapResultForReducer(wi.WorkerId, task.ReducerId)

	if err != nil {
		log.Printf("Failed to read local map result for reducer %d: %v", task.ReducerId, err)
		wi.ReportFailedReduce(task.ReducerId)
		return
	}

	data = append(data, localData...)
	println("Total key-values to reduce:", len(data))
	sort.Sort(ByKey(data))

	reducedData := make([]KeyValue, 0)

	i := 0
	for i < len(data) {
		j := i + 1
		for j < len(data) && data[j].Key == data[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, data[k].Value)
		}
		output := wi.reducef(data[i].Key, values)

		// this is the correct format for each line of Reduce output.
		reducedData = append(reducedData, KeyValue{Key: data[i].Key, Value: output})
		i = j
	}

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

	println("Reduce task reported successfully")

}

func (wi *WorkerInstance) ReportFailedReduce(reducerId uint32) {
	var reportReduceTaskReply ReportReduceTaskReply
	err := wi.Coordinator.ReportReduceTask(ReportReduceTaskArgs{
		Id:        reducerId,
		WorkerId:  wi.WorkerId,
		Success:   false,
		KeyValues: []KeyValue{},
	}, &reportReduceTaskReply)

	if err != nil {
		log.Fatalf("Failed to report failed reduce task: %v", err)
	}
}

func (wi *WorkerInstance) ClearOldWorkerFiles() error {
	dirContent, err := os.ReadDir(TEMP_WORK_DIR)

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
			err := os.Remove(filepath.Join(TEMP_WORK_DIR, file.Name()))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type RpcClient struct {
	conn *rpc.Client
}

func ConnectToRpc(addr string) (RpcClient, error) {
	c, err := rpc.Dial("tcp", addr)

	if err != nil {
		log.Fatalf("rpc.Dial err: %v", err)
	}

	return RpcClient{conn: c}, nil
}

func (c *RpcClient) Close() error {
	return c.conn.Close()
}

type CoordinatorRpc struct {
	rpcClient *RpcClient
}

func (c *CoordinatorRpc) RegisterWorker(args RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	return c.rpcClient.conn.Call("ConnCoordinator.RegisterWorker", args, reply)
}

func (c *CoordinatorRpc) GetWorkTask(reply *GetWorkTaskReply, workerId uint32) error {
	args := GetWorkTaskArgs{
		WorkerId: workerId,
	}
	return c.rpcClient.conn.Call("ConnCoordinator.GetWorkTask", args, reply)
}

func (c *CoordinatorRpc) ReportMapTask(args ReportMapTaskArgs, reply *ReportMapTaskReply) error {
	return c.rpcClient.conn.Call("ConnCoordinator.ReportMapTask", args, reply)
}

func (c *CoordinatorRpc) ReportReduceTask(args ReportReduceTaskArgs, reply *ReportReduceTaskReply) error {
	return c.rpcClient.conn.Call("ConnCoordinator.ReportReduceTask", args, reply)
}

func ConnectToCoordinator(addr string) (CoordinatorRpc, error) {
	rpcClient, err := ConnectToRpc(addr)
	if err != nil {
		return CoordinatorRpc{}, err
	}
	return CoordinatorRpc{rpcClient: &rpcClient}, nil
}

type OtherWorkerRpc struct {
	rpcClient *RpcClient
}

func (c *OtherWorkerRpc) FetchDataForReducer(args FetchDataForReducerArgs, reply *FetchDataForReducerReply) error {
	return c.rpcClient.conn.Call("WorkerRpc.FetchDataForReducer", args, reply)
}

func ConnectToOtherWorker(addr string) (OtherWorkerRpc, error) {
	rpcClient, err := ConnectToRpc(addr)
	if err != nil {
		return OtherWorkerRpc{}, err
	}
	return OtherWorkerRpc{rpcClient: &rpcClient}, nil
}

type WorkerRpc struct {
	port          string
	localWorkerId uint32
}

func StartWorkerRpcServer() (*WorkerRpc, error) {
	workerRpc := &WorkerRpc{}

	l, err := net.Listen("tcp", ":"+PREFFERED_DEFAULT_PORT)
	if err != nil {
		fmt.Println("Failed to start WorkerRpc server on preferred port, trying random port:", err)
		l, err = net.Listen("tcp", ":0")
		if err != nil {
			return nil, fmt.Errorf("WorkerRpc net.Listen err: %v", err)
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

func (c *WorkerRpc) FetchDataForReducer(args FetchDataForReducerArgs, reply *FetchDataForReducerReply) error {
	kvs, err := readMapResultForReducer(c.localWorkerId, args.ReducerId)
	if err != nil {
		return err
	}
	reply.KeyValues = kvs

	return nil
}

func (c *WorkerRpc) PingWorker(args PingWorkerArgs, reply *PingWorkerReply) error {
	reply.WorkerId = c.localWorkerId
	return nil
}

func readMapResultForReducer(workerId uint32, reducerId uint32) ([]KeyValue, error) {
	fileName := fmt.Sprintf("worker-%d-map-result-for-reducer-%d.tmp", workerId, reducerId)

	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		println("Map result file does not exist:", fileName)
		return []KeyValue{}, nil
	}

	data, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	kvs := make([]KeyValue, 0)
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
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
