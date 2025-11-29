package mr

import (
	"fmt"
	"io"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"

const TASK_TIMEOUT = 10 * time.Second

type MapTaskStatus struct {
	taskId    uint32
	completed bool
	workerId  uint32
	assigned  bool
}

type ReduceTaskStatus struct {
	completed bool
	workerId  uint32
	assigned  bool
}

type RemoteWorker struct {
	id          uint32
	rpcEndpoint string
}

type Coordinator struct {
	coordinatorMutex sync.Mutex

	currentWorkerId uint32
	workers         map[uint32]*RemoteWorker

	inputFiles           []string
	nReduce              uint32
	mapTasks             map[uint32]*MapTaskStatus
	completedMapTasks    uint32
	reduceTasks          map[uint32]*ReduceTaskStatus
	completedReduceTasks uint32

	availableMapTasks    chan uint32
	availableReduceTasks chan uint32

	mapTimeoutCancel    map[uint32]chan struct{}
	reduceTimeoutCancel map[uint32]chan struct{}

	newWorkers map[uint32]struct{}

	workDone atomic.Bool
}

func (c *ConnCoordinator) RegisterWorker(args RegisterWorkerArgs, reply *RegisterWorkerReply) error {

	c.coordinator.coordinatorMutex.Lock()
	defer c.coordinator.coordinatorMutex.Unlock()

	var workerId = c.coordinator.currentWorkerId
	c.coordinator.currentWorkerId++

	c.coordinator.workers[workerId] = &RemoteWorker{
		id:          workerId,
		rpcEndpoint: hostFromConn(c) + ":" + args.LocalRpcPort,
	}

	c.coordinator.newWorkers[workerId] = struct{}{}

	reply.WorkerId = workerId
	reply.ReducerCount = c.coordinator.nReduce

	return nil
}

func (c *ConnCoordinator) GetWorkTask(args *GetWorkTaskArgs, reply *GetWorkTaskReply) error {
	c.coordinator.coordinatorMutex.Lock()
	defer c.coordinator.coordinatorMutex.Unlock()

	if c.coordinator.workers[args.WorkerId] == nil {
		println("Unregistered worker", args.WorkerId, "requested work; telling it to exit")
		reply.Type = ExitTask
		return nil
	}

	println("Worker", args.WorkerId, "requested work")
	if c.coordinator.currentWorkerId < 1 {
		reply.Type = TaskWait
		reply.WaitTask = &WaitTask{
			SleepTime: 1 * time.Second,
		}
		println("Worker", args.WorkerId, "waiting for more workers to register")
		return nil
	}

	_, thisWorkerIsNew := c.coordinator.newWorkers[args.WorkerId]
	if !thisWorkerIsNew {
		var newWorkersAvailable = len(c.coordinator.newWorkers) > 0
		if newWorkersAvailable {
			reply.Type = TaskWait
			reply.WaitTask = &WaitTask{
				SleepTime: 1 * time.Second,
			}
			// Give new workers a chance to get tasks first
			return nil
		}
	}

	select {
	case id := <-c.coordinator.availableMapTasks:
		task := c.coordinator.mapTasks[id]

		task.workerId = args.WorkerId
		task.assigned = true

		reply.Type = TaskMap

		file, err := os.Open(c.coordinator.inputFiles[id])
		if err != nil {
			log.Fatalf("cannot open %v", c.coordinator.inputFiles[id])
		}
		defer file.Close()

		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", c.coordinator.inputFiles[id])
		}

		reply.MapTask = &MapTask{
			TaskId:   id,
			FileName: c.coordinator.inputFiles[id],
			Content:  string(content),
		}
		println("Assigned map task", id, "to worker", args.WorkerId)
		c.coordinator.startMapTimeout(id, TASK_TIMEOUT)
		return nil
	default:
		// No reduce tasks available
		break
	}

	if c.coordinator.completedMapTasks != uint32(len(c.coordinator.mapTasks)) {
		reply.Type = TaskWait
		reply.WaitTask = &WaitTask{
			SleepTime: 1 * time.Second,
		}
		println("Worker", args.WorkerId, "waiting for map tasks to complete")
		return nil
	}

	select {
	case id := <-c.coordinator.availableReduceTasks:
		task := c.coordinator.reduceTasks[id]

		task.workerId = args.WorkerId
		task.assigned = true

		reply.Type = TaskReduce

		var dataSources []DataSource
		for workerId, worker := range c.coordinator.workers {
			if workerId == args.WorkerId {
				continue
			}
			if _, ok := c.coordinator.newWorkers[workerId]; ok {
				if id == 0 || id == 2 || id == 5 || id == 9 {
					log.Printf("Excluding data source from new worker %d for reduce task %d which will run on worker %d", workerId, id, args.WorkerId)
				}
				continue
			}

			if id == 0 || id == 2 || id == 5 || id == 9 {
				log.Printf("Including data source from worker %d for reduce task %d which will run on worker %d", workerId, id, args.WorkerId)
			}

			dataSources = append(dataSources, DataSource{
				WorkerId: workerId,
				Endpoint: worker.rpcEndpoint,
			})
		}

		if len(dataSources) == 0 && len(c.coordinator.newWorkers) != (len(c.coordinator.workers)-1) {
			log.Fatalf("No data sources available for reduce task %d assigned to worker %d", id, args.WorkerId)
		}

		reply.ReduceTask = &ReduceTask{
			ReducerId:   id,
			DataSources: dataSources,
		}
		c.coordinator.startReduceTimeout(id, TASK_TIMEOUT)
		println("Assigned reduce task", id, "to worker", args.WorkerId)
		return nil
	default:
		// No reduce tasks available
		break
	}

	if !c.coordinator.workDone.Load() {
		reply.Type = TaskWait
		reply.WaitTask = &WaitTask{
			SleepTime: 1 * time.Second,
		}
		println("Worker", args.WorkerId, "waiting for reduce tasks to complete")
		return nil
	}

	println("All tasks completed. Instructing worker", args.WorkerId, "to exit.")
	reply.Type = ExitTask
	return nil

}

func (c *ConnCoordinator) ReportMapTask(args *ReportMapTaskArgs, reply *ReportMapTaskReply) error {

	c.coordinator.coordinatorMutex.Lock()
	defer c.coordinator.coordinatorMutex.Unlock()

	task := c.coordinator.mapTasks[args.TaskId]
	if !task.assigned || task.workerId != args.WorkerId {
		println("Worker", args.WorkerId, "attempted to report map task", args.TaskId, "which is not assigned to it; ignoring report")
		return nil
	}

	c.coordinator.cancelMapTimeout(args.TaskId)

	if task.completed {
		// Task was already marked completed
		println("Map task", args.TaskId, "was already reported completed; ignoring duplicate report")
		return nil
	}
	task.completed = true
	c.coordinator.completedMapTasks++
	delete(c.coordinator.newWorkers, args.WorkerId)
	println("Map task", args.TaskId, "reported completed")
	return nil
}

func (c *ConnCoordinator) ReportReduceTask(args *ReportReduceTaskArgs, reply *ReportReduceTaskReply) error {
	c.coordinator.coordinatorMutex.Lock()
	defer c.coordinator.coordinatorMutex.Unlock()

	task := c.coordinator.reduceTasks[args.Id]

	if !task.assigned || task.workerId != args.WorkerId {
		println("Worker", args.WorkerId, "attempted to report reduce task", args.Id, "which is not assigned to it; ignoring report")
		return nil
	}

	c.coordinator.cancelReduceTimeout(args.Id)

	if args.Success == false {
		// Mark task as not completed and re-queue
		println("Reduce task", args.Id, "reported failed; re-queuing")
		task.workerId = 0
		task.assigned = false
		task.completed = false
		c.coordinator.availableReduceTasks <- args.Id
		return nil
	}

	if task.completed {
		// Task was already marked completed
		println("Reduce task", args.Id, "was already reported completed; ignoring duplicate report")
		return nil
	}

	task.completed = true
	c.coordinator.completedReduceTasks++
	println("Reduce task", args.Id, "reported completed")

	outFile, err := os.OpenFile("mr-out-0", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer outFile.Close()

	for _, kv := range args.KeyValues {
		_, err := fmt.Fprintf(outFile, "%s %s\n", kv.Key, kv.Value)
		if err != nil {
			log.Fatalf("Failed to write to output file: %v", err)
		}
	}

	if c.coordinator.completedReduceTasks == c.coordinator.nReduce {
		println("All reduce tasks completed; marking work done")
		c.coordinator.workDone.Store(true)
	}

	return nil

}

func (c *Coordinator) startMapTimeout(taskId uint32, duration time.Duration) {
	cancel := make(chan struct{})
	c.mapTimeoutCancel[taskId] = cancel

	go func() {
		select {
		case <-time.After(duration):
			c.coordinatorMutex.Lock()
			task := c.mapTasks[taskId]
			// Atomically clear workerId if task not completed
			if !task.completed {
				println("Map task", taskId, "timed out; re-queuing")
				task.workerId = 0
				task.assigned = false
				c.availableMapTasks <- taskId
			}
			delete(c.mapTimeoutCancel, taskId)
			c.coordinatorMutex.Unlock()
		case <-cancel:
			return
		}
	}()
}

func (c *Coordinator) cancelMapTimeout(taskId uint32) {
	// Note: caller should hold coordinatorMutex
	if cancel, ok := c.mapTimeoutCancel[taskId]; ok {
		delete(c.mapTimeoutCancel, taskId)
		close(cancel)
	}
}

func (c *Coordinator) startReduceTimeout(taskId uint32, duration time.Duration) {
	cancel := make(chan struct{})
	c.reduceTimeoutCancel[taskId] = cancel

	go func() {
		select {
		case <-time.After(duration):
			c.coordinatorMutex.Lock()
			task := c.reduceTasks[taskId]
			if !task.completed {
				println("Reduce task", taskId, "timed out; re-queuing")
				task.workerId = 0
				task.assigned = false
				c.availableReduceTasks <- taskId
			}
			delete(c.reduceTimeoutCancel, taskId)
			c.coordinatorMutex.Unlock()
		case <-cancel:
			return
		}
	}()
}

func (c *Coordinator) cancelReduceTimeout(taskId uint32) {
	// Note: caller should hold coordinatorMutex
	if cancel, ok := c.reduceTimeoutCancel[taskId]; ok {
		delete(c.reduceTimeoutCancel, taskId)
		close(cancel)
	}
}

func hostFromConn(conn net.Conn) string {
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		return conn.RemoteAddr().String()
	}
	return host
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	l, e := net.Listen("tcp", ":1234") // Use TCP instead of Unix socket
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				continue
			}

			// Wrap connection to capture remote address
			wrappedConn := &ConnCoordinator{
				Conn:        conn,
				coordinator: c,
			}

			rpcServer := rpc.NewServer()
			rpcServer.Register(wrappedConn)

			go rpcServer.ServeConn(wrappedConn)
		}
	}()
}

type ConnCoordinator struct {
	net.Conn
	coordinator *Coordinator
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.workDone.Load() {
		ret = true
		time.Sleep(2 * time.Second) // wait for workers to exit
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inputFiles:           files,
		nReduce:              uint32(nReduce),
		mapTasks:             make(map[uint32]*MapTaskStatus),
		reduceTasks:          make(map[uint32]*ReduceTaskStatus),
		availableMapTasks:    make(chan uint32, len(files)),
		availableReduceTasks: make(chan uint32, nReduce),
		workers:              make(map[uint32]*RemoteWorker),
		completedMapTasks:    0,
		completedReduceTasks: 0,
		currentWorkerId:      0,
		newWorkers:           make(map[uint32]struct{}),
		mapTimeoutCancel:     make(map[uint32]chan struct{}),
		reduceTimeoutCancel:  make(map[uint32]chan struct{}),
	}

	for i := range files {
		c.mapTasks[uint32(i)] = &MapTaskStatus{}
		c.availableMapTasks <- uint32(i)
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[uint32(i)] = &ReduceTaskStatus{}
		c.availableReduceTasks <- uint32(i)
	}

	go c.workerHealthCheck()

	os.Create(coordinatorSock()) // Ensure the socket file exists to not break test script that assumes its existence

	os.Remove("mr-out-0")
	c.server()
	return &c
}

func (w *RemoteWorker) PingWorker() (uint32, error) {
	client, err := rpc.Dial("tcp", w.rpcEndpoint)
	if err != nil {
		return 0, err
	}
	defer client.Close()

	var reply PingWorkerReply
	err = client.Call("WorkerRpc.PingWorker", &PingWorkerArgs{}, &reply)
	if err != nil {
		return 0, err
	}
	return reply.WorkerId, nil
}

func (c *Coordinator) workerHealthCheck() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C

		if c.workDone.Load() {
			// No need to check workers if work is done
			break
		}

		c.coordinatorMutex.Lock()
		for _, worker := range c.workers {
			go c.healthCheckSingleWorker(worker)
		}
		c.coordinatorMutex.Unlock()

		if c.workDone.Load() {
			// No need to check workers if work is done
			break
		}
	}
}

func (c *Coordinator) healthCheckSingleWorker(worker *RemoteWorker) {
	remoteId, err := worker.PingWorker()
	if c.workDone.Load() {
		// No need to anything if work is done
		return
	}
	if err != nil {
		println("Worker", worker.id, "is unresponsive; removing from worker list")
		c.handleDeadWorker(worker.id)
	}

	if worker.id != remoteId {
		println("Worker ID mismatch for worker at", worker.rpcEndpoint, "; Presuming old worker gone; removing from worker list")
		c.handleDeadWorker(worker.id)
	}
}

func (c *Coordinator) handleDeadWorker(workerId uint32) {
	c.newWorkers.Delete(workerId)
	for taskId, task := range c.mapTasks {
		workerPtr := task.workerId.Load()
		if workerPtr != nil && *workerPtr == workerId {
			println("Re-queuing map task", taskId, "from dead worker", workerId)
			if task.completed.Load() {
				c.completedMapTasks.Add(^uint32(0)) // decrement completed count. I love Go atomic operations...
			}
			c.cancelMapTimeout(taskId)
			task.workerId.Store(nil)
			task.completed.Store(false)
			c.availableMapTasks <- taskId
		}
	}

	for taskId, task := range c.reduceTasks {
		workerPtr := task.workerId.Load()
		if workerPtr != nil && *workerPtr == workerId && !task.completed.Load() {
			println("Re-queuing reduce task", taskId, "from dead worker", workerId)
			c.cancelReduceTimeout(taskId)
			task.workerId.Store(nil)
			c.availableReduceTasks <- taskId
		}
	}

	c.workers.Delete(workerId)
}
