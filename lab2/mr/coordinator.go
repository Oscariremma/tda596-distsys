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
import "sync/atomic"

const TASK_TIMEOUT = 10 * time.Second

type MapTaskStatus struct {
	taskId    uint32
	completed atomic.Bool
	workerId  atomic.Pointer[uint32]
}

type ReduceTaskStatus struct {
	completed atomic.Bool
	workerId  atomic.Pointer[uint32]
}

type RemoteWorker struct {
	id          uint32
	rpcEndpoint string
}

type Coordinator struct {
	currentWorkerId atomic.Uint32
	workers         sync.Map

	inputFiles           []string
	nReduce              uint32
	mapTasks             map[uint32]*MapTaskStatus
	completedMapTasks    atomic.Uint32
	reduceTasks          map[uint32]*ReduceTaskStatus
	completedReduceTasks atomic.Uint32

	availableMapTasks    chan uint32
	availableReduceTasks chan uint32

	mapTimeoutCancel    sync.Map
	reduceTimeoutCancel sync.Map

	newWorkers sync.Map

	workDone atomic.Bool
}

func (c *ConnCoordinator) RegisterWorker(args RegisterWorkerArgs, reply *RegisterWorkerReply) error {

	var workerId = c.coordinator.currentWorkerId.Add(1) - 1
	c.coordinator.workers.Store(workerId, &RemoteWorker{
		id:          workerId,
		rpcEndpoint: hostFromConn(c) + ":" + args.LocalRpcPort,
	})
	c.coordinator.newWorkers.Store(workerId, struct{}{})

	reply.WorkerId = workerId
	reply.ReducerCount = c.coordinator.nReduce

	return nil
}

func (c *ConnCoordinator) GetWorkTask(args *GetWorkTaskArgs, reply *GetWorkTaskReply) error {
	if _, workerExists := c.coordinator.workers.Load(args.WorkerId); !workerExists {
		println("Unregistered worker", args.WorkerId, "requested work; telling it to exit")
		reply.Type = ExitTask
		return nil
	}

	println("Worker", args.WorkerId, "requested work")
	if c.coordinator.currentWorkerId.Load() < 1 {
		reply.Type = TaskWait
		reply.WaitTask = &WaitTask{
			SleepTime: 1 * time.Second,
		}
		println("Worker", args.WorkerId, "waiting for more workers to register")
		return nil
	}

	if _, thisWorkerIsNew := c.coordinator.newWorkers.Load(args.WorkerId); !thisWorkerIsNew {
		var newWorkersAvailable = false
		c.coordinator.newWorkers.Range(func(key, value any) bool {
			newWorkersAvailable = true
			return false
		})
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

		task.workerId.Store(&args.WorkerId)

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

	if c.coordinator.completedMapTasks.Load() != uint32(len(c.coordinator.mapTasks)) {
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

		task.workerId.Store(&args.WorkerId)

		reply.Type = TaskReduce

		var dataSources []string
		c.coordinator.workers.Range(func(key, value any) bool {
			worker := value.(*RemoteWorker)
			if key.(uint32) == args.WorkerId {
				return true
			}
			if _, ok := c.coordinator.newWorkers.Load(key.(uint32)); ok {
				return true
			}

			dataSources = append(dataSources, worker.rpcEndpoint)
			return true
		})

		// Ensure no crashes causing map tasks to be "undone" at this point which could cause missing data
		if c.coordinator.completedMapTasks.Load() != uint32(len(c.coordinator.mapTasks)) {

			// Re-queue reduce task
			println("Map tasks incomplete after assigning reduce task; re-queuing reduce task", id)
			task.workerId.Store(nil)
			c.coordinator.availableReduceTasks <- id

			reply.Type = TaskWait
			reply.WaitTask = &WaitTask{
				SleepTime: 1 * time.Millisecond,
			}
			println("Worker", args.WorkerId, "waiting for map tasks to complete")
			return nil
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

	task := c.coordinator.mapTasks[args.TaskId]
	taskWorkerId := task.workerId.Load()
	if taskWorkerId == nil || *taskWorkerId != args.WorkerId {
		println("Worker", args.WorkerId, "attempted to report map task", args.TaskId, "which is not assigned to it; ignoring report")
		return nil
	}

	c.coordinator.cancelMapTimeout(args.TaskId)

	if !task.completed.CompareAndSwap(false, true) {
		// Task was already marked completed
		println("Map task", args.TaskId, "was already reported completed; ignoring duplicate report")
		return nil
	}
	c.coordinator.completedMapTasks.Add(1)
	c.coordinator.newWorkers.Delete(args.WorkerId)
	println("Map task", args.TaskId, "reported completed")
	return nil
}

func (c *ConnCoordinator) ReportReduceTask(args *ReportReduceTaskArgs, reply *ReportReduceTaskReply) error {
	task := c.coordinator.reduceTasks[args.Id]

	taskWorkerId := task.workerId.Load()
	if taskWorkerId == nil || *taskWorkerId != args.WorkerId {
		println("Worker", args.WorkerId, "attempted to report reduce task", args.Id, "which is not assigned to it; ignoring report")
		return nil
	}

	c.coordinator.cancelReduceTimeout(args.Id)

	if args.Success == false {
		// Mark task as not completed and re-queue
		println("Reduce task", args.Id, "reported failed; re-queuing")
		task.workerId.Store(nil)
		task.completed.Store(false)
		c.coordinator.availableReduceTasks <- args.Id
		return nil
	}

	if !task.completed.CompareAndSwap(false, true) {
		// Task was already marked completed
		println("Reduce task", args.Id, "was already reported completed; ignoring duplicate report")
		return nil
	}

	c.coordinator.completedReduceTasks.Add(1)
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

	if c.coordinator.completedReduceTasks.Load() == c.coordinator.nReduce {
		println("All reduce tasks completed; marking work done")
		c.coordinator.workDone.Store(true)
	}

	return nil

}

func (c *Coordinator) startMapTimeout(taskId uint32, duration time.Duration) {
	cancel := make(chan struct{})
	c.mapTimeoutCancel.Store(taskId, cancel)

	go func() {
		select {
		case <-time.After(duration):
			task := c.mapTasks[taskId]
			// Atomically clear workerId if task not completed
			if !task.completed.Load() {
				println("Map task", taskId, "timed out; re-queuing")
				task.workerId.Store(nil)
				c.availableMapTasks <- taskId
			}
			c.mapTimeoutCancel.Delete(taskId)
		case <-cancel:
			return
		}
	}()
}

func (c *Coordinator) cancelMapTimeout(taskId uint32) {
	if val, ok := c.mapTimeoutCancel.LoadAndDelete(taskId); ok {
		close(val.(chan struct{}))
	}
}

func (c *Coordinator) startReduceTimeout(taskId uint32, duration time.Duration) {
	cancel := make(chan struct{})
	c.reduceTimeoutCancel.Store(taskId, cancel)

	go func() {
		select {
		case <-time.After(duration):
			task := c.reduceTasks[taskId]
			if !task.completed.Load() {
				println("Reduce task", taskId, "timed out; re-queuing")
				task.workerId.Store(nil)
				c.availableReduceTasks <- taskId
			}
			c.reduceTimeoutCancel.Delete(taskId)
		case <-cancel:
			return
		}
	}()
}

func (c *Coordinator) cancelReduceTimeout(taskId uint32) {
	if val, ok := c.reduceTimeoutCancel.LoadAndDelete(taskId); ok {
		close(val.(chan struct{}))
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
		workers:              sync.Map{},
		completedMapTasks:    atomic.Uint32{},
		completedReduceTasks: atomic.Uint32{},
		currentWorkerId:      atomic.Uint32{},
		newWorkers:           sync.Map{},
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
		c.workers.Range(func(key, value any) bool {
			worker := value.(*RemoteWorker)
			remoteId, err := worker.PingWorker()
			if c.workDone.Load() {
				// No need to check workers if work is done
				return false
			}
			if err != nil {
				println("Worker", worker.id, "is unresponsive; removing from worker list")
				c.handleDeadWorker(worker.id)
			}

			if worker.id != remoteId {
				println("Worker ID mismatch for worker at", worker.rpcEndpoint, "; Presuming old worker gone; removing from worker list")
				c.handleDeadWorker(worker.id)
			}
			return true
		})

		if c.workDone.Load() {
			// No need to check workers if work is done
			break
		}
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
