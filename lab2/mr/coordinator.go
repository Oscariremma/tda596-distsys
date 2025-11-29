package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	taskTimeout            = 5 * time.Second
	defaultCoordinatorPort = "1234"
	workerWaitTime         = 500 * time.Millisecond // Time workers wait before retrying when no tasks available
	workerPingTimeout      = 1 * time.Second        // Timeout for pinging workers
)

// getCoordinatorPort returns the coordinator port from env or default.
func getCoordinatorPort() string {
	if port := os.Getenv("MR_COORDINATOR_PORT"); port != "" {
		return port
	}
	return defaultCoordinatorPort
}

// MapTaskStatus tracks the state of a single map task.
type MapTaskStatus struct {
	completed bool
	workerId  uint32
	assigned  bool
}

// ReduceTaskStatus tracks the state of a single reduce task.
type ReduceTaskStatus struct {
	completed bool
	workerId  uint32
	assigned  bool
}

// RemoteWorker represents a connected worker node.
type RemoteWorker struct {
	id          uint32
	rpcEndpoint string
}

// Coordinator manages map and reduce tasks across workers.
type Coordinator struct {
	mu sync.Mutex

	nextWorkerId uint32
	workers      map[uint32]*RemoteWorker

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

// logProgress logs the current progress of map and reduce tasks.
// Caller must hold mu.
func (c *Coordinator) logProgress(event string) {
	totalMap := uint32(len(c.mapTasks))
	totalReduce := c.nReduce
	pendingMap := totalMap - c.completedMapTasks
	pendingReduce := totalReduce - c.completedReduceTasks
	log.Printf("%s | Progress: Map %d/%d done (%d pending), Reduce %d/%d done (%d pending), Workers: %d",
		event, c.completedMapTasks, totalMap, pendingMap,
		c.completedReduceTasks, totalReduce, pendingReduce,
		len(c.workers))
}

// ConnCoordinator wraps a connection with coordinator reference for RPC handling.
type ConnCoordinator struct {
	net.Conn
	coordinator *Coordinator
}

// RegisterWorker registers a new worker and assigns it a unique ID.
func (c *ConnCoordinator) RegisterWorker(args RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	if c.coordinator.workDone.Load() {
		return fmt.Errorf("all work is allready done")
	}

	c.coordinator.mu.Lock()
	defer c.coordinator.mu.Unlock()

	workerId := c.coordinator.nextWorkerId
	c.coordinator.nextWorkerId++

	c.coordinator.workers[workerId] = &RemoteWorker{
		id:          workerId,
		rpcEndpoint: hostFromConn(c) + ":" + args.LocalRpcPort,
	}
	c.coordinator.newWorkers[workerId] = struct{}{}

	reply.WorkerId = workerId
	reply.ReducerCount = c.coordinator.nReduce

	c.coordinator.logProgress(fmt.Sprintf("Worker %d registered", workerId))
	return nil
}

// GetWorkTask assigns work to a requesting worker.
func (c *ConnCoordinator) GetWorkTask(args *GetWorkTaskArgs, reply *GetWorkTaskReply) error {
	c.coordinator.mu.Lock()

	// Check if worker is registered
	if c.coordinator.workers[args.WorkerId] == nil {
		c.coordinator.mu.Unlock()
		reply.Type = ExitTask
		return nil
	}

	// Wait for at least one worker to be registered
	if c.coordinator.nextWorkerId < 1 {
		c.coordinator.mu.Unlock()
		reply.Type = TaskWait
		reply.WaitTask = &WaitTask{SleepTime: workerWaitTime}
		return nil
	}

	// Give new workers priority for tasks
	_, thisWorkerIsNew := c.coordinator.newWorkers[args.WorkerId]
	if !thisWorkerIsNew && len(c.coordinator.newWorkers) > 0 {
		c.coordinator.mu.Unlock()
		reply.Type = TaskWait
		reply.WaitTask = &WaitTask{SleepTime: workerWaitTime}
		return nil
	}

	// Try to assign a map task
	select {
	case taskId := <-c.coordinator.availableMapTasks:
		return c.assignMapTask(taskId, args.WorkerId, reply)
	default:
	}

	// Check if all map tasks are complete
	if c.coordinator.completedMapTasks != uint32(len(c.coordinator.mapTasks)) {
		c.coordinator.mu.Unlock()
		reply.Type = TaskWait
		reply.WaitTask = &WaitTask{SleepTime: workerWaitTime}
		return nil
	}

	// Try to assign a reduce task
	select {
	case taskId := <-c.coordinator.availableReduceTasks:
		return c.assignReduceTask(taskId, args.WorkerId, reply)
	default:
	}

	// Check if all work is done
	if !c.coordinator.workDone.Load() {
		c.coordinator.mu.Unlock()
		reply.Type = TaskWait
		reply.WaitTask = &WaitTask{SleepTime: workerWaitTime}
		return nil
	}

	c.coordinator.mu.Unlock()
	reply.Type = ExitTask
	return nil
}

// assignMapTask assigns a map task to a worker and reads the input file.
// Caller must hold mu. This function releases mu before doing disk I/O.
func (c *ConnCoordinator) assignMapTask(taskId, workerId uint32, reply *GetWorkTaskReply) error {
	task := c.coordinator.mapTasks[taskId]
	task.workerId = workerId
	task.assigned = true

	fileName := c.coordinator.inputFiles[taskId]
	c.coordinator.startMapTimeout(taskId, taskTimeout)
	c.coordinator.mu.Unlock()

	// Perform disk I/O without holding the lock
	content, err := readInputFile(fileName)
	if err != nil {
		log.Fatalf("Cannot read %v: %v", fileName, err)
	}

	reply.Type = TaskMap
	reply.MapTask = &MapTask{
		TaskId:   taskId,
		FileName: fileName,
		Content:  content,
	}

	return nil
}

// assignReduceTask assigns a reduce task to a worker.
// Caller must hold mu. This function releases mu.
func (c *ConnCoordinator) assignReduceTask(taskId, workerId uint32, reply *GetWorkTaskReply) error {
	task := c.coordinator.reduceTasks[taskId]
	task.workerId = workerId
	task.assigned = true

	dataSources := c.coordinator.buildDataSources(workerId)

	if len(dataSources) == 0 && len(c.coordinator.newWorkers) != (len(c.coordinator.workers)-1) {
		c.coordinator.mu.Unlock()
		log.Fatalf("No data sources available for reduce task %d assigned to worker %d", taskId, workerId)
	}

	c.coordinator.startReduceTimeout(taskId, taskTimeout)
	c.coordinator.mu.Unlock()

	reply.Type = TaskReduce
	reply.ReduceTask = &ReduceTask{
		ReducerId:   taskId,
		DataSources: dataSources,
	}

	return nil
}

// buildDataSources creates the list of data sources for a reduce task.
// Caller must hold mu.
func (c *Coordinator) buildDataSources(assignedWorkerId uint32) []DataSource {
	var dataSources []DataSource

	for workerId, worker := range c.workers {
		if workerId == assignedWorkerId {
			continue
		}
		if _, isNew := c.newWorkers[workerId]; isNew {
			continue
		}
		dataSources = append(dataSources, DataSource{
			WorkerId: workerId,
			Endpoint: worker.rpcEndpoint,
		})
	}

	return dataSources
}

// ReportMapTask handles a worker's report of a completed map task.
func (c *ConnCoordinator) ReportMapTask(args *ReportMapTaskArgs, _ *ReportMapTaskReply) error {
	c.coordinator.mu.Lock()
	defer c.coordinator.mu.Unlock()

	task := c.coordinator.mapTasks[args.TaskId]
	if !task.assigned || task.workerId != args.WorkerId {
		return nil
	}

	c.coordinator.cancelMapTimeout(args.TaskId)

	if task.completed {
		return nil
	}

	task.completed = true
	c.coordinator.completedMapTasks++
	delete(c.coordinator.newWorkers, args.WorkerId)

	c.coordinator.logProgress(fmt.Sprintf("Map task %d completed", args.TaskId))
	return nil
}

// ReportReduceTask handles a worker's report of a completed reduce task.
func (c *ConnCoordinator) ReportReduceTask(args *ReportReduceTaskArgs, _ *ReportReduceTaskReply) error {
	c.coordinator.mu.Lock()

	task := c.coordinator.reduceTasks[args.Id]
	if !task.assigned || task.workerId != args.WorkerId {
		c.coordinator.mu.Unlock()
		return nil
	}

	c.coordinator.cancelReduceTimeout(args.Id)

	if !args.Success {
		task.workerId = 0
		task.assigned = false
		task.completed = false
		c.coordinator.logProgress(fmt.Sprintf("Reduce task %d failed, re-queuing", args.Id))

		// Handle failed workers immediately
		if len(args.FailedWorkerIds) > 0 {
			c.coordinator.mu.Unlock()
			// Mark failed workers as dead
			for _, failedWorkerId := range args.FailedWorkerIds {
				log.Printf("Worker %d reported that worker %d is unresponsive, marking as dead", args.WorkerId, failedWorkerId)
				c.coordinator.handleDeadWorker(failedWorkerId)
			}
			// The task will be re-queued by handleDeadWorker if it was assigned to a dead worker,
			// otherwise we need to re-queue it here
			c.coordinator.mu.Lock()
			if !c.coordinator.reduceTasks[args.Id].assigned {
				c.coordinator.mu.Unlock()
				c.coordinator.availableReduceTasks <- args.Id
				return nil
			}
			c.coordinator.mu.Unlock()
		} else {
			// No specific workers reported as failed, just re-queue the task
			c.coordinator.mu.Unlock()
			c.coordinator.availableReduceTasks <- args.Id
		}
		return nil
	}

	if task.completed {
		c.coordinator.mu.Unlock()
		return nil
	}

	task.completed = true
	c.coordinator.completedReduceTasks++
	allDone := c.coordinator.completedReduceTasks == c.coordinator.nReduce
	c.coordinator.logProgress(fmt.Sprintf("Reduce task %d completed", args.Id))
	c.coordinator.mu.Unlock()

	// Perform disk I/O without holding the lock
	if err := writeReduceOutput(args.KeyValues); err != nil {
		log.Fatalf("Failed to write output: %v", err)
	}

	if allDone {
		log.Println("All tasks completed!")
		c.coordinator.workDone.Store(true)
	}

	return nil
}

// readInputFile reads the content of a file for a map task.
func readInputFile(fileName string) (string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("Warning: failed to close file %s: %v", fileName, err)
		}
	}()

	content, err := io.ReadAll(file)
	if err != nil {
		return "", err
	}
	return string(content), nil
}

// writeReduceOutput appends reduce results to the output file.
func writeReduceOutput(keyValues []KeyValue) error {
	outFile, err := os.OpenFile("mr-out-0", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer func() {
		if err := outFile.Close(); err != nil {
			log.Printf("Warning: failed to close output file: %v", err)
		}
	}()

	for _, kv := range keyValues {
		if _, err := fmt.Fprintf(outFile, "%s %s\n", kv.Key, kv.Value); err != nil {
			return fmt.Errorf("failed to write to output file: %w", err)
		}
	}
	return nil
}

// startMapTimeout starts a timeout goroutine that will re-queue the task if not completed.
// Caller must hold mu.
func (c *Coordinator) startMapTimeout(taskId uint32, duration time.Duration) {
	cancel := make(chan struct{})
	c.mapTimeoutCancel[taskId] = cancel

	go func() {
		select {
		case <-time.After(duration):
			c.mu.Lock()
			defer c.mu.Unlock()

			task := c.mapTasks[taskId]
			if !task.completed {
				task.workerId = 0
				task.assigned = false
				c.availableMapTasks <- taskId
				c.logProgress(fmt.Sprintf("Map task %d timed out, re-queuing", taskId))
			}
			delete(c.mapTimeoutCancel, taskId)
		case <-cancel:
			return
		}
	}()
}

// cancelMapTimeout cancels a pending map task timeout.
// Caller must hold mu.
func (c *Coordinator) cancelMapTimeout(taskId uint32) {
	if cancel, ok := c.mapTimeoutCancel[taskId]; ok {
		delete(c.mapTimeoutCancel, taskId)
		close(cancel)
	}
}

// startReduceTimeout starts a timeout goroutine that will re-queue the task if not completed.
// Caller must hold mu.
func (c *Coordinator) startReduceTimeout(taskId uint32, duration time.Duration) {
	cancel := make(chan struct{})
	c.reduceTimeoutCancel[taskId] = cancel

	go func() {
		select {
		case <-time.After(duration):
			c.mu.Lock()
			task := c.reduceTasks[taskId]
			if !task.completed && task.assigned {
				workerId := task.workerId
				c.mu.Unlock()
				log.Printf("Reduce task %d timed out, treating worker %d as dead", taskId, workerId)
				c.handleDeadWorker(workerId)
			} else {
				c.mu.Unlock()
			}

			c.mu.Lock()
			delete(c.reduceTimeoutCancel, taskId)
			c.mu.Unlock()
		case <-cancel:
			return
		}
	}()
}

// cancelReduceTimeout cancels a pending reduce task timeout.
// Caller must hold mu.
func (c *Coordinator) cancelReduceTimeout(taskId uint32) {
	if cancel, ok := c.reduceTimeoutCancel[taskId]; ok {
		delete(c.reduceTimeoutCancel, taskId)
		close(cancel)
	}
}

// hostFromConn extracts the host address from a connection.
func hostFromConn(conn net.Conn) string {
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		return conn.RemoteAddr().String()
	}
	return host
}

// server starts the RPC server for coordinator.
func (c *Coordinator) server() {
	port := getCoordinatorPort()
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	log.Printf("Coordinator listening on port %s", port)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				continue
			}

			wrappedConn := &ConnCoordinator{
				Conn:        conn,
				coordinator: c,
			}

			rpcServer := rpc.NewServer()
			if err := rpcServer.Register(wrappedConn); err != nil {
				log.Printf("Warning: failed to register RPC handler: %v", err)
				if closeErr := conn.Close(); closeErr != nil {
					log.Printf("Warning: failed to close connection: %v", closeErr)
				}
				continue
			}
			go rpcServer.ServeConn(wrappedConn)
		}
	}()
}

// Done returns true if all MapReduce jobs have completed.
func (c *Coordinator) Done() bool {
	if c.workDone.Load() {
		return true
	}
	return false
}

// MakeCoordinator creates and initializes a new Coordinator.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inputFiles:           files,
		nReduce:              uint32(nReduce),
		mapTasks:             make(map[uint32]*MapTaskStatus),
		reduceTasks:          make(map[uint32]*ReduceTaskStatus),
		availableMapTasks:    make(chan uint32, len(files)),
		availableReduceTasks: make(chan uint32, nReduce),
		workers:              make(map[uint32]*RemoteWorker),
		newWorkers:           make(map[uint32]struct{}),
		mapTimeoutCancel:     make(map[uint32]chan struct{}),
		reduceTimeoutCancel:  make(map[uint32]chan struct{}),
	}

	// Initialize map tasks
	for i := range files {
		c.mapTasks[uint32(i)] = &MapTaskStatus{}
		c.availableMapTasks <- uint32(i)
	}

	// Initialize reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[uint32(i)] = &ReduceTaskStatus{}
		c.availableReduceTasks <- uint32(i)
	}

	go c.workerHealthCheck()

	// Ensure the socket file exists (for test script compatibility)
	if f, err := os.Create(coordinatorSock()); err != nil {
		log.Printf("Warning: failed to create socket file: %v", err)
	} else {
		if err := f.Close(); err != nil {
			log.Printf("Warning: failed to close socket file: %v", err)
		}
	}
	if err := os.Remove("mr-out-0"); err != nil && !os.IsNotExist(err) {
		log.Printf("Warning: failed to remove old output file: %v", err)
	}

	c.server()
	return &c
}

// PingWorker sends a ping to a worker and returns its ID.
// Uses workerPingTimeout for the connection and RPC call.
func (w *RemoteWorker) PingWorker() (uint32, error) {
	conn, err := net.DialTimeout("tcp", w.rpcEndpoint, workerPingTimeout)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = conn.Close()
	}()

	client := rpc.NewClient(conn)
	defer func() {
		_ = client.Close()
	}()

	// Set a deadline for the RPC call
	if err := conn.SetDeadline(time.Now().Add(workerPingTimeout)); err != nil {
		return 0, fmt.Errorf("failed to set deadline: %w", err)
	}

	var reply PingWorkerReply
	if err := client.Call("WorkerRpc.PingWorker", &PingWorkerArgs{}, &reply); err != nil {
		return 0, err
	}
	return reply.WorkerId, nil
}

// workerHealthCheck periodically checks if workers are still alive.
func (c *Coordinator) workerHealthCheck() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if c.workDone.Load() {
			return
		}

		c.mu.Lock()
		workers := make([]*RemoteWorker, 0, len(c.workers))
		for _, worker := range c.workers {
			workers = append(workers, worker)
		}
		c.mu.Unlock()

		for _, worker := range workers {
			go c.healthCheckSingleWorker(worker)
		}
	}
}

// healthCheckSingleWorker checks if a single worker is still responsive.
func (c *Coordinator) healthCheckSingleWorker(worker *RemoteWorker) {
	if c.workDone.Load() {
		return
	}

	remoteId, err := worker.PingWorker()
	if c.workDone.Load() {
		return
	}

	if err != nil {
		c.handleDeadWorker(worker.id)
		return
	}

	if worker.id != remoteId {
		c.handleDeadWorker(worker.id)
	}
}

// handleDeadWorker removes a dead worker and re-queues its tasks.
func (c *Coordinator) handleDeadWorker(workerId uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if worker still exists (may have already been removed)
	if _, exists := c.workers[workerId]; !exists {
		return
	}

	delete(c.newWorkers, workerId)

	mapTasksRequeued := 0
	reduceTasksRequeued := 0

	// Re-queue map tasks assigned to the dead worker
	for taskId, task := range c.mapTasks {
		if task.assigned && task.workerId == workerId {
			if task.completed {
				c.completedMapTasks--
			}
			c.cancelMapTimeout(taskId)
			task.workerId = 0
			task.assigned = false
			task.completed = false
			c.availableMapTasks <- taskId
			mapTasksRequeued++
		}
	}

	// Re-queue reduce tasks assigned to the dead worker
	for taskId, task := range c.reduceTasks {
		if task.assigned && task.workerId == workerId && !task.completed {
			c.cancelReduceTimeout(taskId)
			task.workerId = 0
			task.assigned = false
			c.availableReduceTasks <- taskId
			reduceTasksRequeued++
		}
	}

	delete(c.workers, workerId)

	c.logProgress(fmt.Sprintf("Worker %d lost (re-queued %d map, %d reduce tasks)",
		workerId, mapTasksRequeued, reduceTasksRequeued))
}
