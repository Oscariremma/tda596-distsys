package mr

//
// RPC definitions for coordinator-worker and worker-worker communication.
//

import (
	"os"
	"strconv"
	"time"
)

// TaskType represents the type of task assigned to a worker.
type TaskType int

const (
	ExitTask   TaskType = iota // Worker should exit
	TaskWait                   // Worker should wait before requesting again
	TaskMap                    // Worker should execute a map task
	TaskReduce                 // Worker should execute a reduce task
)

// Coordinator-Worker RPC definitions

// MapTask contains information for executing a map task.
type MapTask struct {
	TaskId   uint32
	FileName string
	Content  string
}

// DataSource identifies where a worker can fetch intermediate data for a reduce task.
type DataSource struct {
	WorkerId uint32
	Endpoint string
}

// ReduceTask contains information for executing a reduce task.
type ReduceTask struct {
	ReducerId   uint32
	DataSources []DataSource
}

// WaitTask instructs a worker to wait before requesting work again.
type WaitTask struct {
	SleepTime time.Duration
}

// GetWorkTaskArgs contains the arguments for requesting a task.
type GetWorkTaskArgs struct {
	WorkerId uint32
}

// GetWorkTaskReply contains the coordinator's response to a task request.
type GetWorkTaskReply struct {
	Type       TaskType
	MapTask    *MapTask
	ReduceTask *ReduceTask
	WaitTask   *WaitTask
}

// ReportReduceTaskArgs contains the arguments for reporting a reduce task result.
type ReportReduceTaskArgs struct {
	WorkerId        uint32
	Id              uint32
	Success         bool
	KeyValues       []KeyValue
	FailedWorkerIds []uint32 // Worker IDs from which data could not be fetched
}

// ReportReduceTaskReply is the response to a reduce task report (currently empty).
type ReportReduceTaskReply struct{}

// ReportMapTaskArgs contains the arguments for reporting a map task completion.
type ReportMapTaskArgs struct {
	WorkerId uint32
	TaskId   uint32
}

// ReportMapTaskReply is the response to a map task report (currently empty).
type ReportMapTaskReply struct{}

// RegisterWorkerArgs contains the arguments for worker registration.
type RegisterWorkerArgs struct {
	LocalRpcPort string
}

// RegisterWorkerReply contains the coordinator's response to worker registration.
type RegisterWorkerReply struct {
	WorkerId     uint32
	ReducerCount uint32
}

// Worker-Worker RPC definitions

// FetchDataForReducerArgs contains the arguments for fetching intermediate data.
type FetchDataForReducerArgs struct {
	ReducerId        uint32
	ExpectedWorkerId uint32
}

// FetchDataForReducerReply contains the intermediate data for a reduce task.
type FetchDataForReducerReply struct {
	KeyValues []KeyValue
}

// PingWorkerArgs contains the arguments for a health check ping (currently empty).
type PingWorkerArgs struct{}

// PingWorkerReply contains the response to a health check ping.
type PingWorkerReply struct {
	WorkerId uint32
}

// coordinatorSock returns a unique UNIX-domain socket name for the coordinator.
// Used for test script compatibility.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
