package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

type TaskType int

const (
	ExitTask TaskType = iota
	TaskWait
	TaskMap
	TaskReduce
)

type MapTask struct {
	TaskId   uint32
	FileName string
	Content  string
}

type DataSource struct {
	WorkerId uint32
	Endpoint string
}

type ReduceTask struct {
	ReducerId   uint32
	DataSources []DataSource
}

type WaitTask struct {
	SleepTime time.Duration
}

type GetWorkTaskArgs struct {
	WorkerId uint32
}

type GetWorkTaskReply struct {
	Type       TaskType
	MapTask    *MapTask
	ReduceTask *ReduceTask
	WaitTask   *WaitTask
}

type ReportReduceTaskArgs struct {
	WorkerId  uint32
	Id        uint32
	Success   bool
	KeyValues []KeyValue
}

type ReportReduceTaskReply struct {
}

type ReportMapTaskArgs struct {
	WorkerId uint32
	TaskId   uint32
}

type RegisterWorkerArgs struct {
	LocalRpcPort string
}

type ReportMapTaskReply struct {
}

type RegisterWorkerReply struct {
	WorkerId     uint32
	ReducerCount uint32
}

// Worker-worker RCP definitions.

type FetchDataForReducerArgs struct {
	ReducerId        uint32
	ExpectedWorkerId uint32
}

type FetchDataForReducerReply struct {
	KeyValues []KeyValue
}

type PingWorkerArgs struct {
}

type PingWorkerReply struct {
	WorkerId uint32
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
