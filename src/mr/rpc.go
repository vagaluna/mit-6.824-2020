package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskRPC struct {
	Phase Phase
	Index int
	Input string
}

type RequestTaskArgs struct {
	PrevTask *TaskRPC
	WorkerId int
}

type RequestTaskReply struct {
	Task *TaskRPC
}

type RegisterWorkerArgs struct {
	WorkerId int
}

type RegisterWorkerReply struct {
	MapCount int
	ReduceCount int
}

const NoMoreTaskError string = "No More Task"


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
