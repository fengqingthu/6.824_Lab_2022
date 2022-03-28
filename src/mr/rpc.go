package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// arguments for workers to request tasks
type RequestTaskArgs struct {
	WorkerID string
}

type RequestTaskReply struct {
	AssignedTask Task
}

// arguments for workers to turn in finished tasks
type FinishedTaskArgs struct {
	WorkerID     string
	FinishedTask Task // note here returns a new task instance (as value), not the assigned one
}

type FinishedTaskReply struct {
	Message string
}

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

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
