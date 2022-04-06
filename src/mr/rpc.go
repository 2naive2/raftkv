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

type TaskType int64

var (
	TaskTypeMap    = TaskType(0)
	TaskTypeReduce = TaskType(1)
)

type AssignTaskRequest struct {
}

type AssignTaskReply struct {
	TaskType TaskType
	FileName string
}

type TaskDoneRequest struct {
	TaskType TaskType
	FileName string
}

type TaskDoneRepply struct {
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
