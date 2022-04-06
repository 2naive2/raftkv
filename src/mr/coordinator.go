package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	lg "github.com/sirupsen/logrus"
)

var (
	DefaultTimeout = time.Second * 10
)

var (
	ErrNoMatchingState = errors.New("no matching state")
)

type Coordinator struct {
	ReduceTaskNumber    int64
	MapInputSplit       chan string
	MapStateMutex       sync.Mutex
	RunningMapTaskState map[string]*time.Timer
}

func (c *Coordinator) AssignTask(req *AssignTaskRequest, reply *AssignTaskReply) error {
	// map task not finished yet,all worker will process
	// map task
	if len(c.MapInputSplit) > 0 {
		taskName := <-c.MapInputSplit
		reply.TaskType = TaskTypeMap
		reply.FileName = taskName
		reply.ReduceTaskNumber = c.ReduceTaskNumber

		lg.Infof("job %v assigned", taskName)

		go func() {
			timeout := time.NewTimer(DefaultTimeout)
			c.RunningMapTaskState[taskName] = timeout
			<-timeout.C
			lg.Warn("task timeout for job : " + taskName)
			c.MapInputSplit <- taskName
		}()

	}
	return nil
}

func (c *Coordinator) TaskDone(req *TaskDoneRequest, reply *TaskDoneReply) error {
	if req.TaskType == TaskTypeMap {
		timeout := c.RunningMapTaskState[req.FileName]
		if timeout == nil {
			lg.Error("no state kept for job :%v" + req.FileName)
			return ErrNoMatchingState
		}
		stop := timeout.Stop()
		if !stop {
			lg.Infof("cancel job %v failed", req.FileName)
		} else {
			lg.Infof("job :%v canceled successfully", req.FileName)
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.ReduceTaskNumber = int64(nReduce)
	c.MapInputSplit = make(chan string, len(files))
	for _, file := range files {
		c.MapInputSplit <- file
	}

	c.MapStateMutex = sync.Mutex{}
	c.RunningMapTaskState = make(map[string]*time.Timer)

	c.server()
	return &c
}
