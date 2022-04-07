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
	"github.com/spf13/cast"
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

	ReduceInputSplit       chan int64
	ReduceStateMutex       sync.Mutex
	RunningReduceTaskState map[string]*time.Timer

	SliceMutex         sync.Mutex
	ReduceTaskPosition map[string][]string
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

	} else if len(c.ReduceInputSplit) > 0 {
		taskName := <-c.ReduceInputSplit
		reply.TaskType = TaskTypeReduce
		reply.CurrentReduceTaskNumber = taskName
		reply.ReduceFileNames = c.ReduceTaskPosition[cast.ToString(taskName)]

		go func() {
			timeout := time.NewTimer(DefaultTimeout)
			c.RunningReduceTaskState[cast.ToString(taskName)] = timeout
			<-timeout.C
			lg.Warnf("task timeout for job :%v", taskName)
			c.ReduceInputSplit <- taskName
		}()

	}
	return nil
}

func (c *Coordinator) TaskDone(req *TaskDoneRequest, reply *TaskDoneReply) error {
	if req.TaskType == TaskTypeMap {
		timeout := c.RunningMapTaskState[req.FileName]
		if timeout == nil {
			lg.Errorf("no state kept for job :%v", req.FileName)
			return ErrNoMatchingState
		}
		stop := timeout.Stop()
		if !stop {
			lg.Infof("cancel job %v failed", req.FileName)
		} else {
			lg.Infof("job :%v canceled successfully", req.FileName)
			for reduceNum, position := range req.ResultPosition {
				c.SliceMutex.Lock()
				c.ReduceTaskPosition[reduceNum] = append(c.ReduceTaskPosition[reduceNum], position)
				c.SliceMutex.Unlock()
			}
		}
		lg.Infof("reduce task :%v", c.ReduceTaskPosition)
	} else if req.TaskType == TaskTypeReduce {
		timeout := c.RunningReduceTaskState[req.FileName]
		if timeout == nil {
			lg.Errorf("no state kept for job :%v", req.FileName)
			return ErrNoMatchingState
		}
		stop := timeout.Stop()
		if !stop {
			lg.Infof("cancel job %v failed", req.FileName)
		} else {
			lg.Infof("job :%v canceled successfully", req.FileName)
		}
		lg.Infof("reduce task:%v done", req.FileName)
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
	return len(c.MapInputSplit) == 0 && len(c.ReduceInputSplit) == 0
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
	c.ReduceTaskPosition = make(map[string][]string)
	for _, file := range files {
		c.MapInputSplit <- file
	}
	c.MapStateMutex = sync.Mutex{}
	c.RunningMapTaskState = make(map[string]*time.Timer)

	c.ReduceInputSplit = make(chan int64, c.ReduceTaskNumber)
	for i := 0; i < int(c.ReduceTaskNumber); i++ {
		c.ReduceInputSplit <- int64(i)
	}
	c.RunningReduceTaskState = make(map[string]*time.Timer)
	c.SliceMutex = sync.Mutex{}
	c.ReduceStateMutex = sync.Mutex{}

	c.server()
	return &c
}
