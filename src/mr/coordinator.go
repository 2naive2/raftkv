package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cast"
)

var (
	DefaultTimeout = time.Second * 10
)

var (
	ErrNoMatchingState = errors.New("no matching state")
)

type Coordinator struct {
	ReduceTaskNumber int64
	MapInputSplit    chan string
	// RunningMapTaskState map[string]*time.Timer
	RunningMapTaskState sync.Map
	RunningMapTask      int64
	RunningReduceTask   int64

	ReduceInputSplit       chan int64
	RunningReduceTaskState sync.Map

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
		atomic.AddInt64(&c.RunningMapTask, 1)

		go func() {
			timeout := time.NewTimer(DefaultTimeout)
			c.RunningMapTaskState.Store(taskName, timeout)
			<-timeout.C

			atomic.AddInt64(&c.RunningMapTask, -1)

			c.MapInputSplit <- taskName
		}()

	} else if atomic.LoadInt64(&c.RunningMapTask) == 0 && len(c.ReduceInputSplit) > 0 {
		taskName := <-c.ReduceInputSplit
		reply.TaskType = TaskTypeReduce
		reply.CurrentReduceTaskNumber = taskName
		c.SliceMutex.Lock()
		reply.ReduceFileNames = c.ReduceTaskPosition[cast.ToString(taskName)]
		c.SliceMutex.Unlock()
		atomic.AddInt64(&c.RunningReduceTask, 1)

		go func() {
			timeout := time.NewTimer(DefaultTimeout)
			c.RunningReduceTaskState.Store(cast.ToString(taskName), timeout)
			<-timeout.C

			c.ReduceInputSplit <- taskName
			atomic.AddInt64(&c.RunningReduceTask, -1)
		}()
	} else {
		reply.TaskType = TaskTypeNoTask
	}
	return nil
}

func (c *Coordinator) TaskDone(req *TaskDoneRequest, reply *TaskDoneReply) error {
	if req.TaskType == TaskTypeMap {
		val, ok := c.RunningMapTaskState.Load(req.FileName)
		if !ok {
			return ErrNoMatchingState
		}
		timeout := val.(*time.Timer)
		stop := timeout.Stop()
		if !stop {

		} else {

			atomic.AddInt64(&c.RunningMapTask, -1)

			for reduceNum, position := range req.ResultPosition {
				c.SliceMutex.Lock()
				c.ReduceTaskPosition[reduceNum] = append(c.ReduceTaskPosition[reduceNum], position)
				c.SliceMutex.Unlock()
			}
		}
	} else if req.TaskType == TaskTypeReduce {
		val, ok := c.RunningReduceTaskState.Load(req.FileName)
		if !ok {
			return ErrNoMatchingState
		}
		timeout := val.(*time.Timer)
		stop := timeout.Stop()
		if !stop {

		} else {
			atomic.AddInt64(&c.RunningReduceTask, -1)

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
	return len(c.MapInputSplit) == 0 && len(c.ReduceInputSplit) == 0 && atomic.LoadInt64(&c.RunningMapTask) == 0 && atomic.LoadInt64(&c.RunningReduceTask) == 0
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

	c.ReduceInputSplit = make(chan int64, c.ReduceTaskNumber)
	for i := 0; i < int(c.ReduceTaskNumber); i++ {
		c.ReduceInputSplit <- int64(i)
	}
	// c.RunningReduceTaskState = make(map[string]*time.Timer)
	c.SliceMutex = sync.Mutex{}

	c.server()
	return &c
}
