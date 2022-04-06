package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"

	lg "github.com/sirupsen/logrus"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	assignReq := &AssignTaskRequest{}
	assignReply := &AssignTaskReply{}
	call("Coordinator.AssignTask", assignReq, assignReply)

	if assignReply.TaskType == TaskTypeMap {
		file, err := os.Open(assignReply.FileName)
		if err != nil {
			log.Fatalf("cannot open %v", assignReply.FileName)
		}
		defer file.Close()
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", assignReply.FileName)
		}
		interResult := mapf(assignReply.FileName, string(content))

		tempFiles := make([]*json.Encoder, 0, assignReply.ReduceTaskNumber)
		for i := 0; i < int(assignReply.ReduceTaskNumber); i++ {
			tempFile, err := os.Create(fmt.Sprintf("mr-%v-%v", assignReply.FileName, i))
			if err != nil {
				lg.Error("create temporary file failed")
				return
			}
			tempFiles = append(tempFiles, json.NewEncoder(tempFile))
		}

		for _, pair := range interResult {
			bucket := ihash(pair.Key) % int(assignReply.ReduceTaskNumber)
			tempFiles[bucket].Encode(&pair)
		}

	}

	doneReq := &TaskDoneRequest{
		TaskType: assignReply.TaskType,
		FileName: assignReply.FileName,
	}
	doneReply := &TaskDoneReply{}
	call("Coordinator.TaskDone", doneReq, doneReply)
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
