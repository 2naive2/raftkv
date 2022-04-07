package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cast"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	for {
		assignReq := &AssignTaskRequest{}
		assignReply := &AssignTaskReply{}

		assignRes := call("Coordinator.AssignTask", assignReq, assignReply)
		//if can't contact master,exit
		if !assignRes {
			return
		}
		if assignReply.TaskType == TaskTypeNoTask {
			continue
		}

		doneReq := &TaskDoneRequest{
			TaskType:       assignReply.TaskType,
			ResultPosition: make(map[string]string),
		}
		doneReply := &TaskDoneReply{}

		if assignReply.TaskType == TaskTypeMap {

			doneReq.FileName = assignReply.FileName
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
				fileName := strings.TrimLeft(assignReply.FileName, "./")
				position := fmt.Sprintf("mr-%v-%v", fileName, i)
				doneReq.ResultPosition[cast.ToString(i)] = position
				tempFile, err := os.Create(position)
				if err != nil {

					return
				}
				tempFiles = append(tempFiles, json.NewEncoder(tempFile))
			}

			for _, pair := range interResult {
				bucket := ihash(pair.Key) % int(assignReply.ReduceTaskNumber)
				err := tempFiles[bucket].Encode(&pair)
				if err != nil {

				}
			}
		} else if assignReply.TaskType == TaskTypeReduce {

			doneReq.FileName = cast.ToString(assignReply.CurrentReduceTaskNumber)
			pairs := []KeyValue{}
			for _, file := range assignReply.ReduceFileNames {
				reduceFile, err := os.Open(file)
				if err != nil {

					continue
				}
				decoder := json.NewDecoder(reduceFile)
				for {
					kv := KeyValue{}
					if err := decoder.Decode(&kv); err != nil {
						break
					}
					pairs = append(pairs, kv)
				}
			}
			sort.Sort(ByKey(pairs))

			ofileName := fmt.Sprintf("mr-out-%v", assignReply.CurrentReduceTaskNumber)
			ofile, err := os.Create(ofileName)
			if err != nil {

			}

			i := 0
			for i < len(pairs) {
				j := i + 1
				for j < len(pairs) && pairs[j].Key == pairs[i].Key {
					j++
				}
				sameKeySet := []string{}
				for k := i; k < j; k++ {
					sameKeySet = append(sameKeySet, pairs[k].Value)
				}

				reduceRes := reducef(pairs[i].Key, sameKeySet)
				fmt.Fprintf(ofile, "%v %v\n", pairs[i].Key, reduceRes)
				i = j
			}
			ofile.Close()

		}

		call("Coordinator.TaskDone", doneReq, doneReply)
	}

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
