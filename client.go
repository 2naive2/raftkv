package main

import (
	"net/rpc"

	"github.com/2naive2/raftkv/kvraft"
	lg "github.com/sirupsen/logrus"
)

func main() {
	req := &kvraft.GetArgs{
		Key: "name",
	}
	resp := &kvraft.GetReply{}
	client, err := rpc.DialHTTP("tcp", ":1234")
	if err != nil {
	}
	err = client.Call("KVServer.Get", req, resp)
	if err != nil {
		lg.Errorf("rpc failed,err:%v", err)
	}
}
