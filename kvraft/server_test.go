package kvraft

import (
	"testing"
	"time"
)

func TestServer(t *testing.T) {
	cfg := make_config(t, 3, false, 0)
	defer cfg.cleanup()

	cfg.begin("server test")
	ck := cfg.makeClient(cfg.All())
	time.Sleep(time.Second * 3)

	// req := PutAppendArgs{
	// 	Key:   "stuff",
	// 	Value: "lee",
	// 	Op:    "Put",
	// }
	// resp := PutAppendReply{}
	ck.Put("stuff", "lee")
	val := ck.Get("stuff")
	D("value:%v", val)

	ck.Put("", "")
	val = ck.Get("")
	D("value:%v", val)
	// ck.servers[0].Call("KVServer.PutAppend", &req, &resp)
	// if resp.Err == "" {
	// 	D("[%v] success", 0)
	// } else {
	// 	D("[%d] error:%v", 0, resp.Err)
	// }
	// time.Sleep(time.Millisecond * 100)

	// getReq := GetArgs{
	// 	Key: "stuff",
	// }
	// getResp := GetReply{}
	// ck.servers[0].Call("KVServer.Get", &getReq, &getResp)
	// if resp.Err == "" {
	// 	D("[%v] success,value:%v", 0, getResp.Value)
	// } else {
	// 	D("[%d] error:%v", 0, resp.Err)
	// }

	// ck.servers[1].Call("KVServer.PutAppend", &req, &resp)
	// if resp.Err == "" {
	// 	D("[%v] success", 1)
	// } else {
	// 	D("[%d] error:%v", 1, resp.Err)
	// }
	// time.Sleep(time.Millisecond * 100)

	// ck.servers[2].Call("KVServer.PutAppend", &req, &resp)
	// if resp.Err == "" {
	// 	D("[%v] success", 2)
	// } else {
	// 	D("[%d] error:%v", 2, resp.Err)
	// }

	// ck.Put("stuff", "Lee")
}
