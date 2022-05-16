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
	time.Sleep(time.Second * 5)

	req := PutAppendArgs{
		Key:   "stuff",
		Value: "lee",
		Op:    "Put",
	}
	resp := PutAppendReply{}

	ck.servers[0].Call("KVServer.PutAppend", &req, &resp)
	if resp.Err == "" {
		D("[%v] success", 0)
	}

	ck.servers[1].Call("KVServer.PutAppend", &req, &resp)
	if resp.Err == "" {
		D("[%v] success", 1)
	}
	ck.servers[2].Call("KVServer.PutAppend", &req, &resp)
	if resp.Err == "" {
		D("[%v] success", 2)
	}

	// ck.Put("stuff", "Lee")
}
