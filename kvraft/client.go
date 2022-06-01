package kvraft

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net/rpc"
	"time"

	"github.com/2naive2/raftkv/model"
	lg "github.com/sirupsen/logrus"
	"github.com/spf13/cast"
)

var (
	CommandTimeout = time.Millisecond * 1000
	ErrTimeout     = errors.New("timeout")
)

type Clerk struct {
	servers       []*rpc.Client
	currentLeader int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func establishClientConn(ends []*rpc.Client, conf *model.Conf) {
	for {
		done := true
		for i, end := range ends {
			if end == nil {
				done = false
				client, err := rpc.DialHTTP("tcp", conf.ServerAddress[cast.ToString(i)])
				if err != nil {
					continue
				}
				ends[i] = client
			}
		}
		if done {
			lg.Infof("client side all connection established")
			break
		}
		time.Sleep(time.Second)
	}
}

func MakeClerk() *Clerk {
	ck := new(Clerk)
	conf := model.GetConf()
	servers := make([]*rpc.Client, len(conf.ServerAddress))
	ck.servers = servers
	ck.currentLeader = -1

	go establishClientConn(servers, conf)
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	req := GetArgs{
		Key: key,
	}

	if ck.currentLeader != -1 {
		resp := GetReply{}
		ok := CallWithTimeout(CommandTimeout, ck.servers[ck.currentLeader].Call, "KVServer.Get", &req, &resp)
		// ok := ck.servers[ck.currentLeader].Call("KVServer.Get", &req, &resp)
		if ok == nil && resp.Err == "" {
			D("[client] get done,resp:%+v", resp)
			return resp.Value
		}
	}

	for i := 0; ; i = (i + 1) % len(ck.servers) {
		resp := GetReply{}
		// ok := ck.servers[i].Call("KVServer.Get", &req, &resp)
		ok := CallWithTimeout(CommandTimeout, ck.servers[i].Call, "KVServer.Get", &req, &resp)
		if ok == nil && resp.Err == "" {
			ck.currentLeader = i
			D("[client] get done,resp:%+v", resp)
			return resp.Value
		} else {
			D("[client] get failed,err:%v", resp.Err)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//

func (ck *Clerk) PutAppend(key string, value string, op string) {
	req := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}

	if ck.currentLeader != -1 {
		resp := PutAppendReply{}
		ok := CallWithTimeout(CommandTimeout, ck.servers[ck.currentLeader].Call, "KVServer.PutAppend", &req, &resp)
		// ok := ck.servers[ck.currentLeader].Call("KVServer.PutAppend", &req, &resp)
		if ok == nil && resp.Err == "" {
			D("[client] put append done,resp:%+v,current leader:%v", resp, ck.currentLeader)
			return
		}
	}

	for i := 0; ; i = (i + 1) % len(ck.servers) {
		resp := PutAppendReply{}
		// ok := ck.servers[i].Call("KVServer.PutAppend", &req, &resp)
		ok := CallWithTimeout(CommandTimeout, ck.servers[i].Call, "KVServer.PutAppend", &req, &resp)
		if ok == nil && resp.Err == "" {
			ck.currentLeader = i
			D("[client] put append done,resp:%+v,current leader:%v", resp, ck.currentLeader)
			return
		}
		D("{%d} error:%v", i, resp.Err)
		time.Sleep(CommandTimeout)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func CallWithTimeout(timeout time.Duration, fn func(svcMeth string, args interface{}, reply interface{}) error, svcMeth string, args interface{}, reply interface{}) error {
	resChan := make(chan error)
	go func() {
		res := fn(svcMeth, args, reply)
		resChan <- res
	}()

	select {
	case <-time.After(CommandTimeout):
		return ErrTimeout
	case ok := <-resChan:
		if ok != nil {
			return errors.New(fmt.Sprintf("rpc failed,err:%v", ok))
		}
		return nil
	}
}
