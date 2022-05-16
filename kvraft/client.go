package kvraft

import (
	"crypto/rand"
	"errors"
	"math/big"
	"time"

	"6.824/labrpc"
	lg "github.com/sirupsen/logrus"
)

var (
	CommandTimeout = time.Millisecond * 20
	ErrTimeout     = errors.New("timeout")
)

type Clerk struct {
	servers       []*labrpc.ClientEnd
	currentLeader int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.currentLeader = -1
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
	resp := GetReply{}

	if ck.currentLeader != -1 {
		err := CallWithTimeout(CommandTimeout, ck.servers[ck.currentLeader].Call, "KVServer.Get", &req, &resp)
		if err == nil {
			return resp.Value
		}
	}

	for i, end := range ck.servers {
		err := CallWithTimeout(CommandTimeout, end.Call, "KVServer.Get", &req, &resp)
		if err == nil {
			ck.currentLeader = i
			return resp.Value
		}
	}

	// You will have to modify this function.
	return ""
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

func CallWithTimeout(timeout time.Duration, fn func(svcMeth string, args interface{}, reply interface{}) bool, svcMeth string, args interface{}, reply interface{}) error {
	resChan := make(chan bool)
	go func() {
		res := fn(svcMeth, args, reply)
		resChan <- res
	}()

	select {
	case <-time.After(CommandTimeout):
		return ErrTimeout
	case ok := <-resChan:
		if !ok {
			return errors.New("put append no reply")
		}
		lg.Infof("put append response:%+v", reply)
		return nil
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	req := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	resp := PutAppendReply{}

	if ck.currentLeader != -1 {
		err := CallWithTimeout(CommandTimeout, ck.servers[ck.currentLeader].Call, "KVServer.PutAppend", &req, &resp)
		if err == nil {
			return
		}
	}

	for i, end := range ck.servers {
		err := CallWithTimeout(CommandTimeout, end.Call, "KVServer.PutAppend", &req, &resp)
		if err == nil {
			ck.currentLeader = i
			return
		}
	}

	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
