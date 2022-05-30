package kvraft

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/2naive2/raftkv/labgob"
	"github.com/2naive2/raftkv/labrpc"
	"github.com/2naive2/raftkv/raft"

	lg "github.com/sirupsen/logrus"
)

type OpType int

var (
	OpTypeGet    = OpType(0)
	OpTypePut    = OpType(1)
	OpTypeAppend = OpType(2)
)

const Debug = false

func D(format string, a ...interface{}) (n int, err error) {
	debug := os.Getenv("debug")
	if debug == "true" {
		lg.Infof(format, a...)
	}
	return
}

type Op struct {
	OpType OpType
	Key    string
	Value  string
}

type DB struct {
	data map[string]string
	mu   sync.Mutex
}

func (db *DB) Get(key string) (err error, value string) {
	// D("get, key:%v,data:%v", key, db.data)
	db.mu.Lock()
	defer db.mu.Unlock()
	val := db.data[key]
	return nil, val
}

func (db *DB) Put(key, value string) (err error) {
	// D("put , key:%v value:%v", key, value)
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data[key] = value
	// D("data after put:%v", db.data)
	return nil
}

func (db *DB) Append(key, value string) (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.data[key] += value
	return nil
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	committedCommand int64
	cmdMu            sync.Mutex
	DB               DB
	resultBuffer     map[int64]string
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) error {
	D("[%d] receive get request,key:[%v]", kv.me, args.Key)

	op := Op{
		OpType: OpTypeGet,
		Key:    args.Key,
	}
	cmdIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = Err(fmt.Sprintf("[%v] peer is not leader", kv.me))
		return nil
	}

	res := ""
	//wait for this command to be executed
	for {
		committedCommand := atomic.LoadInt64(&kv.committedCommand)
		if committedCommand < int64(cmdIndex) {
			time.Sleep(time.Millisecond * 50)
			continue
		}

		kv.cmdMu.Lock()
		res = kv.resultBuffer[int64(cmdIndex)]
		kv.cmdMu.Unlock()
		break
	}
	reply.Value = res
	D("[%d] command:%d done", kv.me, cmdIndex)
	return nil
}

func (kv *KVServer) applyCommands() {
	for msg := range kv.applyCh {
		if !msg.CommandValid {
			continue
		}

		D("[%d] apply command:%+v", kv.me, msg)

		cmd := msg.Command.(Op)
		switch cmd.OpType {
		case OpTypeGet:
			_, val := kv.DB.Get(cmd.Key)
			kv.cmdMu.Lock()
			kv.resultBuffer[int64(msg.CommandIndex)] = val
			kv.cmdMu.Unlock()
		case OpTypePut:
			kv.DB.Put(cmd.Key, cmd.Value)
		case OpTypeAppend:
			kv.DB.Append(cmd.Key, cmd.Value)
		}

		atomic.StoreInt64(&kv.committedCommand, int64(msg.CommandIndex))
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	D("[%d] receive put append request,op:[%v]  key:[%v] value:[%v]", kv.me, args.Op, args.Key, args.Value)
	var kind OpType
	switch args.Op {
	case "Put":
		kind = OpTypePut
	case "Append":
		kind = OpTypeAppend
	}

	op := Op{
		OpType: kind,
		Key:    args.Key,
		Value:  args.Value,
	}
	cmdIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = Err(fmt.Sprintf("[%v] peer is not leader", kv.me))
		return nil
	}

	//wait for this command to be executed
	for {
		committedCommand := atomic.LoadInt64(&kv.committedCommand)
		if committedCommand < int64(cmdIndex) {
			time.Sleep(time.Millisecond * 50)
			continue
		}
		break
	}

	D("[%d] command:%d done", kv.me, cmdIndex)
	return nil
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) serveConn(addr string) {
	rpc.Register(kv)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", addr)
	if err != nil {

	}
	go http.Serve(l, nil)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, addr string) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.DB = DB{
		data: make(map[string]string),
	}

	kv.resultBuffer = make(map[int64]string)

	go kv.applyCommands()
	go kv.serveConn(addr)

	return kv
}
