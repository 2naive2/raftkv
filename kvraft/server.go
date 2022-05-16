package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"

	lg "github.com/sirupsen/logrus"
)

type OpType int

var (
	OpTypeGet    = OpType(0)
	OpTypePut    = OpType(1)
	OpTypeAppend = OpType(2)
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	OpType OpType
	Key    string
	Value  string
}

type DB struct {
}

func (db *DB) Get(key string) (err error, value string) {
	return
}

func (db *DB) Put(key, value string) (err error) {
	return
}

func (db *DB) Append(key, value string) (err error) {
	return
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

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	lg.Infof("receive get request:%+v", args)
	reply.Err = "no error in get"
	reply.Value = "你好呀"
	// Your code here.
}

func (kv *KVServer) ApplyCommands() {
	for msg := range kv.applyCh {
		if !msg.CommandValid {
			continue
		}

		cmd := msg.Command.(Op)
		res := ""
		switch cmd.OpType {
		case OpTypeGet:
		case OpTypePut:
		case OpTypeAppend:
		}

		kv.cmdMu.Lock()
		kv.committedCommand = int64(msg.CommandIndex)
		kv.resultBuffer[int64(msg.CommandIndex)] = res
		kv.cmdMu.Unlock()
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	lg.Infof("receive put append request:%+v", args)
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
		reply.Err = "peer is not leader"
		return
	}

	//wait for this command to be executed
	for {
		kv.cmdMu.Lock()
		if kv.committedCommand < int64(cmdIndex) {
			kv.mu.Unlock()
			time.Sleep(time.Millisecond * 50)
			continue
		}
		kv.cmdMu.Unlock()
		break
	}
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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
