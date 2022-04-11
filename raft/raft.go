package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
	lg "github.com/sirupsen/logrus"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	lg.SetFormatter(&lg.TextFormatter{FullTimestamp: true})
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Data interface{}
	Term int64
}

type RaftState int64

var (
	RaftStateFollwer   = RaftState(0)
	RaftStateCandidate = RaftState(1)
	RaftStateLeader    = RaftState(2)
)

var (
	HeartBeatInterval = time.Millisecond * 100
	ElectionMinTime   = time.Millisecond * 300
	ElectionDeltaTime = 300
	RPCTimeout        = time.Millisecond * 300
	RPCRetryInterval  = time.Millisecond * 100
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int64
	votedFor    *int64
	entries     []LogEntry
	state       RaftState

	commitIndex   int64
	lastAppliedID int64

	electionTimeout  *time.Timer
	heartBeatTimeout *time.Timer
	heartBeatMu      sync.Mutex
	rander           rand.Rand

	ApplyChan chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(atomic.LoadInt64(&rf.currentTerm)), atomic.LoadInt64((*int64)(&rf.state)) == int64(RaftStateLeader)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	result := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		result <- ok
	}()
	select {
	case <-time.After(RPCTimeout):
		return false
	case res := <-result:
		return res
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryRequest, reply *AppendEntryResponse) bool {
	result := make(chan bool, 1)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		result <- ok
	}()
	select {
	case <-time.After(RPCTimeout):
		return false
	case res := <-result:
		return res
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	if atomic.LoadInt64((*int64)(&rf.state)) != int64(RaftStateLeader) {
		return -1, int(rf.currentTerm), false
	}

	lastLog, lastTerm := int64(len(rf.entries)-1), rf.entries[len(rf.entries)-1].Term

	entry := LogEntry{Term: rf.currentTerm, Data: command}
	rf.entries = append(rf.entries, entry)

	go func(term, leaderCommit int64) {
		replicated := 1
		var mu sync.Mutex
		cond := sync.NewCond(&mu)
		req := AppendEntryRequest{
			Term:         term,
			LeaderID:     int64(rf.me),
			PrevLogIndex: lastLog,
			PrevLogTerm:  lastTerm,
			Entries:      []LogEntry{entry},
			LeaderCommit: leaderCommit,
		}
		resps := make([]AppendEntryResponse, len(rf.peers))

		//! try all RPC until all followers respond
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				for !rf.sendAppendEntries(i, &req, &resps[i]) {
					time.Sleep(RPCRetryInterval)
				}
				if resps[i].Success {
					mu.Lock()
					replicated++
					cond.Broadcast()
					mu.Unlock()
				}

			}(i)
		}

		mu.Lock()
		for replicated > len(rf.peers)/2 {
			cond.Wait()
		}
		mu.Unlock()
		lg.Infof("[%d] agree on :%d", rf.me, len(rf.entries)-1)

		rf.commitIndex = lastLog + 1
		msg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: len(rf.entries) - 1,
		}
		// rf.commitIndex = maxInt64(rf.commitIndex, len)
		rf.ApplyChan <- msg
	}(rf.currentTerm, rf.commitIndex)

	return len(rf.entries) - 1, int(rf.currentTerm), true
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.

func (rf *Raft) sendHeartBeatToAllServer() {
	req := AppendEntryRequest{
		Term:         atomic.LoadInt64(&rf.currentTerm),
		LeaderID:     int64(rf.me),
		PrevLogIndex: int64(len(rf.entries) - 1),
		PrevLogTerm:  rf.entries[len(rf.entries)-1].Term,
		Entries:      []LogEntry{},
		LeaderCommit: rf.commitIndex,
	}
	resps := make([]AppendEntryResponse, len(rf.peers))

	var wg sync.WaitGroup
	wg.Add(len(rf.peers) - 1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.sendAppendEntries(i, &req, &resps[i])
			wg.Done()
		}(i)
	}
	wg.Wait()

	for _, resp := range resps {
		if resp.Term > atomic.LoadInt64(&rf.currentTerm) {
			atomic.StoreInt64(&rf.currentTerm, resp.Term)
			atomic.StoreInt64((*int64)(&rf.state), int64(RaftStateFollwer))
			lg.Infof("[%d] leader invalidated due to higher term ,server term:%d ,leader term:%d", rf.me, resp.Term, rf.currentTerm)
		}
	}

}

func (rf *Raft) heartBeat() {
	for atomic.LoadInt64((*int64)(&rf.state)) == int64(RaftStateLeader) {
		<-rf.heartBeatTimeout.C
		// send HeartBeat
		go rf.sendHeartBeatToAllServer()
		rf.heartBeatMu.Lock()
		rf.heartBeatTimeout.Reset(HeartBeatInterval)
		rf.heartBeatMu.Unlock()
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		<-rf.electionTimeout.C
		if atomic.LoadInt64((*int64)(&rf.state)) == int64(RaftStateLeader) {
			rf.electionTimeout.Reset(genRandomElectionTimeout())
			continue
		}
		lg.Infof("[%d] election timeout and start an election,current term:%v", rf.me, atomic.LoadInt64(&rf.currentTerm))
		atomic.StoreInt64((*int64)(&rf.state), int64(RaftStateCandidate))
		atomic.AddInt64(&rf.currentTerm, 1)
		rf.mu.Lock()
		voteFor := int64(rf.me)
		rf.votedFor = &voteFor
		rf.mu.Unlock()
		votes := 1
		finished := 0
		// vote for myself
		voteMutex := sync.Mutex{}
		cond := sync.NewCond(&voteMutex)
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(i int) {
				lastLog, lastTerm := int64(len(rf.entries)-1), rf.entries[len(rf.entries)-1].Term
				req := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateID:  int64(rf.me),
					LastLogIndex: lastLog,
					LastLogTerm:  lastTerm,
				}
				resp := RequestVoteReply{}
				ok := rf.sendRequestVote(i, &req, &resp)
				voteMutex.Lock()
				defer voteMutex.Unlock()
				if ok && resp.VoteGranted {
					votes++
				}

				if resp.Term > atomic.LoadInt64(&rf.currentTerm) {
					atomic.StoreInt64(&rf.currentTerm, resp.Term)
					atomic.StoreInt64((*int64)(&rf.state), int64(RaftStateFollwer))
					lg.Infof("[%d] leader invalidated due to higher term ,server term:%d ,leader term:%d", rf.me, resp.Term, rf.currentTerm)
				}

				finished++
				cond.Broadcast()
			}(i)
		}

		voteMutex.Lock()
		for !(votes > len(rf.peers)/2 || finished == (len(rf.peers))-1) {
			cond.Wait()
		}
		if votes > len(rf.peers)/2 {
			lg.Infof("[%d] win election for term:%v with votes:%v", rf.me, atomic.LoadInt64(&rf.currentTerm), votes)
			atomic.StoreInt64((*int64)(&rf.state), int64(RaftStateLeader))

			// start heart beat (periodically send heart beat to all servers)
			// for this turn,immediatelly trigger a heart beat to be sent
			// send heart beat to all server to establish
			// authority
			rf.heartBeatMu.Lock()
			rf.heartBeatTimeout = time.NewTimer(0)
			rf.heartBeatMu.Unlock()
			go rf.heartBeat()
		} else {
			lg.Infof("[%d] lose  election for term:%v", rf.me, atomic.LoadInt64(&rf.currentTerm))
		}
		voteMutex.Unlock()

		rf.electionTimeout.Reset(genRandomElectionTimeout())
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.ApplyChan = applyCh

	rf.electionTimeout = time.NewTimer(genRandomElectionTimeout())
	rf.entries = []LogEntry{{Term: 0, Data: -1}}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
