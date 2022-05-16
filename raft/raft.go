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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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
	SyncLoginterval   = time.Millisecond * 50
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
	votedFor    int64
	entries     []LogEntry
	state       RaftState

	commitIndex   int64
	lastAppliedID int64

	electionTimeout  *time.Timer
	heartBeatTimeout *time.Timer

	ApplyChan chan ApplyMsg

	syncLogTimeout *time.Timer
	nextIndex      []int64
	matchIndex     []int64

	applyChanIndex []int64
	firstCommit    int64
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(atomic.LoadInt64(&rf.currentTerm)), atomic.LoadInt64((*int64)(&rf.state)) == int64(RaftStateLeader)
}

func (rf *Raft) becomeFollower(term int64) {
	rf.state = RaftStateFollwer
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	rf.state = RaftStateLeader

	// reinitialize voloatile state
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = int64(len(rf.entries))
	}

	// start heart beat (periodically send heart beat to all servers)
	// for this turn,immediatelly trigger a heart beat to be sent
	// send heart beat to all server to establish
	// authority
	rf.heartBeatTimeout = time.NewTimer(0)
	go rf.heartBeat()
	rf.syncLogTimeout = time.NewTimer(0)
	go rf.syncLogs()
	rf.firstCommit = 0
}

func (rf *Raft) becomeCandidate() {
	rf.state = RaftStateCandidate
	rf.currentTerm++
	rf.votedFor = int64(rf.me)
	rf.persist()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(atomic.LoadInt64(&rf.currentTerm))
	e.Encode(rf.votedFor)
	e.Encode(rf.entries)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	term := int64(0)
	var votedFor int64
	entries := []LogEntry{}

	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&entries) != nil {
		lg.Error("decode from persistent state failed")
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.entries = entries
		lg.Infof("[%d] loaded state, term:%v , currentTerm:%v entries:%v", rf.me, rf.currentTerm, rf.votedFor, rf.entries)
	}
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
		if reply.Term > atomic.LoadInt64(&rf.currentTerm) {
			atomic.StoreInt64(&rf.currentTerm, reply.Term)
			atomic.StoreInt64((*int64)(&rf.state), int64(RaftStateFollwer))
			rf.persist()
		}
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
		if reply.Term > atomic.LoadInt64(&rf.currentTerm) {
			atomic.StoreInt64(&rf.currentTerm, reply.Term)
			atomic.StoreInt64((*int64)(&rf.state), int64(RaftStateFollwer))
			rf.persist()
		}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != RaftStateLeader {
		return -1, int(atomic.LoadInt64(&rf.currentTerm)), false
	}

	entry := LogEntry{Term: rf.currentTerm, Data: command}
	rf.entries = append(rf.entries, entry)
	rf.persist()
	lg.Infof("[%d] leader append new entry %v,entries :%v", rf.me, entry, rf.entries)

	// immediate begin to sync log to followers
	rf.syncLogTimeout.Stop()
	rf.syncLogTimeout.Reset(0)

	return len(rf.entries) - 1, int(atomic.LoadInt64(&rf.currentTerm)), true
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
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			// prevIndex := minInt64(rf.nextIndex[i]-1, int64(len(rf.entries)-1)) // its log may has chanegd from when its nextIndex was set
			req := AppendEntryRequest{
				Term:         atomic.LoadInt64(&rf.currentTerm),
				LeaderID:     int64(rf.me),
				PrevLogIndex: int64(len(rf.entries)) - 1,
				PrevLogTerm:  rf.entries[len(rf.entries)-1].Term,
				Entries:      []LogEntry{},
				LeaderCommit: rf.commitIndex,
			}
			resp := AppendEntryResponse{}
			rf.sendAppendEntries(i, &req, &resp)
		}(i)
	}
}

func (rf *Raft) heartBeat() {
	for {
		<-rf.heartBeatTimeout.C
		rf.mu.Lock()
		if rf.state != RaftStateLeader {
			rf.mu.Unlock()
			break
		}
		rf.sendHeartBeatToAllServer()
		rf.mu.Unlock()
		rf.heartBeatTimeout.Reset(HeartBeatInterval)
	}
}

func (rf *Raft) startAnElection() {
	votes := 1
	finished := 0
	var voteMu sync.Mutex
	cond := sync.NewCond(&voteMu)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.mu.Lock()
			lastLog, lastTerm := int64(len(rf.entries)-1), rf.entries[len(rf.entries)-1].Term
			rf.mu.Unlock()
			req := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  int64(rf.me),
				LastLogIndex: lastLog,
				LastLogTerm:  lastTerm,
			}
			resp := RequestVoteReply{}
			ok := rf.sendRequestVote(i, &req, &resp)
			voteMu.Lock()
			if ok && resp.VoteGranted {
				votes++
			}
			finished++
			voteMu.Unlock()
			cond.Broadcast()
		}(i)
	}

	grantedVotes := 0
	voteMu.Lock()
	defer voteMu.Unlock()
	for !(votes > len(rf.peers)/2 || finished == (len(rf.peers))-1) {
		cond.Wait()
		grantedVotes = votes
	}

	rf.mu.Lock()
	if grantedVotes > len(rf.peers)/2 && rf.state == RaftStateCandidate {
		rf.becomeLeader()
		lg.Infof("[%d] win election for term:%v with votes:%v", rf.me, atomic.LoadInt64(&rf.currentTerm), grantedVotes)
	} else {
		lg.Infof("[%d] lose  election for term:%v", rf.me, atomic.LoadInt64(&rf.currentTerm))
	}
	rf.mu.Unlock()
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		<-rf.electionTimeout.C

		rf.mu.Lock()
		if rf.state == RaftStateLeader {
			rf.mu.Unlock()
			rf.electionTimeout.Reset(genRandomElectionTimeout())
			continue
		}

		lg.Infof("[%d] election timeout and start an election,current term:%v", rf.me, atomic.LoadInt64(&rf.currentTerm))
		rf.becomeCandidate()
		rf.mu.Unlock()

		// non-blocking,periodically issue an election when timer fires
		rf.startAnElection()

		rf.electionTimeout.Reset(genRandomElectionTimeout())
	}
}

//! single threaded
func (rf *Raft) syncLogs() {
	syncing := make([]int64, len(rf.peers))
	for {
		<-rf.syncLogTimeout.C
		rf.mu.Lock()
		if rf.state != RaftStateLeader {
			lg.Warnf("[%d] is deprected from leader", rf.me)
			rf.mu.Unlock()
			return
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			if len(rf.entries)-1 < int(rf.nextIndex[i]) {
				continue
			}

			if atomic.LoadInt64(&syncing[i]) == 1 {
				continue
			}

			go func(i int) {
				//prevent multiple sync to same server
				atomic.StoreInt64(&syncing[i], 1)
				defer atomic.StoreInt64(&syncing[i], 0)
				for {
					rf.mu.Lock()
					if rf.state != RaftStateLeader {
						rf.mu.Unlock()
						return
					}
					lastLogIndex := rf.nextIndex[i] - 1
					lastLogTerm := rf.entries[lastLogIndex].Term
					syncEntries := rf.entries[rf.nextIndex[i]:]
					term := rf.currentTerm

					req := AppendEntryRequest{
						Term:         term,
						LeaderID:     int64(rf.me),
						PrevLogIndex: lastLogIndex,
						PrevLogTerm:  lastLogTerm,
						Entries:      syncEntries,
						LeaderCommit: rf.commitIndex,
					}
					resp := AppendEntryResponse{}

					rf.mu.Unlock()
					if !rf.sendAppendEntries(i, &req, &resp) {
						continue
					}

					lg.Infof("[%d] sync entries :%v to [%d]", rf.me, syncEntries, i)
					rf.mu.Lock()
					if rf.state != RaftStateLeader {
						// immediately terminal sync log thread
						rf.mu.Unlock()
						return
					}

					if resp.Success {
						rf.nextIndex[i] = req.PrevLogIndex + 1 + int64(len(syncEntries))
						rf.matchIndex[i] = req.PrevLogIndex + int64(len(syncEntries))
						// immediate issue an round of checks to update leader commit
						updateCommitIndex(rf)
						rf.mu.Unlock()
						break
					} else {
						rf.nextIndex[i] = req.PrevLogIndex
						lg.Infof("[%d] detect inconsistency in [%d],new nextIndex:%d", rf.me, i, rf.nextIndex[i])
						rf.mu.Unlock()
					}
				}
			}(i)
		}

		rf.mu.Unlock()
		rf.syncLogTimeout.Reset(SyncLoginterval)
	}
	lg.Warnf("[%d] is deprected from leader", rf.me)
}

func updateCommitIndex(rf *Raft) {
	for cur := rf.commitIndex + 1; int(cur) < len(rf.entries); cur++ {
		if rf.entries[cur].Term != rf.currentTerm {
			continue
		}
		replicated := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= cur {
				replicated++
			}
		}
		if replicated > len(rf.peers)/2 {
			rf.commitIndex = cur
			// * only update commit thread will access first-commit
			if rf.firstCommit == 0 {
				for i := rf.applyChanIndex[rf.me] + 1; i <= cur; i++ {
					msg := ApplyMsg{
						CommandValid: true,
						Command:      rf.entries[i].Data,
						CommandIndex: int(i),
					}
					// rf.commitIndex = maxInt64(rf.commitIndex, len)
					rf.ApplyChan <- msg
					lg.Infof("[%d] leader commits log at %d", rf.me, i)
				}
				rf.firstCommit = 1
			} else {
				//tome : might commit entries from previous term!
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.entries[cur].Data,
					CommandIndex: int(cur),
				}
				rf.ApplyChan <- msg
				lg.Infof("[%d] leader commits log at %d", rf.me, cur)
			}
			rf.applyChanIndex[rf.me] = cur
		}
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
	rf.votedFor = -1

	rf.electionTimeout = time.NewTimer(genRandomElectionTimeout())
	rf.entries = []LogEntry{{Term: 0, Data: -1}}
	rf.nextIndex = make([]int64, len(rf.peers))
	rf.matchIndex = make([]int64, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
	}

	rf.applyChanIndex = make([]int64, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
