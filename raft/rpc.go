package raft

import (
	"sync/atomic"
	//lg "github.com/sirupsen/logrus"
)

type RequestVoteArgs struct {
	Term         int64
	CandidateID  int64
	LastLogIndex int64
	LastLogTerm  int64
}

type RequestVoteReply struct {
	Term        int64
	VoteGranted bool
}

// stub impl for RequestVite
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// transition to a new term
	//todo: consider logIndex
	if args.Term > atomic.LoadInt64(&rf.currentTerm) {
		atomic.StoreInt64(&rf.currentTerm, args.Term)
		atomic.StoreInt64((*int64)(&rf.state), int64(RaftStateFollwer))
		rf.votedFor = -1
		rf.persist()
	}
	reply.Term = atomic.LoadInt64(&rf.currentTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !tuppleBigger(args.LastLogTerm, args.LastLogIndex, rf.entries[len(rf.entries)-1].Term, int64(len(rf.entries)-1)) {
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		//lg.Infof("[%d] vote for [%d]", rf.me, args.CandidateID)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.persist()

		rf.electionTimeout.Stop()
		rf.electionTimeout.Reset(genRandomElectionTimeout())
	}
}

type AppendEntryRequest struct {
	Term         int64
	LeaderID     int64
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64
}

type AppendEntryResponse struct {
	Term    int64
	Success bool
}

func (rf *Raft) AppendEntries(req *AppendEntryRequest, resp *AppendEntryResponse) {
	resp.Term = atomic.LoadInt64(&rf.currentTerm)
	if req.Term < atomic.LoadInt64(&rf.currentTerm) {
		resp.Success = false
		//lg.Infof("[%d] reject append entry,cur term : %d,req term:%d,entries:%v", rf.me, rf.currentTerm, req.Term, rf.entries)
		return
	}

	if req.Term > atomic.LoadInt64(&rf.currentTerm) {
		atomic.StoreInt64(&rf.currentTerm, req.Term)
		rf.persist()
	}

	atomic.StoreInt64((*int64)(&rf.state), int64(RaftStateFollwer))

	rf.mu.Lock()
	if len(rf.entries)-1 < int(req.PrevLogIndex) || rf.entries[req.PrevLogIndex].Term != req.PrevLogTerm {
		resp.Success = false
		//! delete conflicting entries
		if len(rf.entries)-1 >= int(req.PrevLogIndex) {
			rf.entries = rf.entries[:req.PrevLogIndex]
		}
		//lg.Infof("[%d] reject append entry,cur entries : %d,req :%+v", rf.me, rf.entries, req)
		rf.mu.Unlock()
		return
	}

	lastSharedIndex := minInt64(int64(len(rf.entries))-1, req.PrevLogIndex+int64(len(req.Entries)))
	for i := req.PrevLogIndex + 1; i <= lastSharedIndex; i++ {
		rf.entries[i] = req.Entries[i-req.PrevLogIndex-1]
	}
	if int(req.PrevLogIndex)+len(req.Entries) > len(rf.entries) {
		rf.entries = append(rf.entries, req.Entries[lastSharedIndex-req.PrevLogIndex:]...)
	}

	rf.entries = append(rf.entries[:req.PrevLogIndex+1], req.Entries...)

	if req.LeaderCommit > rf.commitIndex {
		newCommitIndex := minInt64(req.LeaderCommit, int64(len(rf.entries)-1))
		for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.entries[i].Data,
				CommandIndex: int(i),
			}
			rf.ApplyChan <- msg
			//lg.Infof("[%d] follower commits log at %d,with entries:%+v", rf.me, i, rf.entries)
		}
		rf.commitIndex = newCommitIndex
		rf.applyChanIndex[rf.me] = newCommitIndex
	}
	rf.mu.Unlock()

	resp.Success = true
	resp.Term = atomic.LoadInt64(&rf.currentTerm)
	// //lg.Infof("[%d] reset election timeout due to heart beat", rf.me)
	rf.electionTimeout.Stop()
	rf.electionTimeout.Reset(genRandomElectionTimeout())
}
