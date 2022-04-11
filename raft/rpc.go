package raft

import (
	"sync/atomic"

	lg "github.com/sirupsen/logrus"
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
		rf.votedFor = nil
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (rf.votedFor != nil && *rf.votedFor == args.CandidateID) || (rf.votedFor == nil && args.LastLogIndex >= rf.commitIndex) {
		lg.Infof("[%d] vote for [%d]", rf.me, args.CandidateID)
		reply.VoteGranted = true
		voteID := args.CandidateID
		rf.votedFor = &voteID

		rf.electionTimeout.Stop()
		rf.electionTimeout.Reset(genRandomElectionTimeout())
	}
	reply.Term = atomic.LoadInt64(&rf.currentTerm)
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
	if req.Term < atomic.LoadInt64(&rf.currentTerm) {
		resp.Success = false
		return
	}

	// if len(rf.entries)-1 < int(req.PrevLogIndex) || rf.entries[req.PrevLogIndex].Term != req.PrevLogTerm {
	// 	if len(rf.entries)-1 >= int(req.PrevLogIndex) {
	// 		rf.entries = rf.entries[:req.PrevLogIndex]
	// 	}
	// 	resp.Success = false
	// 	return
	// }

	if req.Term > atomic.LoadInt64(&rf.currentTerm) {
		atomic.StoreInt64(&rf.currentTerm, req.Term)
	}
	atomic.StoreInt64((*int64)(&rf.state), int64(RaftStateFollwer))
	// rf.entries = append(rf.entries[:req.PrevLogIndex], req.Entries...)

	//todo : commit entries
	if req.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt64(req.LeaderCommit, int64(len(rf.entries)-1))
	}
	resp.Success = true
	resp.Term = atomic.LoadInt64(&rf.currentTerm)
	// lg.Infof("[%d] reset election timeout due to heart beat", rf.me)
	rf.electionTimeout.Stop()
	rf.electionTimeout.Reset(genRandomElectionTimeout())
}
