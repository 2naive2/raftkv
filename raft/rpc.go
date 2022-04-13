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
	reply.Term = atomic.LoadInt64(&rf.currentTerm)
	//todo: consider logIndex
	if args.Term > atomic.LoadInt64(&rf.currentTerm) {
		atomic.StoreInt64(&rf.currentTerm, args.Term)
		atomic.StoreInt64((*int64)(&rf.state), int64(RaftStateFollwer))
		rf.votedFor = nil
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	lg.Infof("[%d] tuple: entries:%v, %d %d %d %d", rf.me, rf.entries, args.LastLogTerm, args.LastLogIndex, rf.entries[len(rf.entries)-1].Term, int64(len(rf.entries)-1))
	if !tuppleBigger(args.LastLogTerm, args.LastLogIndex, rf.entries[len(rf.entries)-1].Term, int64(len(rf.entries)-1)) {
		return
	}

	if rf.votedFor == nil || (rf.votedFor != nil && *rf.votedFor == args.CandidateID) {
		lg.Infof("[%d] vote for [%d]", rf.me, args.CandidateID)
		reply.VoteGranted = true
		voteID := args.CandidateID
		rf.votedFor = &voteID

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
		lg.Infof("[%d] reject append entry,cur term : %d,req term:%d,entries:%v", rf.me, rf.currentTerm, req.Term, rf.entries)
		return
	}

	if req.Term > atomic.LoadInt64(&rf.currentTerm) {
		atomic.StoreInt64(&rf.currentTerm, req.Term)
	}

	atomic.StoreInt64((*int64)(&rf.state), int64(RaftStateFollwer))

	rf.mu.Lock()
	if len(rf.entries)-1 < int(req.PrevLogIndex) || rf.entries[req.PrevLogIndex].Term != req.PrevLogTerm {
		resp.Success = false
		lg.Infof("[%d] reject append entry,cur entries : %d,req :%+v", rf.me, rf.entries, req)
		rf.mu.Unlock()
		return
	}
	rf.entries = append(rf.entries[:req.PrevLogIndex+1], req.Entries...)
	rf.mu.Unlock()

	// todo consider resend to channel
	if req.LeaderCommit > rf.commitIndex {
		newCommitIndex := minInt64(req.LeaderCommit, int64(len(rf.entries)-1))
		for i := rf.commitIndex + 1; i <= newCommitIndex; i++ {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.entries[i].Data,
				CommandIndex: int(i),
			}
			rf.ApplyChan <- msg
			lg.Infof("[%d] follower commits log at %d,with entries:%+v", rf.me, i, rf.entries)
		}
		rf.commitIndex = newCommitIndex
		rf.applyChanIndex[rf.me] = newCommitIndex
	}
	resp.Success = true
	resp.Term = atomic.LoadInt64(&rf.currentTerm)
	// lg.Infof("[%d] reset election timeout due to heart beat", rf.me)
	rf.electionTimeout.Stop()
	rf.electionTimeout.Reset(genRandomElectionTimeout())
}
