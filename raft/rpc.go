package raft

import (
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if !logNewer(args.LastLogTerm, args.LastLogIndex, rf.entries[len(rf.entries)-1].Term, int64(len(rf.entries)-1)) {
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	resp.Term = rf.currentTerm
	if req.Term < rf.currentTerm {
		resp.Success = false
		//lg.Infof("[%d] reject append entry,cur term : %d,req term:%d,entries:%v", rf.me, rf.currentTerm, req.Term, rf.entries)
		return
	}

	rf.electionTimeout.Stop()
	rf.electionTimeout.Reset(genRandomElectionTimeout())

	if req.Term > rf.currentTerm {
		rf.becomeFollower(req.Term)
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(rf.entries)-1 < int(req.PrevLogIndex) || rf.entries[req.PrevLogIndex].Term != req.PrevLogTerm {
		resp.Success = false
		//lg.Infof("[%d] reject append entry,cur entries : %d,req :%+v", rf.me, rf.entries, req)
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it
	for i := 0; i < minInt(len(req.Entries), len(rf.entries)-1-int(req.PrevLogIndex)); i++ {
		if req.Entries[i].Term != rf.entries[int(req.PrevLogIndex)+i+1].Term {
			rf.entries = rf.entries[:int(req.PrevLogIndex)+i+1]
			break
		}
	}

	// Append any new entries not already in the log
	rf.entries = copySlice(rf.entries, int(req.PrevLogIndex)+1, req.Entries)
	rf.persist()

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

	resp.Success = true
	// //lg.Infof("[%d] reset election timeout due to heart beat", rf.me)
	rf.electionTimeout.Stop()
	rf.electionTimeout.Reset(genRandomElectionTimeout())
}
