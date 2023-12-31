package raft

func (rf *Raft) electionHandle() {
	rf.mu.Lock()
	rf.becomeCandidate()
	rf.mu.Unlock()
	var vote = RequestVoteArgs{
		Term:        rf.currTerm,
		CandidateId: rf.me,
	}
	for i := range rf.peers {
		if i == rf.me {
			// 不发给自己
			continue
		}
		go rf.sendRequestVoteAndHandle(i, &vote)
	}
}

func (rf *Raft) electionTimeoutHandle() {
	rf.mu.Lock()
	rf.becomeFollower(rf.currTerm, -1)
	rf.mu.Unlock()
}

func (rf *Raft) electedHandle() {
	rf.mu.Lock()
	rf.becomeLeader()
	rf.mu.Unlock()
	rf.sendAppendEntriesHandle()
}

func (rf *Raft) sendAppendEntriesHandle() {
	for i := range rf.peers {
		if i == rf.me {
			// 不发给自己
			continue
		}
		rf.mu.Lock()
		var args = AppendEntriesArgs{
			Term:     rf.currTerm,
			LeaderId: rf.me,
		}
		if len(rf.log) > 0 && rf.matchIndex[i] < len(rf.log)-1 {
			args.PrevLogIndex = rf.matchIndex[i]
			args.Entries = rf.log[rf.nextIndex[i]:len(rf.log)]
		}
		rf.mu.Unlock()
		go rf.sendAppendEntriesAndHandle(i, &args)
	}
}

func (rf *Raft) appendEntriesOkHandle() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Follower {
		rf.electionTick = 0
	} else if rf.state == Candidate {
		rf.becomeFollower(rf.currTerm, rf.leader)
	}
}
