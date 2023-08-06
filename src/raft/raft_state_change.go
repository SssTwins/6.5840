package raft

import "log"

func (rf *Raft) becomeFollower(term uint64, leader int) {
	rf.tickReset(rf.currTerm)
	rf.state = Follower
	rf.currTerm = term
	rf.leader = leader
	rf.votedFor = -1
	rf.tick = rf.tickElection
	log.Printf("%x became follower at term %d", rf.me, rf.currTerm)
}

func (rf *Raft) becomeCandidate() {
	rf.tickReset(rf.currTerm)
	rf.state = Candidate
	rf.tick = rf.tickElectionTimeout
	rf.votedFor = rf.me
	rf.currTerm++
	rf.voteCount = 1
	log.Printf("%x became candidate at term %d", rf.me, rf.currTerm)
	// todo
}
