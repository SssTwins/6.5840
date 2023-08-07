package raft

import (
	"log"
	"math/rand"
)

func (rf *Raft) becomeFollower(term uint64, leader int) {
	rf.tickReset(rf.currTerm)
	rf.state = Follower
	rf.currTerm = term
	rf.leader = leader
	rf.votedFor = -1
	rf.tick = rf.tickElection
	rf.restRandElectionTimeoutTick()
	log.Printf("%x became follower at term %d", rf.me, rf.currTerm)
}

func (rf *Raft) becomeCandidate() {
	rf.tickReset(rf.currTerm)
	rf.state = Candidate
	rf.tick = rf.tickElectionTimeout
	rf.restRandElectionTimeoutTick()
	rf.votedFor = rf.me
	rf.currTerm++
	rf.voteCount = 1
	log.Printf("%x became candidate at term %d", rf.me, rf.currTerm)
}

func (rf *Raft) becomeLeader() {
	rf.tickReset(rf.currTerm)
	rf.state = Leader
	rf.tick = rf.tickHeartbeat
	rf.restRandElectionTimeoutTick()
	rf.leader = rf.me
	for i := range rf.peers {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	log.Printf("%x became leader at term %d", rf.me, rf.currTerm)
}

func (rf *Raft) restRandElectionTimeoutTick() {
	r := rand.New(rand.NewSource(int64(rf.me)))
	rf.randElectionTimeoutTick = rf.electionTickTimeout + r.Intn(rf.electionTickTimeout)
}
