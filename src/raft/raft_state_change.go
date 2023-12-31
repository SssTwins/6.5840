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
	log.Printf("%x became leader at term %d", rf.me, rf.currTerm)
}

func (rf *Raft) restRandElectionTimeoutTick() {
	rf.randElectionTimeoutTick = rf.electionTickTimeout + rand.Intn(rf.electionTickTimeout)
}
