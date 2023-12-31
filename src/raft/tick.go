package raft

import "time"

// ticker 计时器，5ms一次tick
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		rf.tick()
		rf.mu.Unlock()
		time.Sleep(time.Duration(5) * time.Millisecond)
	}
}

// follower的选举超时方法，此方法会触发选举
func (rf *Raft) tickElection() {
	rf.electionTick++
	if rf.electionTick >= rf.randElectionTimeoutTick {
		rf.electionTick = 0
		rf.msgCh <- RfMsg{mt: MsgElection}
	}
}

// candidate的选举超时方法，此方法会转变为follower
func (rf *Raft) tickElectionTimeout() {
	rf.electionTick++
	if rf.electionTick >= rf.randElectionTimeoutTick {
		rf.electionTick = 0
		rf.msgCh <- RfMsg{mt: MsgElectionTimeout}
	}
}

func (rf *Raft) tickHeartbeat() {
	rf.heartbeatTick++
	if rf.state != Leader {
		return
	}
	if rf.heartbeatTick >= rf.heartbeatTickTimeout {
		rf.heartbeatTick = 0
		rf.msgCh <- RfMsg{mt: MsgAppendEntries}
	}
}

// ticker 超时之后重置状态
func (rf *Raft) tickReset(term uint64) {
	if rf.currTerm != term {
		rf.currTerm = term
		rf.votedFor = -1
		rf.voteCount = 0
	}
	rf.leader = -1
	rf.electionTick = 0
	rf.heartbeatTick = 0
}
