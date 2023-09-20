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
	"6.5840-twins/src/labrpc"
	"io"
	"log"
	"sync"
	"sync/atomic"
	//	"6.5840/labgob"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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
	Command interface{}
	Term    uint64
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state

	// this peer's index into peers[] 同时作为raft节点id
	me int

	// set by Kill()
	dead int32

	// Your data here (2A, 2B, 2C).

	// raft节点的状态
	state State

	// 当前节点所处任期
	currTerm uint64

	// 当前任期投票给了谁，未投票设置为-1
	votedFor int

	// 当前任期获取的投票数量
	voteCount int

	applyCh chan ApplyMsg

	// 当前任期的leader peer's index -1表示集群暂未存在leader
	leader int

	// 超时方法
	tick func()

	// 心跳计数
	heartbeatTick int

	// 选举tick计数
	electionTick int

	// 随机的选举超时计数
	randElectionTimeoutTick int

	// 心跳超时计数限界
	heartbeatTickTimeout int

	// 选举超时计数限界
	electionTickTimeout int

	// 消息通知channel
	msgCh chan RfMsg

	// 被提交日志最大索引
	commitIndex int

	// 已应用到状态机的最大索引
	lastApply int

	// 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	nextIndex []int

	// 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为-1，单调递增）
	matchIndex []int

	// 日志条目
	log []LogEntry
}

type State = uint8

// Raft节点三种状态：Follower、Candidate、Leader
const (
	Follower State = iota
	PreCandidate
	Candidate
	Leader
)

type MsgType uint8

const (
	// MsgElection 触发选举
	MsgElection MsgType = iota

	// MsgElectionTimeout 等待选举投票超时
	MsgElectionTimeout

	// MsgBeElected 当选
	MsgBeElected

	// MsgAppendEntries 发送心跳消息
	MsgAppendEntries

	// MsgAppendEntriesOk 心跳成功消息
	MsgAppendEntriesOk
)

type RfMsg struct {
	mt MsgType
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := int(rf.currTerm)
	isLeader := rf.state == Leader
	rf.mu.Unlock()
	// Your code here (2A).
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := rf.state == Leader
	if isLeader {
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{
			Command: command,
			Term:    rf.currTerm,
		})
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: len(rf.log) - 1,
		}
		term = int(rf.currTerm)
		rf.msgCh <- RfMsg{mt: MsgAppendEntries}
	}
	// Your code here (2B).
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	log.SetFlags(log.Llongfile | log.Lmicroseconds | log.Ldate)
	//log.SetOutput(os.Stderr)
	log.SetOutput(io.Discard)
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		applyCh:   applyCh,
		msgCh:     make(chan RfMsg, 100),
		// 每5ms一次tick计数
		electionTickTimeout:  30,
		heartbeatTickTimeout: 20,
		// 日志索引记录
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
		log: []LogEntry{{
			Term: 0,
		}},
	}
	for i := range rf.peers {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.becomeFollower(0, -1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.loop()
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func (rf *Raft) loop() {
	for rf.killed() == false {
		select {
		case msg := <-rf.msgCh:
			switch msg.mt {
			case MsgElection:
				rf.electionHandle()
			case MsgBeElected:
				rf.electedHandle()
			case MsgElectionTimeout:
				rf.electionTimeoutHandle()
			case MsgAppendEntries:
				rf.sendAppendEntriesHandle()
			case MsgAppendEntriesOk:
				rf.appendEntriesOkHandle()
			}
		}
	}
}
