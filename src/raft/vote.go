package raft

import "log"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {

	// 当前任期id
	Term uint64

	// 请求的候选人id
	CandidateId int

	// 候选人的最后日志条目的索引值
	LastLogIndex int

	// 候选人最后日志条目的任期号
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {

	// 当前任期号，以便于候选人去更新自己的任期号
	Term uint64

	// 回应为true则获得投票
	Reply bool
}

// RequestVote example RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果Candidate节点term小于follower，或二者term相同但是Candidate节点日志索引小于follower
	// 说明Candidate节点过时，拒绝投票
	if args.Term < rf.currTerm {
		reply.Term = rf.currTerm
		reply.Reply = false
		return
	}

	// 未投票并且候选人的日志至少和自己一样新，那么就投票给他
	if rf.votedFor == -1 {
		rf.currTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.Term = rf.currTerm
		reply.Reply = true
		return
	}
	if rf.state == Leader && args.Term > rf.currTerm {
		rf.becomeFollower(args.Term, -1)
	}

	reply.Term = rf.currTerm
	reply.Reply = false
	return
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus, there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendRequestVoteAndHandle(server int, vote *RequestVoteArgs) {
	var reply RequestVoteReply
	// 发送申请到某个节点
	requestVote := rf.sendRequestVote(server, vote, &reply)
	if requestVote {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 如果candidate节点term小于follower节点
		// 当前candidate节点无效
		// candidate节点转变为follower节点
		if reply.Term > rf.currTerm {
			rf.becomeFollower(reply.Term, -1)
			return
		}

		if reply.Reply {
			if rf.state == Leader {
				return
			}
			rf.voteCount++
			log.Printf("当前任期: %d, 节点id: %d, 票数: %d, 状态: %x,", rf.currTerm, rf.me, rf.voteCount, rf.state)
			if rf.state == Candidate && rf.voteCount > len(rf.peers)/2 {
				rf.msgCh <- RfMsg{mt: MsgBeElected}
			}
		}
	}
}
