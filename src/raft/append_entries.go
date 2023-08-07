package raft

type AppendEntriesArgs struct {

	// 当前leader term
	Term uint64

	// 当前 leader id
	LeaderId int

	// 紧邻新日志条目之前的那个日志条目的索引
	PrevLogIndex int

	// 紧邻新日志条目之前的那个日志条目的任期
	PrevLogTerm uint64

	// 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	Entries []interface{}

	// 领导人的已知已提交的最高的日志条目的索引
	LeaderCommit int
}

type AppendEntriesReply struct {

	// 当前 follower term
	Term uint64

	// 心跳请求是否成功
	Success bool
}

func (rf *Raft) sendHeartbeat(index int, ae *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[index].Call("Raft.AppendEntries", ae, reply)
	return ok
}

// AppendEntries  rpc method
func (rf *Raft) AppendEntries(ae *AppendEntriesArgs, reply *AppendEntriesReply) {

	// 如果leader节点term小于follower节点，不做处理并返回
	if ae.Term < rf.currTerm {
		reply.Term = rf.currTerm
		return
	}

	// 如果leader节点term大于follower节点
	// 说明 follower 过时，重置follower节点term
	if ae.Term > rf.currTerm {
		rf.currTerm = ae.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	// 将当前follower节点term返回给leader
	reply.Term = rf.currTerm

	reply.Success = true
	reply.Term = rf.currTerm
	rf.leader = ae.LeaderId
	// 心跳成功，发送消息
	rf.msgCh <- RfMsg{mt: MsgAppendEntriesOk}
	return
}
