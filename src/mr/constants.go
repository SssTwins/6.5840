package mr

type CoordinatorState int

type TaskState int

type ReqType int

const (
	Map CoordinatorState = iota
	Reduce
	NoTask
	Done
)

const (
	Idle TaskState = iota
	InProcess
	Success
)

const (
	Ask ReqType = iota
	Submit
)
