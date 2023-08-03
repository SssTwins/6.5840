package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RequestArgs struct {

	// 请求类型
	ReqType ReqType

	// 完成的任务id，map任务是文件的id，reduce任务是nReduce分片id
	TaskId int
}

type Reply struct {

	// 需要执行的任务类型
	TaskType CoordinatorState

	// 文件id，map任务是文件的id
	FileId int

	// nReduce分片id
	NReduceId int

	// 文件名称
	Filename string

	// reduce分片数
	NReduce int

	// 文件数
	FileNum int

	// 提交任务请求的专用返回值
	Reply bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
