package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {

	// 协调器的状态，分发map还是reduce
	state CoordinatorState

	inputFile []string

	// map阶段任务处理状态
	mapFilesState []TaskState

	// reduce阶段文件处理状态
	reduceFilesState []TaskState

	// 需要处理的文件数量
	mapNum uint

	// nReduce
	reduceNum uint

	// 文件的map阶段任务处理完成数量
	mapFinished uint

	// 文件的reduce阶段任务处理完成数量
	reduceFinished uint

	// 请求锁
	mux sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) RpcReq(args *RequestArgs, reply *Reply) error {
	switch c.state {
	case Map:
		c.mux.Lock()
		defer c.mux.Unlock()
		// 请求任务
		switch args.ReqType {
		case Ask:
			for i := range c.mapFilesState {
				if c.mapFilesState[i] == Idle {
					c.mapFilesState[i] = InProcess
					reply.Filename = c.inputFile[i]
					reply.NReduce = c.reduceNum
					reply.TaskType = Map
					reply.TaskId = i
					return nil
				}
			}
			reply.TaskType = NoTask
			return nil
		case Submit:
			c.mapFilesState[args.TaskId] = Success
			reply.Reply = true
			return nil
		default:
			reply.Reply = false
			reply.TaskType = NoTask
			return nil
		}
	case Reduce:
		c.mux.Lock()
		defer c.mux.Unlock()
	case Done:
		return nil
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.state == Done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		state:            Map,
		inputFile:        files,
		mapFilesState:    make([]TaskState, len(files)),
		reduceFilesState: make([]TaskState, len(files)),
		mapNum:           uint(len(files)),
		reduceNum:        uint(nReduce),
		mapFinished:      0,
		reduceFinished:   0,
		mux:              sync.Mutex{},
	}
	c.server()
	log.Println("start coordinator")
	return &c
}
