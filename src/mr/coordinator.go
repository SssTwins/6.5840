package mr

import (
	"log"
	"sync"
	"time"
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
	reduceNum int

	// 文件的map阶段任务处理完成数量
	mapFinished uint

	// 文件的reduce阶段任务处理完成数量
	reduceFinished uint

	// 请求锁
	mux sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// RpcReq an example RPC handler.
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
			return c.mapAskHandle(reply)
		case Submit:
			return c.mapSubmitHandle(args, reply)
		default:
			reply.Reply = false
			reply.TaskType = NoTask
			return nil
		}
	case Reduce:
		c.mux.Lock()
		defer c.mux.Unlock()
		switch args.ReqType {
		case Ask:
			for i := range c.reduceFilesState {
				if c.reduceFilesState[i] == Idle {
					c.reduceFilesState[i] = InProcess
					reply.NReduce = c.reduceNum
					reply.TaskType = Reduce
					reply.FileId = i
					reply.NReduceId = i
					reply.FileNum = len(c.inputFile)
					log.Printf("请求处理reduce任务：%d\n", i)
					go func(i int, c *Coordinator) {
						time.Sleep(time.Duration(10) * time.Second)
						c.mux.Lock()
						if c.reduceFilesState[i] == InProcess {
							c.reduceFilesState[i] = Idle
						}
						c.mux.Unlock()
					}(i, c)
					return nil
				}
			}
			reply.TaskType = NoTask
			return nil
		case Submit:
			if c.reduceFilesState[args.TaskId] == InProcess {
				c.reduceFilesState[args.TaskId] = Success
				reply.Reply = true
				log.Printf("完成reduce任务：%d\n", args.TaskId)
				c.reduceFinished++
				if int(c.reduceFinished) == c.reduceNum {
					c.state = Done
				}
				return nil
			}
			reply.Reply = false
			return nil
		default:
			reply.Reply = false
			reply.TaskType = NoTask
			return nil
		}
	case Done:
		reply.TaskType = Done
		return nil
	}
	return nil
}

func (c *Coordinator) mapSubmitHandle(args *RequestArgs, reply *Reply) error {
	if c.mapFilesState[args.TaskId] == InProcess {
		c.mapFilesState[args.TaskId] = Success
		reply.Reply = true
		c.mapFinished++
		if int(c.mapFinished) == len(c.inputFile) {
			c.state = Reduce
		}
		log.Printf("完成map任务：%d\n", args.TaskId)
		return nil
	}
	reply.Reply = false
	return nil
}

func (c *Coordinator) mapAskHandle(reply *Reply) error {
	for i := range c.mapFilesState {
		if c.mapFilesState[i] == Idle {
			c.mapFilesState[i] = InProcess
			reply.Filename = c.inputFile[i]
			reply.NReduce = c.reduceNum
			reply.TaskType = Map
			reply.FileId = i
			log.Printf("请求处理map任务：%d\n", i)
			go func(i int, c *Coordinator) {
				time.Sleep(time.Duration(10) * time.Second)
				c.mux.Lock()
				if c.mapFilesState[i] == InProcess {
					c.mapFilesState[i] = Idle
				}
				c.mux.Unlock()
			}(i, c)
			return nil
		}
	}
	reply.TaskType = NoTask
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

// Done main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.state == Done
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		state:            Map,
		inputFile:        files,
		mapFilesState:    make([]TaskState, len(files)),
		reduceFilesState: make([]TaskState, nReduce),
		mapNum:           uint(len(files)),
		reduceNum:        nReduce,
		mapFinished:      0,
		reduceFinished:   0,
		mux:              sync.Mutex{},
	}
	for i := 0; i < len(files); i++ {
		c.mapFilesState[i] = Idle
	}
	for i := 0; i < nReduce; i++ {
		c.reduceFilesState[i] = Idle
	}
	c.server()
	log.Println("start coordinator")
	return &c
}
