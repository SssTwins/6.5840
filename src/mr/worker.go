package mr

import (
	"fmt"
	"io"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type WorkerState int

const (
	WorkerIdle WorkerState = iota
	WorkerProcess
)

type mapFunc func(string, string) []KeyValue
type reduceFunc func(string, []string) string
type worker struct {

	// map方法
	mapf mapFunc

	// reduce 方法
	reducef reduceFunc
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := worker{
		mapf:    mapf,
		reducef: reducef,
	}
	reply, _ := w.askTask()
	if reply.TaskType == Map {
		var intermediate []KeyValue
		for _, filename := range os.Args[2:] {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := w.mapf(filename, string(content))
			intermediate = append(intermediate, kva...)
		}

		//
		// a big difference from real MapReduce is that all the
		// intermediate data is in one place, intermediate[],
		// rather than being partitioned into NxM buckets.
		//

		sort.Sort(ByKey(intermediate))

		oname := "mr-out-0"
		ofile, _ := os.Create(oname)

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := w.reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		ofile.Close()
	}
	os.Exit(1)
}

func (w *worker) askTask() (*Reply, error) {
	args := RequestArgs{
		ReqType: Ask,
	}
	reply := Reply{}

	ok := call("Coordinator.RpcReq", &args, &reply)
	if ok {
		return &reply, nil
	} else {
		return nil, fmt.Errorf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
