package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// KeyValue Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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
	for {
		reply, _ := w.askTask()
		switch reply.TaskType {
		case Map:
			log.Printf("worker 开始处理map任务：%d\n", reply.FileId)
			// lab提供的map操作
			var intermediate []KeyValue
			filename := reply.Filename
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
			// 声明临时文件和编码器
			outFiles := make([]*os.File, reply.NReduce)
			outFileEncoders := make([]*json.Encoder, reply.NReduce)
			// 创建临时文件
			for i := 0; i < reply.NReduce; i++ {
				fileName := "mr-twins-out-tmp-" + strconv.Itoa(reply.FileId) + "-" + strconv.Itoa(i)
				os.Remove(fileName)
				create, err := os.Create(fileName)
				if err != nil {
					log.Fatalf("%v", err)
				}
				outFiles[i] = create
				outFileEncoders[i] = json.NewEncoder(outFiles[i])
			}
			for _, kv := range intermediate {
				i := ihash(kv.Key) % reply.NReduce
				file = outFiles[i]
				enc := outFileEncoders[i]
				enc.Encode(&kv)
			}
			w.submitTask(reply.FileId)
		case Reduce:
			var kva []KeyValue
			for i := 0; i < reply.FileNum; i++ {
				filePath := "mr-twins-out-tmp-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.NReduceId)
				file, _ := os.Open(filePath)
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}
			sort.Sort(ByKey(kva))
			oname := "mr-out-" + strconv.Itoa(reply.NReduceId)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}

			ofile.Close()
			w.submitTask(reply.NReduceId)
		case Done:
			os.Exit(1)
		}
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

func (w *worker) submitTask(taskId int) (*Reply, error) {
	args := RequestArgs{
		ReqType: Submit,
		TaskId:  taskId,
	}
	reply := Reply{}

	ok := call("Coordinator.RpcReq", &args, &reply)
	if ok && reply.Reply {
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
