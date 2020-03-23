package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var workerId int
var nMap int
var nReduce int

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()
	register()
	var prev *TaskRPC = nil
	for {
		task, err := RequestTask(workerId, prev)
		if err != nil {
			log.Fatalf("Worker %v RequestTask failed with error: %v", workerId, err)
		} else if task == nil {
			log.Printf("Worker %v No more task. Worker exit.", workerId)
			break
		} else {
			if task.Phase == Map {
				executeMapTask(task.Index, task.Input, mapf)
			} else {
				executeReduceTask(task.Index, reducef)
			}

			prev = task
		}
	}
}

func executeMapTask(index int, input string, mapf func(string, string) []KeyValue) {
	kvs := doMap(input, mapf)
	saveMapOutput(index, kvs)
	time.Sleep(time.Second)
}

func doMap(input string, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(input)
	if err != nil {
		log.Fatalf("[Map] Worker %v failed to open file %v", workerId, input)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("[Map] Worker %v failed to read file %v", workerId, input)
	}
	file.Close()

	return mapf(input, string(content))
}

func saveMapOutput(index int, allKV []KeyValue) {
	hashedKV := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		hashedKV[i] = []KeyValue{}
	}
	for _, kv := range allKV {
		bucket := ihash(kv.Key) % nReduce
		hashedKV[bucket] = append(hashedKV[bucket], kv)
	}

	for i, kvs := range hashedKV {
		filename := fmt.Sprintf("mr-%v-%v", index, i)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("[Map] Worker %v failed to create file %v", workerId, filename)
		}
		encoder := json.NewEncoder(file)
		for _, kv := range kvs {
			err = encoder.Encode(&kv)
			if err != nil {
				log.Fatalf("[Map] Worker %v failed to write key %v :  value %v", workerId, kv.Key, kv.Value)
			}
		}
		file.Close()
	}
}

func executeReduceTask(index int, reducef func(string, []string) string) {
	kvs := gatherIntermediates(index)
	result := doReduce(kvs, reducef)
	saveReduceOutput(index, result)
}

func gatherIntermediates(index int) []KeyValue {
	var results []KeyValue
	for i := 0; i < nMap; i++ {
		kvs := readIntermediate(i, index)
		results = append(results, kvs...)
	}
	sort.Sort(ByKey(results))

	return results
}

func readIntermediate(mapIndex int, reduceIndex int) []KeyValue {
	filename := fmt.Sprintf("mr-%v-%v", mapIndex, reduceIndex)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("[Reduce] Worker %v failed to open file %v", workerId, filename)
	}

	decoder := json.NewDecoder(file)
	var kvs []KeyValue
	for {
		var kv KeyValue
		err := decoder.Decode(&kv)
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("[Reduce] Worker %v failed to decode JSON in file %v", workerId, filename)
		} else {
			kvs = append(kvs, kv)
		}
	}

	file.Close()
	return kvs
}

func doReduce(sortedKVs []KeyValue,
	reducef func(string, []string) string) []KeyValue {
	n := len(sortedKVs)
	var results []KeyValue
	i := 0
	for i < n  {
		key := sortedKVs[i].Key
		var values []string
		var j int
		for j = i; j < n && sortedKVs[j].Key == key; j++ {
			values = append(values, sortedKVs[j].Value)
		}
		output := reducef(key, values)
		results = append(results, KeyValue{key, output})
		i = j
	}
	return results
}

func saveReduceOutput(index int, output []KeyValue) {
	filename := fmt.Sprintf("mr-out-%v", index)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("[Reduce] Worker %v failed to create output file %v", workerId, filename)
	}

	for _, kv := range output {
		_, err := fmt.Fprintf(file, "%v %v\n", kv.Key, kv.Value)
		if err != nil {
			log.Fatalf("[Reduce] Worker %v failed to write key %v: value %v", workerId, kv.Key, kv.Value)
		}
	}

	file.Close()
}

func RequestTask(workerId int, prev *TaskRPC) (*TaskRPC, error) {
	args := RequestTaskArgs{prev, workerId}
	reply := RequestTaskReply{}

	err := call("Master.RequestTask", &args, &reply)
	if err != nil {
		return nil, err
	} else {
		return reply.Task, nil
	}
}

func register() {
	workerId = os.Getpid()
	args := RegisterWorkerArgs{workerId}
	reply := RegisterWorkerReply{}
	err := call("Master.RegisterWorker", &args, &reply)
	if err != nil {
		log.Fatalln("Register failed: ", err)
	}

	nMap = reply.MapCount
	nReduce = reply.ReduceCount
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	return err
}
