package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	// generate a random worker ID
	uuidWithHyphen := uuid.New()
	workerID := strings.Replace(uuidWithHyphen.String(), "-", "", -1)
	if DebugPrints {
		fmt.Printf("Worker %s launched\n", workerID)
	}
	//simple logic: request work, get work done, handin work
	for {
		// periodically request task
		task := Task{}
		for task.TaskType == "" {
			task = requestTask(workerID)
			time.Sleep(50 * time.Millisecond)
		}
		// finish task
		if task.TaskType == "Map" {
			executeMap(&task, mapf)
		}
		if task.TaskType == "Reduce" {
			executeReduce(&task, reducef)
		}
		if task.TaskType == "Please exit" {
			return
		}
		// hand in task
		if handinTask(workerID, task) == "Please exit" {
			return
		}
	}
}

//
// method to work on map tasks, update the task filenames
//
func executeMap(task *Task, mapf func(string, string) []KeyValue) {
	// map task has only one filename
	filename := task.TaskFile[0]
	nReduce := task.TaskR
	x := task.TaskX
	ofilenames := []string{}
	ofiles := make([]*os.File, nReduce)
	oenc := make([]*json.Encoder, nReduce)
	for i := 0; i < nReduce; i++ {
		ofiles[i], _ = ioutil.TempFile("", "mr-tmp-*")
		oenc[i] = json.NewEncoder(ofiles[i])
	}
	// read from input file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// map
	kva := mapf(filename, string(content))
	// distribute and output to temp files
	for _, kv := range kva {
		i := ihash(kv.Key) % nReduce
		err := oenc[i].Encode(&kv)
		if err != nil {
			panic("Encode failure")
		}
	}
	// rename when finished
	for i, ofile := range ofiles {
		oname := "mr-" + strconv.Itoa(x) + "-" + strconv.Itoa(i)
		ofilenames = append(ofilenames, oname)
		temp := filepath.Join(ofile.Name())
		os.Rename(temp, oname)
		ofile.Close()
	}
	// fmt.Printf("File %v Map task result: %v\n", filename, ofilenames)
	task.TaskFile = ofilenames
}

//
// method to work on reudce tasks, do not modify task objects
//
func executeReduce(task *Task, reducef func(string, []string) string) {
	// read from input files
	filenames := task.TaskFile
	x := task.TaskX
	kva := []KeyValue{}

	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	// sort
	sort.Sort(ByKey(kva))
	// scan and merge keys
	ofilename := "mr-out-" + strconv.Itoa(x)
	ofile, _ := os.Create(ofilename)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	ofile.Close()
}

//
// method to request task from server
//
func requestTask(workerID string) Task {
	args := RequestTaskArgs{workerID}
	reply := RequestTaskReply{Task{}}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		if reply.AssignedTask.TaskType != "" {
			if DebugPrints {
				fmt.Printf("Worker %s got %s %d task\n", workerID, reply.AssignedTask.TaskType, reply.AssignedTask.TaskX)
			}
		}
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply.AssignedTask
}

//
// method to handin task to server
//
func handinTask(workerID string, task Task) string {
	args := FinishedTaskArgs{workerID, task}
	reply := FinishedTaskReply{}
	ok := call("Coordinator.ReceiveTask", &args, &reply)
	if ok {
		if DebugPrints {
			fmt.Printf("Worker %s completed %s %d task\n", workerID, task.TaskType, task.TaskX)
		}
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply.Message
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
