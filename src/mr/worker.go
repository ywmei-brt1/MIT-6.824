package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	for {
		t := getTask()
		if t.Err != nil {
			fmt.Printf("Coordinator has no more jobs, worker can terminate: %v\n", t.Err)
			return
		}
		if t.WorkerNum == -1 {
			fmt.Printf("Worker need to check with Coordinator again after 30s to see if there are still some jobs to do.\n")
			time.Sleep(30 * time.Second)
			continue
		}
		fmt.Printf("Assigned a task: %v\n", t.FileName)
		// Split all the intermediate map result into nReduce buckets.
		mapResults, err := calInterMapResults(t, mapf)
		if err != nil {
			finishTask(t, err)
			time.Sleep(30 * time.Second)
			continue
		}
		fmt.Printf("Save intermediate results to Files: %v\n", t.FileName)
		err = saveInterMapToFiles(t, mapResults)
		if err != nil {
			finishTask(t, err)
			time.Sleep(30 * time.Second)
			continue
		}
		time.Sleep(10 * time.Second)
		finishTask(t, err)
		fmt.Printf("Assigned a task: %v finished. Already informed Coordinator.\n", t.FileName)
		fmt.Printf("==============================================================\n")

	}
}

// Slice are passed by reference by default. So the caller will have the modified copy.
func calInterMapResults(t *Task, mapf func(string, string) []KeyValue) ([][]KeyValue, error) {
	filename := t.FileName
	nReduce := t.NReduce
	var mapResults [][]KeyValue
	for i := 0; i < t.NReduce; i++ {
		mapResults = append(mapResults, []KeyValue{})
	}
	var intermediate []KeyValue
	file, err := os.Open(filename)
	if err != nil {
		//log.Fatalf("cannot open %v", filename)
		return nil, err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		//log.Fatalf("cannot read %v", filename)
		return nil, err
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	sort.Sort(ByKey(intermediate))
	for _, kv := range intermediate {
		mapResults[ihash(kv.Key)%nReduce] = append(mapResults[ihash(kv.Key)%nReduce], kv)
	}
	return mapResults, nil
}

func saveInterMapToFiles(t *Task, mapResults [][]KeyValue) error {
	x := strconv.Itoa(t.WorkerNum)
	// Create the intermediate files and save the result inside.
	for i := 0; i < t.NReduce; i++ {
		y := strconv.Itoa(i)
		filename := "mr-" + x + "-" + y
		file, err := os.Create(filename)
		if err != nil {
			//log.Fatalf("cannot create %v", filename)
			return err
		}
		enc := json.NewEncoder(file)
		for _, kv := range mapResults[i] {
			err = enc.Encode(&kv)
			if err != nil {
				//log.Fatalf("cannot write to %v", filename)
				return err
			}
		}
		file.Close()
	}
	return nil
}

// getTask get a Task from coordinator.
func getTask() *Task {
	args := ExampleArgs{}
	t := Task{}
	call("Coordinator.SendTask", args, &t)
	return &t
}

// finishTask tell coordinator a task has been finished from the worker side.
func finishTask(t *Task, err error) {
	reply := ExampleReply{}
	f := Finished{t.FileName, t.WorkerNum, err}
	call("Coordinator.FinishTask", &f, &reply)
	return
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
// A simple way to implement this is if the worker got a false value here, it
// can assume the coordinator had finished the job. So the worker can terminate.
// A better design is when coordinator finished the job, send "pleaseExit" task
// to the workers so that the worker terminate right away.
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
