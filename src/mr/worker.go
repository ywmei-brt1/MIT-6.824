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
	"sync"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
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
	// Your worker implementation here.
	args := JobArgs{}
	for {
		reply := JobReply{}
		CallJob(&args, &reply)
		if reply.Type == "Done" {
			break
		}
		switch reply.Type {
		case "Map":
			args = DoMap(&reply, mapf)
		case "Reduce":
			args = DoReduce(&reply, reducef)
		}
	}
	return
}

// DoMap generate a list of intermediate files, for the Xth mapper, it generates
// intermediate files like mr-x-ihash, mr-x-ihash, ...
func DoMap(job *JobReply, mapf func(string, string) []KeyValue) JobArgs {
	// generate the content for the intermediate files.
	contents := make([][]KeyValue, int(job.NReduce))
	filename := job.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	defer file.Close()
	kva := mapf(filename, string(content))
	for _, kv := range kva {
		hash := ihash(kv.Key) % int(job.NReduce)
		contents[hash] = append(contents[hash], kv)
	}
	wg := sync.WaitGroup{}
	// create a list of intermediate files, and write result into them
	mapperID := job.WorkerID
	intermediateFiles := make([]string, int(job.NReduce))
	for i := 0; i < int(job.NReduce); i++ {
		wg.Add(1)
		go func(i int) {
			filename := fmt.Sprintf("mr-%v-%v", mapperID, i)
			file, err := os.Create(filename)
			defer file.Close()
			if err != nil {
				log.Fatalf("cannot create %v", filename)
			}
			enc := json.NewEncoder(file)
			for _, kv := range contents[i] {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode %v", filename)
				}
			}
			intermediateFiles[i] = filename
			wg.Done()
		}(i)
	}
	wg.Wait()
	return JobArgs{
		Type:      job.Type,
		Filename:  job.Filename,
		TempFiles: intermediateFiles,
	}
}

func DoReduce(job *JobReply, reducef func(string, []string) string) JobArgs {
	reducerID := job.WorkerID
	// open all the intermediate files, read them back to kva
	intermediateFiles := job.TempFiles
	var intermediate []KeyValue
	for _, filename := range intermediateFiles {
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
			intermediate = append(intermediate, kv)
		}
	}

	// create a file to save to aggregated output.
	filename := fmt.Sprintf("mr-temp-out-%v", reducerID)
	os.Create(filename)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("cannot create %v", filename)
	}
	defer file.Close()

	sort.Sort(ByKey(intermediate))
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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	return JobArgs{
		Type:     job.Type,
		Filename: job.Filename,
		TempFiles: []string{
			filename,
		},
	}
}

func CallJob(args *JobArgs, reply *JobReply) {
	call("Coordinator.AssignTask", args, reply)
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
