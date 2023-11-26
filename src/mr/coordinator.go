package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// Coordinator can be initialized with a list of files, and n, the number of reduce tasks
// That is to say, there are unlimited number of map task, as long as there are worker available.
// However, each worker only supposed to generate n output files. mr-X-0, mr-X-1, ... mr-X-(n-1)
//
// 1) The coordinator will hold a channel of all the input files.
// When there is a worker available, the worker will ask coordinator to assign a map task (a file) to it.
// 2) The coordinator will also need to record all the in-flight tasks and check the health of the workers periodically.
// If a worker died, the task will be added back to the channel, so that another healthy worker can pick it up.
//
//	 Q: If a worker, say worker 5, successfully finished a task, and generate out mr-5-0,
//		but died while generating mr-5-100, how to keep mr-5-0 but delete the partially finished mr-5-100?
//
//		A: We will rename the worker, next time it get named as worker 6, so, ideally, when coordinator detects worker 6 died,
//		before coordinator reassign worker 6's input file, coordinator will clean up worker 6's staled output files. But in this lab,
//		we don't need to. Coordinator can have a field to record all the finished intermediate files.
//		The reduce job is only going to work on these intermediate files.
type Coordinator struct {
	// Your definitions here.
	// Mutex lock that avoid data racing
	// TODO(ywmei): When to use mutex, when to use sync.Map?
	mu sync.Mutex
	// nReduce is the number of reduce jobs (col numbers)
	nReduce int
	// nMap is the number of map jobs (row numbers)
	nMap int
	// mr is the mapping between input file names to the num of worker do the map job.
	mr map[string]int
	// A channel of all the files that need to be mapped
	files chan string
	// A hashmap of all the in-flight map jobs. key is the map-worker idx (int), value is the file name (string) that the worker is working on.
	inFlightMapJobs map[string]bool
	// A hashmap of finished map jobs, where key is the file name string that has been finished, value is the map-worker idx (int).
	finishedMapJobs map[string]bool
	// A hashmap of all the in-flight reduce jobs. key is the reduce-worker idx (int), value is the file name (string) that the worker is working on.
	inFlightReduceJobs map[string]bool
	// A hashmap of finished reduced jobs, where key is the file name string that has been finished, value is the reduce-worker idx (int).
	finishedReduceJobs map[string]bool
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:            nReduce,
		nMap:               len(files),
		mr:                 map[string]int{},
		files:              make(chan string, len(files)),
		inFlightMapJobs:    make(map[string]bool),
		finishedMapJobs:    make(map[string]bool),
		inFlightReduceJobs: make(map[string]bool),
		finishedReduceJobs: make(map[string]bool),
	}
	for i, f := range files {
		c.files <- f
		c.mr[f] = i
	}
	fmt.Printf("There are totally %v input files.\n", len(files))
	// Your code here.
	c.server()
	return &c
}

// Your code here -- RPC handlers for the worker to call.

// SendTask will send map task to worker who is requesting for a job.
// If there is no more map tasks, it will send reduce task to the worker.
// If there is no available map task, but map phase haven't finished yet. It will ask the worker come back and check after 1 second.
// If there is no available reduce task, but reduce phase haven't finished yet. It will ask the worker come back and check after 1 second.
func (c *Coordinator) SendTask(args *ExampleArgs, t *Task) error {
	// read operations are always concurrent-safe in GoLang.
	if len(c.files) > 0 {
		t.FileName = <-c.files
		t.WorkerNum = c.mr[t.FileName]
		t.NReduce = c.nReduce
		return nil
	}
	fmt.Printf("len(c.finishedMapJobs) vs c.nMap : %v, %v \n", len(c.finishedMapJobs), c.nMap)
	fmt.Printf("in flight map jobs are: %+v \n", c.inFlightMapJobs)
	// map job not finished, ask the worker to come back latter
	if len(c.finishedMapJobs) != c.nMap {
		// TODO: tell the worker come back latter
		t.FileName = "ComeBackLatter"
		t.WorkerNum = -1
		t.NReduce = c.nReduce
		return nil
	}
	// NOW need to do health check and make sure we terminate slow workers.
	// also need to send reduce task from here?
	//t.Err = fmt.Errorf("no more task")
	t.WorkerNum = -1
	return nil
}

func (c *Coordinator) FinishTask(f *Finished, reply *ExampleReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if f.Err != nil {
		c.files <- f.FileName
		return nil
	}
	delete(c.inFlightMapJobs, f.FileName)
	c.finishedMapJobs[f.FileName] = true
	fmt.Printf("Map Job: %v has been finished by worker %v.\n", f.FileName, f.WorkerNum)
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 2
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
	ret := false

	// Your code here.

	return ret
}
