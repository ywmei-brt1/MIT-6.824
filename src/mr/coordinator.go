package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//go get -u go.uber.org/zap
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *zap.Logger

func init() {
	config := zap.NewProductionConfig() // Or zap.NewDevelopmentConfig()
	config.OutputPaths = []string{"mr-coordinator.log"}
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	Logger, _ = config.Build()
}

type Coordinator struct {
	nFiles     int32
	nReduce    int32
	mapperCnt  atomic.Int32
	reducerCnt atomic.Int32
	mutex      sync.Mutex

	mapJobs               chan *JobReply
	finishedMapJobs       sync.Map
	finishedMapJobsCounts atomic.Int32

	reduceJobs               chan *JobReply
	finishedReduceJobs       sync.Map
	finishedReduceJobsCounts atomic.Int32

	doneJobs chan bool    // when this channel is closed, neither mapper nor reducer is needed anymore.
	done     atomic.Int32 // greater than zero if coordinator can be terminated
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
	return c.done.Load() != 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:    int32(nReduce),
		nFiles:     int32(len(files)),
		mapJobs:    make(chan *JobReply, len(files)),
		reduceJobs: make(chan *JobReply, nReduce),
		doneJobs:   make(chan bool),
	}
	for _, f := range files {
		Logger.Info("Registering file to coordinator", zap.String("file:", f))
		mapJob := JobReply{
			Type:     "Map",
			Filename: f,
			NReduce:  c.nReduce,
		}
		c.mapJobs <- &mapJob
	}
	c.server()
	return &c
}

// AssignTask tells a worker what to do.
func (c *Coordinator) AssignTask(args *JobArgs, reply *JobReply) error {
	c.acknowledge(args)
	c.dispatchTask(reply)
	return nil
}

func (c *Coordinator) acknowledge(args *JobArgs) {
	switch args.Type {
	case "":
		return
	case "Map":
		c.acknowledgeMap(args)
	case "Reduce":
		c.acknowledgeReduce(args)
	}
}

func (c *Coordinator) acknowledgeMap(args *JobArgs) {
	if _, ok := c.finishedMapJobs.Load(args.Filename); !ok {
		c.finishedMapJobs.Store(args.Filename, args.TempFiles)
		if c.finishedMapJobsCounts.Add(1) == c.nFiles {
			c.mutex.Lock()
			// The call for this function should be low, so lock should be OK.
			c.convertToReducer()
			c.mutex.Unlock()
		}
	}
}

// convertToReducer convert the coordinator to a Reducer, and fill the reducer
// job channel with reduce jobs.
func (c *Coordinator) convertToReducer() {
	reduceJobToBeDone := make(map[string][]string)
	c.finishedMapJobs.Range(func(k, v interface{}) bool {
		for _, f := range v.([]string) {
			reducerName := strings.Split(f, "-")[2]
			reduceJobToBeDone[reducerName] = append(reduceJobToBeDone[reducerName], f)
		}
		return true
	})
	for i := int32(0); i < c.nReduce; i++ {
		reduceJob := JobReply{
			Type:      "Reduce",
			Filename:  fmt.Sprintf("mr-out-%d", i),
			TempFiles: reduceJobToBeDone[fmt.Sprintf("%d", i)],
		}
		c.reduceJobs <- &reduceJob
	}
}

func (c *Coordinator) acknowledgeReduce(args *JobArgs) {
	// the args.Filename should be mr-out-0, mr-out-1, or mr-out-2...
	if _, ok := c.finishedReduceJobs.Load(args.Filename); !ok {
		c.finishedReduceJobs.Store(args.Filename, args.TempFiles)
		os.Rename(args.TempFiles[0], args.Filename)
		if c.finishedReduceJobsCounts.Add(1) == c.nReduce {
			close(c.doneJobs) // mark all map reduce jobs has been done, we don't need worker anymore.
			c.done.Add(1)
		}
	}
}

func (c *Coordinator) dispatchTask(reply *JobReply) {
	select {
	case reduceJob := <-c.reduceJobs:
		// Put the job back to the queue so other workers can still race on this job.
		// Pass the instance instead of the pointer to avoid racing with line 116.
		go func(reduceJob JobReply) {
			time.Sleep(10 * time.Second)
			if _, ok := c.finishedReduceJobs.Load(reduceJob.Filename); !ok {
				c.reduceJobs <- &reduceJob
			}
		}(*reduceJob)
		reduceJob.WorkerID = c.reducerCnt.Add(1)
		*reply = *reduceJob
	case mapJob := <-c.mapJobs:
		// Put the job back to the queue so other workers can still race on this job.
		go func(mapJob JobReply) {
			time.Sleep(10 * time.Second)
			if _, ok := c.finishedMapJobs.Load(mapJob.Filename); !ok {
				c.mapJobs <- &mapJob
			}
		}(*mapJob)
		mapJob.WorkerID = c.mapperCnt.Add(1)
		*reply = *mapJob
	case _, ok := <-c.doneJobs:
		if !ok {
			*reply = JobReply{
				Type: "Done",
			}
		} else {
			log.Fatal("Complete should never be filled with element.")
		}
		return
	}
}
