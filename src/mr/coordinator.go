package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
)

type Coordinator struct {
	// Your definitions here.
	jobQueue  chan Job
	waitQueue chan Job
	// 未完成的job数量
	jobCount  int
	finishSet Set
	nReduce   int
	//
	state bool
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
// if the entire Job has finished.
func (c *Coordinator) Done() bool {
	return c.jobCount == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	filesize := len(files)
	preciseFileName := make([]string, 0)
	for i := 0; i < filesize; i++ {
		fileNames, err := filepath.Glob(files[i])
		if err != nil {
			log.Fatal(err)
		}
		preciseFileName = append(preciseFileName, fileNames...)
	}
	c := Coordinator{
		jobQueue:  make(chan Job, nReduce+len(preciseFileName)),
		waitQueue: make(chan Job, nReduce+len(preciseFileName)),
		jobCount:  nReduce + len(preciseFileName),
		nReduce:   nReduce,
	}

	c.server()
	return &c
}
func (c *Coordinator) GetJob(args Args) Args {
	if c.jobCount == 0 {
		args.IsFinish = true
		return args
	}
	if len(c.jobQueue) != 0 {
		job := <-c.jobQueue
		args.Job = job
		return args
	}
	job := <-c.waitQueue
	for c.finishSet.contains(job.FileName) {
		job = <-c.waitQueue
	}
	c.waitQueue <- job
	args.Job = job
	return args
}
func (c *Coordinator) Finish(args Args) {
	c.finishSet.add(args.Job.FileName)
	c.jobCount--
}
