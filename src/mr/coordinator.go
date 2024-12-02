package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
)

type Coordinator struct {
	// Your definitions here.
	jobQueue  chan *Job
	waitQueue chan *Job
	// 未完成的job数量
	jobCount  int
	finishSet Set
	nReduce   int
	// 提示已进入reduce阶段
	state chan struct{}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	_ = rpc.Register(c)
	rpc.HandleHTTP()
	var l net.Listener
	var e error
	if runtime.GOOS == "windows" {
		l, e = net.Listen("tcp", ":1234")
	} else {
		sockname := coordinatorSock()
		_ = os.Remove(sockname)
		l, e = net.Listen("unix", sockname)
	}
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire Job has finished.
func (c *Coordinator) Done() bool {
	return c.jobCount <= 0
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
		jobQueue:  make(chan *Job, nReduce+len(preciseFileName)),
		waitQueue: make(chan *Job, nReduce+len(preciseFileName)),
		state:     make(chan struct{}),
		finishSet: make(Set),
		jobCount:  nReduce + len(preciseFileName),
		nReduce:   nReduce,
	}
	for i := range preciseFileName {
		c.jobQueue <- &Job{
			FileName:  preciseFileName[i],
			JobType:   false,
			SendTimes: 0,
		}
	}
	go func() {
		<-c.state
		for i := 0; i < nReduce; i++ {
			c.jobQueue <- &Job{
				FileName:  "1-" + strconv.Itoa(i) + ".txt",
				JobType:   true,
				SendTimes: 0,
			}
		}
	}()
	c.server()
	return &c
}

var index int64 = 0

func (c *Coordinator) GetJob(a *Args, args *Args) error {
	args.NReduce = c.nReduce
	if a.WorkNum == -1 {
		args.WorkNum = int(atomic.AddInt64(&index, 1))
	}
	if c.jobCount == 0 {
		args.IsFinish = true
		return nil
	}
	// 优先返回jobQueue任务
	if len(c.jobQueue) != 0 {
		job := <-c.jobQueue
		args.Job = job
		job.SendTimes++
		return nil
	}
	select {
	case job := <-c.jobQueue:
		args.Job = job
		job.SendTimes++
		return nil
	case job := <-c.waitQueue:
		for c.finishSet.contains(job.FileName) {
			job = <-c.waitQueue
		}
		job.SendTimes++
		c.waitQueue <- job
		args.Job = job
		return nil
	}
}

var (
	finishedSet = make(Set)
	mutex       = &sync.Mutex{}
)

func (c *Coordinator) Finish(args *Args, ret *Args) error {
	c.finishSet.add(args.Job.FileName)

	jobType := "reduce"
	if !args.Job.JobType {
		jobType = "map"
	}

	fmt.Printf("job type:%s, fileName: %s, sendTimes:%d\n", jobType, args.Job.FileName, args.Job.SendTimes)

	key := fmt.Sprintf("%s-%s", jobType, args.Job.FileName)
	mutex.Lock()
	defer mutex.Unlock()

	if !finishedSet.contains(key) {
		finishedSet.add(key)
		c.jobCount--

		if args.Job.JobType {
			ret.ReduceAck = true
		}

		if c.jobCount == c.nReduce {
			c.state <- struct{}{}
			close(c.state)
		}
	}

	return nil
}
