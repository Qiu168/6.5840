package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type Args struct {
	Job      Job
	IsFinish bool
	WorkNum  int
	NReduce  int
}

type Job struct {
	FileName string
	//true  : reduce
	//false : map
	JobType bool
	//发送给worker执行的次数
	SendTimes int
}

type Set map[string]struct{}

func (s Set) contains(str string) bool {
	_, ok := s[str]
	return ok
}

func (s Set) add(str string) {
	s[str] = struct{}{}
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
