package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	var args = &Args{WorkNum: -1}
	// 规定map的num为奇数，reduce的num为偶数
	for !args.IsFinish {
		args = getJob(args)

		fileName := args.Job.FileName
		if !args.Job.JobType {
			file, err := os.ReadFile(fileName)
			if err != nil {
				continue
			}
			//执行mapf方法
			keyValues := mapf(fileName, string(file))
			//写文件
			err = writeKV(keyValues, args.WorkNum, args.NReduce)
			if err != nil {
				return
			}
			//告诉master map执行完了
			finish(args)
		} else {
			//如果是Reduce，获取map文件
			file, err := os.Open(fileName)
			if err != nil {
				return
			}
			all, err := io.ReadAll(file)
			if err != nil {
				return
			}
			split := strings.Split(string(all), "\n")
			m := make(map[string][]string)
			for i := range split {
				str := strings.TrimSpace(split[i])
				kv := strings.Split(str, ":")
				if m[kv[0]] == nil {
					m[kv[0]] = make([]string, 0)
				}
				m[kv[0]] = append(m[kv[0]], kv[1])
			}
			//执行reduce
			var s string
			for kv := range m {
				s = reducef(kv, m[kv])
				oname := "mr-out-1"
				ofile, _ := os.Create(oname)
				fmt.Fprintf(ofile, "%v %v\n", kv, s)
			}
			finish(args)
		}

	}
}

func finish(args *Args) {
	call("coordinator.Finish", args, nil)
}

func getJob(args *Args) *Args {
	call("coordinator.GetJob", args, &args)
	return args
}

type void struct{}

var v void

func writeKV(kv []KeyValue, workNum int, nReduce int) error {
	set := make(map[string]void)

	for i := range kv {
		itoa := strconv.Itoa(ihash(kv[i].Key) % nReduce)
		//防止并发问题？
		var fileName = "/" + strconv.Itoa(workNum) + "-" + itoa + ".txt"
		set[itoa] = v
		err := os.WriteFile(fileName, []byte(kv[i].Key+":"+kv[i].Value), fs.ModeAppend)
		if err != nil {
			return err
		}
	}
	return nil
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
