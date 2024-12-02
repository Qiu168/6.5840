package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"log"
	"net/rpc"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	var args = &Args{WorkNum: -1}

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
				fmt.Printf("map write kv error : %v", err)
				return
			}
			//告诉master map执行完了
			finish(args)
		} else {
			//如果是Reduce，获取中间文件
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
				str := split[i]
				if strings.TrimSpace(str) == "" {
					continue
				}
				kv := strings.Split(str, ":")
				if m[kv[0]] == nil {
					m[kv[0]] = make([]string, 0)
				}
				if len(kv) != 2 {
					fmt.Println("kv no :", kv, " kv.size:", len(kv))
					continue
				}
				m[kv[0]] = append(m[kv[0]], kv[1])
			}
			//执行reduce
			var s string
			//oname := "mr-out-1"
			var outStr string
			for kv := range m {
				s = reducef(kv, m[kv])
				outStr += fmt.Sprintf("%s\t%s\n", kv, s)
			}
			//err = writeToFile(oname, outStr)
			//if err != nil {
			//	log.Fatal(err)
			//	return
			//}
			args.Job.Result = outStr
			finish(args)
		}
	}
}

func sortFile(oname string) {
	file, err := os.OpenFile(oname, os.O_RDWR, fs.ModePerm)
	if err != nil {
		return
	}
	defer file.Close()
	// 先读取文件内容
	content, err := io.ReadAll(file)
	if err != nil {
		return
	}
	// 将文件内容按行分割
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	// 对行进行排序
	sort.Strings(lines)
	// 将排序后的行写回文件
	_ = file.Truncate(0)
	_, _ = file.Seek(0, 0)
	for _, line := range lines {
		_, _ = file.WriteString(line + "\n")
	}
}

func finish(args *Args) {
	call("Coordinator.Finish", args, &struct{}{})
}

func getJob(args *Args) *Args {
	call("Coordinator.GetJob", args, args)
	return args
}

func writeKV(kv []KeyValue, workNum int, nReduce int) error {

	var strMap = make(map[string]string)
	for i := range kv {
		itoa := strconv.Itoa(ihash(kv[i].Key) % nReduce)
		//防止并发问题？
		var fileName = strconv.Itoa(workNum) + "-" + itoa + ".txt"
		strMap[fileName] += kv[i].Key + ":" + kv[i].Value + "\n"
	}
	group := sync.WaitGroup{}
	group.Add(len(strMap))
	var err error
	for s := range strMap {
		s := s
		go func() {
			err = writeToFile(s, strMap[s])
			group.Done()
		}()
	}
	group.Wait()
	return err
}
func writeToFile(filename, data string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_RDWR|os.O_CREATE, fs.ModePerm)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	_, err = writer.WriteString(data)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return err
	}
	err = writer.Flush()
	if err != nil {
		fmt.Println("Error flushing to file:", err)
		return err
	}
	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	var c *rpc.Client
	var err error
	if runtime.GOOS == "windows" {
		c, err = rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	} else {
		sockname := coordinatorSock()
		c, err = rpc.DialHTTP("unix", sockname)
	}
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
