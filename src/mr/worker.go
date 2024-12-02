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

// Worker main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := &Args{WorkNum: -1}

	for !args.IsFinish {
		args = getJob(args)

		if args.Job.JobType {
			handleReduceJob(args, reducef)
		} else {
			handleMapJob(args, mapf)
		}

		finish(args)
	}
}

func handleMapJob(args *Args, mapf func(string, string) []KeyValue) {
	file, err := os.ReadFile(args.Job.FileName)
	if err != nil {
		log.Printf("Error reading file %s: %v", args.Job.FileName, err)
		return
	}

	keyValues := mapf(args.Job.FileName, string(file))
	if err := writeKV(keyValues, args.WorkNum, args.NReduce); err != nil {
		log.Printf("Error in map write kv: %v", err)
	}
}

func handleReduceJob(args *Args, reducef func(string, []string) string) {
	content, err := os.ReadFile(args.Job.FileName)
	if err != nil {
		log.Printf("Error reading file %s: %v", args.Job.FileName, err)
		return
	}

	kvMap := parseContent(string(content))
	var outStr string
	for key, values := range kvMap {
		result := reducef(key, values)
		outStr += fmt.Sprintf("%v\t%v\n", key, result)
	}
	args.Job.Result = outStr
}

func parseContent(content string) map[string][]string {
	lines := strings.Split(strings.TrimSpace(content), "\n")
	kvMap := make(map[string][]string)
	for _, line := range lines {
		kv := strings.SplitN(line, ":", 2)
		if len(kv) != 2 {
			log.Printf("Invalid line format: %s", line)
			continue
		}
		key, value := kv[0], kv[1]
		kvMap[key] = append(kvMap[key], value)
	}
	return kvMap
}

func sortFile(oname string) error {
	file, err := os.OpenFile(oname, os.O_RDWR, fs.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// 读取文件内容
	content, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// 将文件内容按行分割并排序
	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	sort.Strings(lines)

	// 清空文件并写入排序后的内容
	if err := file.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate file: %w", err)
	}
	if _, err := file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek file: %w", err)
	}

	writer := bufio.NewWriter(file)
	for _, line := range lines {
		if _, err := writer.WriteString(line + "\n"); err != nil {
			return fmt.Errorf("failed to write line: %w", err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %w", err)
	}

	return nil
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
