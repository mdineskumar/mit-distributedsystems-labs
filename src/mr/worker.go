package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
	"os"
	"io/ioutil"
	"encoding/json"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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
	workerId := os.Getpid()
	for {
		reply := askTask(workerId)
		switch reply.TaskType {
		case MapTask:
			doMap(mapf, reply)
			reportTask(MapTask, reply.TaskId, workerId)
		case ReduceTask:
			doReduce(reducef, reply)
			reportTask(ReduceTask, reply.TaskId, workerId)
		case WaitTask:
			time.Sleep(200*time.Millisecond)
		case ExitTask:
			return
		}
	}

}

func doMap(mapf func(string, string) []KeyValue, reply TaskReply){
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v",reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil{
		log.Fatalf("cannot read %v",reply.FileName)

	}
	file.Close()
	kva := mapf(reply.FileName, string(content))

	buckets := make([][]KeyValue, reply.NReduce)

	for _, kv := range kva {
		b := ihash(kv.Key) % reply.NReduce
		buckets[b] = append(buckets[b],kv)
	}

	//why we need to write each buckets in file
	//TODO
	writeMapOutput(reply.TaskId, reply.NReduce, buckets)
	
}

func writeMapOutput(mapTaskId, nReduce int, buckets [][]KeyValue) error {
	for r:= 0; r <nReduce;r++ {
		finalName := fmt.Sprintf("mr-%d-%d",mapTaskId,r)
		err := writeAtomic(".",finalName, func(f *os.File) error {
			enc := json.NewEncoder(f)
			for _, kv := range buckets[r] {
				if err := enc.Encode(&kv); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}


func doReduce(reducef func(string, []string) string, reply TaskReply) {
	var intermediate []KeyValue
	for m:=0; m<reply.NMap; m++ {
		f, err := os.Open(fmt.Sprintf("mr-%d-%d",m, reply.TaskId))
		if err != nil { continue}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if dec.Decode(&kv) != nil {break}
			intermediate = append(intermediate, kv)
		}
		f.Close()
	}
	sort.Sort(ByKey(intermediate))
	
	oname := fmt.Sprintf("mr-out-%d", reply.TaskId)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

}

func writeAtomic(dir, finalName string, write func(f *os.File) error) error {
	tmp, err := os.CreateTemp(dir, "mr-tmp-")
	if err != nil {return err}

	if err := write(tmp); err != nil {
		tmp.Close()
		return err
	}

	tmp.Close()
	return os.Rename(tmp.Name(), finalName)
}
 
func askTask(workerId int) TaskReply {
	args := TaskArgs{WorkerId: workerId}
	reply := TaskReply{}

	ok := call("Coordinator.AskTask", &args, &reply)

	//task have to be done in this right
	//in the reply we have filename we can go those
	

	if ok {
		fmt.Println("Worker successfully asked task.", args, reply)
	} else {
		reply.TaskType = ExitTask
	}

	return reply

}

func reportTask(taskType TaskType, taskId int, workerId int) {
	args := ReportTaskArgs{TaskType: taskType, TaskId: taskId, WorkerId: workerId}
	reply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if !ok {
		//reply.TaskType = ExitTask
	}
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
