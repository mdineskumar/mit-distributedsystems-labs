package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask // when all the map tasks in progress without finishing all we cant go to reduce tasks
	ExitTask // all the tasks are finished
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	taskType  TaskType
	status    TaskStatus
	fileName  string
	workerId  int
	startTime time.Time
	taskId    int
}

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	files       []string
	nReduce     int
	nMap        int
	phase       TaskType
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	fmt.Printf("worker: %d done task: %d \n",args.WorkerId, args.TaskId)

	c.mu.Lock()
	defer c.mu.Unlock()

	//find task and marked it as completed
	//does c.phase need to cjeck?
	if args.TaskType == MapTask {
		for i := range c.mapTasks {
			if (c.mapTasks[i].taskId == args.TaskId) && (c.mapTasks[i].workerId == args.WorkerId) {
				//what if task reassigned to other worker. this slow worker come after that
					c.mapTasks[i].status = Completed
					return nil
			}
		}
	} else if args.TaskType == ReduceTask {
		for i := range c.reduceTasks {
			if (c.reduceTasks[i].taskId == args.TaskId) && (c.reduceTasks[i].workerId == args.WorkerId) {
				c.reduceTasks[i].status = Completed
				return nil
			}
		}
	}

	return nil

}


func (c *Coordinator) AskTask(args *TaskArgs, reply *TaskReply) error {
	fmt.Printf("worker: %d asking task\n", args.WorkerId)
	c.mu.Lock()
	defer c.mu.Unlock() // i missed this part

	if c.phase == MapTask && allDone(c.mapTasks) {
		c.phase = ReduceTask
	}

	if c.phase == ReduceTask && allDone(c.reduceTasks) {
		c.phase = ExitTask
	}

	if c.phase == MapTask {
		resetStaleTasks(c.mapTasks)

		for i := range c.mapTasks {
			if c.mapTasks[i].status == Idle {
				fmt.Printf("worker assign for task %d: %s\n", args.WorkerId, c.mapTasks[i].fileName)
				c.mapTasks[i].status = InProgress
				c.mapTasks[i].startTime = time.Now()
				c.mapTasks[i].workerId = args.WorkerId
			

				reply.FileName = c.mapTasks[i].fileName
				reply.TaskId = i
				reply.TaskType = MapTask
				reply.NReduce = c.nReduce

				return nil
			}

		}
		//all the map tasks in progress
		reply.TaskType = WaitTask

	} else if c.phase == ReduceTask {
		resetStaleTasks(c.reduceTasks)
		for i := range c.reduceTasks {
			if c.reduceTasks[i].status == Idle {
				fmt.Printf("worker assign reduce task id : %d to workerid: %d\n", c.reduceTasks[i].taskId, args.WorkerId)
				c.reduceTasks[i].status = InProgress
				c.reduceTasks[i].startTime = time.Now()
				c.reduceTasks[i].workerId = args.WorkerId

				reply.TaskType = ReduceTask
				reply.TaskId = i
				reply.NMap = c.nMap
				return nil
			}
		}
		//why we exit if all reduce task inprogress
		reply.TaskType = ExitTask
	} else {
		reply.TaskType = ExitTask
	}

	return nil
}

func resetStaleTasks(tasks []Task) {
	for i := range tasks {
		if tasks[i].status == InProgress && time.Since(tasks[i].startTime) > 10 * time.Second {
			tasks[i].status = Idle
		}
	}
}

func allDone(tasks []Task) bool {
	// i stuck here why i cant do this
	// if you use for i := range then it will give index not task careful
	for _, t := range tasks{
		if t.status != Completed{
			return false
		}
	}
	return true
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	c.mu.Lock()
	defer c.mu.Unlock()
	// reuse same source of truth
	return c.phase == ExitTask
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase:       MapTask,
		files:       files,
		nReduce:     nReduce,
		nMap:        len(files), //why do we need nMap
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}

	//initialize maptasks
	for i := range c.mapTasks {
		c.mapTasks[i] = Task{taskType: MapTask, fileName: c.files[i], status: Idle, taskId: i}
	}
	//initilaize reducetasks
	for i := range c.reduceTasks {
		c.reduceTasks[i] = Task{taskType: ReduceTask, status: Idle, taskId: i}
	}

	c.server()
	return &c
}
