package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Phase int

const (
	Map Phase = iota
	Reduce
)

type TaskStatus int

const (
	Created TaskStatus = iota
	Started
	Finished
)

type Task struct {
	phase Phase
	index int
	status TaskStatus
	timer *time.Timer
	sync.Mutex
}

const TaskTimeout = 10

type Master struct {
	// Your definitions here.
	input           []string
	nMap            int
	nReduce         int
	phase           Phase
	mapTasks        []*Task
	reduceTasks     []*Task
	toScheduleTasks []*Task
	nFinished       int
	done            bool
	lock			sync.Mutex
	cond			*sync.Cond
}

type RequestTaskError string

func (error RequestTaskError) Error() string {
	return string(error)
}

func createTask(phase Phase, index int) *Task {
	task := Task{phase: phase, index: index, status: Created}
	return &task
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) initialize(files []string, nReduce int) {
	m.input = files
	m.nMap = len(files)
	m.nReduce = nReduce
	m.phase = Map

	m.mapTasks = make([]*Task, m.nMap)
	for i := 0; i < m.nMap; i++ {
		m.mapTasks[i] = createTask(Map, i)
	}

	m.reduceTasks = make([]*Task, nReduce)
	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = createTask(Reduce, i)
	}

	m.cond = sync.NewCond(&m.lock)

	m.nFinished = 0
	m.done = false
}

func (m *Master) startMap() {
	log.Println("Start Map")
	m.phase = Map
	m.toScheduleTasks = m.mapTasks
}

func (m *Master) startReduce() {
	log.Println("Start Reduce")
	m.phase = Reduce
	m.toScheduleTasks = m.reduceTasks
}

func (m *Master) scheduleTask() *Task {
	m.lock.Lock()
	for len(m.toScheduleTasks) == 0 && !m.done {
		m.cond.Wait()
	}
	defer m.lock.Unlock()
	if m.done {
		return nil
	}

	task := m.toScheduleTasks[0]
	m.toScheduleTasks = m.toScheduleTasks[1:]

	task.status = Started
	task.timer = time.AfterFunc(TaskTimeout * time.Second, func() {
		task.Lock()
		if task.status != Finished {
			m.retryTask(task)
		}
		task.Unlock()
	})

	return task
}

func (m *Master) retryTask(task *Task) {
	log.Printf("Retry task %v - %v", task.phase, task.index)
	task.status = Created
	m.lock.Lock()
	m.toScheduleTasks = append(m.toScheduleTasks, task)
	m.cond.Signal()
	m.lock.Unlock()
}

func (m *Master) finishTask(phase Phase, index int) {
	var task *Task
	if phase == Map {
		task = m.mapTasks[index]
	} else {
		task = m.reduceTasks[index]
	}
	task.Lock()
	if task.status != Finished {
		task.status = Finished
		task.timer.Stop()
		m.lock.Lock()
		m.nFinished += 1

		if m.phase == Map && m.nFinished == m.nMap {
			m.startReduce()
			m.cond.Broadcast()
		} else if m.phase == Reduce && m.nFinished == m.nMap + m.nReduce {
			m.finishMR()
			m.cond.Broadcast()
		}
		m.lock.Unlock()
	}
	task.Unlock()
}

func (m *Master) finishMR() {
	log.Println("MR program finished")
	m.done = true
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	reply.MapCount = m.nMap
	reply.ReduceCount = m.nReduce
	return nil
}

func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	if args.PrevTask != nil {
		log.Printf("Worker %v finished task %v - %v",
			args.WorkerId, args.PrevTask.Phase, args.PrevTask.Index)
		m.finishTask(args.PrevTask.Phase, args.PrevTask.Index)
	}

	task := m.scheduleTask()
	if task == nil {
		reply.Task = nil
	} else {
		t := TaskRPC{
			Phase: task.phase,
			Index: task.index,
		}
		if task.phase == Map {
			t.Input = m.input[task.index]
		}
		reply.Task = &t
	}

	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.done
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.initialize(files, nReduce)
	m.startMap()

	m.server()
	return &m
}
