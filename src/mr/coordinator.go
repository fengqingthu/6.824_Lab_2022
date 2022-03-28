package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const DebugPrints = false

//
// a task class
//
type Task struct {
	// "Map", "Reduce" and "Please exit"
	TaskType string
	// the filenames associated with this task
	TaskFile []string
	// the index of current task, M map tasks and N reduce tasks
	TaskX int
	// nReduce argument for workers
	TaskR int
}

// a thread-safe map for tasks
type TaskState struct {
	// locks
	mu sync.RWMutex
	// state are represented as int: 0 for idle, 1 for in-progress, 2 for finished
	state map[*Task]int
	// track which worker each task is assigned to
	workerID map[*Task]string
	// a counter to keep track of number of unfinished works
	unfinished int
}

// read methods
func (t *TaskState) getState(key *Task) (int, string) {
	t.mu.RLock()
	val := t.state[key]
	id := t.workerID[key]
	t.mu.RUnlock()
	return val, id
}

func (t *TaskState) getUnfinished() int {
	t.mu.RLock()
	val := t.unfinished
	t.mu.RUnlock()
	return val
}

// write methods
func (t *TaskState) setState(key *Task, state int, id string) {
	t.mu.Lock()
	t.state[key] = state
	t.workerID[key] = id
	t.mu.Unlock()
}

func (t *TaskState) updateUnfinished(increment int) {
	t.mu.Lock()
	t.unfinished = t.unfinished + increment
	t.mu.Unlock()
}

//
// a thread-safe map for workers
//
type WorkerState struct {
	// locks
	mu sync.RWMutex
	// map workerID to is_dead bool
	dead map[string]bool
	// map workerID to the task object that it is current working on
	task map[string]*Task
}

// read method
func (w *WorkerState) getState(id string) (bool, *Task) {
	w.mu.RLock()
	val := w.dead[id]
	task := w.task[id]
	w.mu.RUnlock()
	return val, task
}

// write method
func (w *WorkerState) setState(id string, val bool, task *Task) {
	w.mu.Lock()
	w.dead[id] = val
	w.task[id] = task
	w.mu.Unlock()
}

//
// thread-safe overall phase variable
//
type PhaseState struct {
	mu    sync.RWMutex
	phase int
}

// read method
func (p *PhaseState) getState() int {
	p.mu.RLock()
	val := p.phase
	p.mu.RUnlock()
	return val
}

// method to increment to next phase
func (p *PhaseState) incrementState() {
	p.mu.Lock()
	p.phase++
	p.mu.Unlock()
}

type Coordinator struct {
	// 0 as map phase, 1 as reduce phase, 2 as completed
	phase *PhaseState
	// a channel to create and assign tasks
	taskQueue chan *Task
	// a channel to check in-processing tasks
	checkQueue chan *Task
	// a table to track states of all tasks
	taskTable *TaskState
	// a table to track states of workers
	workerTable *WorkerState
}

//
// RPC handler: pop available tasks from taskQueue, assign it to worker requests and update taskTable
//
func (c *Coordinator) AssignTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	// if all work finished, let worker exit
	workerID := args.WorkerID
	if c.phase.getState() == 2 {
		reply.AssignedTask.TaskType = "Please exit"
		if DebugPrints {
			fmt.Printf("Shut down worker %s\n", workerID)
		}
		return nil
	}
	for {
		select { // request from taskQueue
		case task := <-c.taskQueue:
			reply.AssignedTask.TaskType = task.TaskType
			reply.AssignedTask.TaskX = task.TaskX
			reply.AssignedTask.TaskFile = task.TaskFile
			reply.AssignedTask.TaskR = task.TaskR
			c.taskTable.setState(task, 1, workerID)
			c.workerTable.setState(workerID, false, task)
			c.checkQueue <- task
			if DebugPrints {
				fmt.Printf("Assigned %s %d task to worker %s\n", task.TaskType, task.TaskX, workerID)
			}
			return nil
		default:
			time.Sleep(50 * time.Millisecond)
			return nil
		}
	}
}

//
// RPC handler: receive finished tasks from worker and update taskTable and workerTable
//
func (c *Coordinator) ReceiveTask(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	// if worker marked as dead, ignore and let it exit
	dead, _ := c.workerTable.getState(args.WorkerID)
	if dead {
		reply.Message = "Please exit"
		return nil
	}
	// update stateTable and workerTable
	id := args.WorkerID
	_, task := c.workerTable.getState(id) // here using workerTable to get the task object on the server side
	// only update the file names when receiving map tasks
	if task.TaskType == "Map" {
		task.TaskFile = args.FinishedTask.TaskFile
	}
	c.taskTable.setState(task, 2, "")
	c.workerTable.setState(id, false, nil)
	if DebugPrints {
		fmt.Printf("Received %s %d task from worker %s\n", task.TaskType, task.TaskX, id)
	}
	// decrement overall unfinished task counter in checker thread for synchronization purpose
	return nil
}

//
// a method periodically checking if in-progressing workers are alive
// if no response, mark worker as dead and re-push task back to taskQueue
func (c *Coordinator) checker() {
	for {
		select {
		case task := <-c.checkQueue:
			go func(t *Task) { // lambda check func
				sec := 0
				for sec < 10 {
					state, id := c.taskTable.getState(t)
					if state == 2 {
						// work done
						c.taskTable.updateUnfinished(-1)
						return
					}
					dead, _ := c.workerTable.getState(id)
					if dead {
						// already dead
						return
					}
					// if task still not finished, mark worker dead, update taskTable, push task back to taskQueue
					if sec == 9 {
						if DebugPrints {
							fmt.Printf("Worker %s died\n", id)
						}
						c.workerTable.setState(id, true, nil)
						c.taskTable.setState(t, 0, "")
						c.taskQueue <- t
						return
					}
					time.Sleep(time.Second)
					sec++
				}
			}(task)
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}

//
// start a thread that listens for RPCs from worker.go
//
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

// initialize map tasks
// read from file, split chunks and create tasks, push to taskQueue and update taskTable
//
func (c *Coordinator) initMapTasks(files []string, nReduce int) {
	i := 0
	for _, file := range files {
		// push tasks into taskQueue
		c.taskQueue <- &Task{"Map", []string{file}, i, nReduce}
		// increment unfinished work
		c.taskTable.updateUnfinished(1)
		i++
	}
	if DebugPrints {
		fmt.Printf("Initialized %d Map tasks\n", c.taskTable.getUnfinished())
	}
	c.taskTable.updateUnfinished(-1)
}

//
// initialize reduce tasks
// according to returned map results, collect file names for recude tasks and push to queue
func (c *Coordinator) initReduceTasks(nReduce int) {
	// iterate through all map tasks (all finished), update their type and push back to taskQueue
	for i := 0; i < nReduce; i++ {
		task := &Task{"Reduce", []string{}, i, nReduce}
		c.taskTable.mu.RLock()
		// no SUHA guarantee, so need to check map result filenames one by one
		for oldTask := range c.taskTable.state {
			for _, filename := range oldTask.TaskFile {
				if strings.HasSuffix(filename, "-"+strconv.Itoa(i)) {
					task.TaskFile = append(task.TaskFile, filename)
				}
			}
		}
		c.taskTable.mu.RUnlock()
		c.taskQueue <- task
		c.taskTable.updateUnfinished(1)
	}
	if DebugPrints {
		fmt.Printf("Initialized %d Reduce tasks\n", c.taskTable.getUnfinished())
	}
	c.taskTable.updateUnfinished(-1)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// simply check the phase variable
	return c.phase.getState() == 2
}

// create a taskState instance
func makeTaskState() *TaskState {
	taskState := TaskState{}
	taskState.state = make(map[*Task]int)
	taskState.workerID = make(map[*Task]string)
	taskState.unfinished = 0
	return &taskState
}

// create a workerState instance
func makeWorkerState() *WorkerState {
	workerState := WorkerState{}
	workerState.dead = make(map[string]bool)
	workerState.task = make(map[string]*Task)
	return &workerState
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	const BUFFER = 8
	// instantiate coordinator
	c := Coordinator{
		&PhaseState{},
		make(chan *Task, BUFFER),
		make(chan *Task, BUFFER),
		makeTaskState(),
		makeWorkerState()}

	// release listener threads
	c.server()
	go c.checker()

	// map phase
	go c.initMapTasks(files, nReduce)
	for c.taskTable.getUnfinished() >= 0 {
		time.Sleep(50 * time.Millisecond)
	}
	if DebugPrints {
		fmt.Println("========== Map phase completed ==========")
	}
	// reduce phase
	c.phase.incrementState()
	c.taskTable.updateUnfinished(1)
	go c.initReduceTasks(nReduce)
	for c.taskTable.getUnfinished() >= 0 {
		time.Sleep(50 * time.Millisecond)
	}
	if DebugPrints {
		fmt.Println("========== Reduce phase completed ==========")
	}
	c.phase.incrementState()
	return &c
}
