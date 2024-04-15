package mr

import "log"
import "net"
import "net/http"
import "net/rpc"
import "os"
import "io"
import "time"


type taskPhase int

type task struct {
    id          int         // task unique ID
    filename    string      // param for mapTask (update when task is added)
    content     string      // param for mapTask (update when task is added)
    key         string      // param for reduceTask (TODO: update when map is comleted)
    values      []KeyValue  // param for reduceTask (TODO: update when map is comleted)
}

type taskState struct {
    task
    phase       taskPhase   // waiting for map(0) or waiting for reduce(1) or completed(2)
    workerID    int         // the ID of the worker who get the task
}

type worker struct {
    workerID    int
    alive       bool
    lastPing    int64
    taskID      int
}

type Coordinator struct {
    workers         map[int]worker      // workerID -> worker/taskID
    taskStates      map[int]taskState   // taskID -> task/workerID

    intermediate    []KeyValue      // results of map() (TODO: update in CompleteTask())
    wordsCount      map[string]int  // results of reduce() (TODO: update in CompleteTask())

    mapComplete     int     // number of map complete (TODO: update in CompleteTask())
    reduceComplete  int     // number of reduce complete (TODO: update in CompleteTask())
    done            bool    // true if all tasks completed (TODO: update in CompleteTask())
}

var nReduce int

const (
    waitingForMap       taskPhase = 0
    mapping             taskPhase = 1
    waitingForReduce    taskPhase = 2
    reducing            taskPhase = 3
    completed           taskPhase = 4

    scheduleInterval time.Duration = time.Millisecond * 500

    timeoutSeconds int64 = 10
)


// Your code here -- RPC handlers for the worker to call.


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
    reply.Y = args.X + 1
    return nil
}


// (change the identifer(PID) if workers are on differnt machine
func (c *Coordinator) Ping(args *PingArgs, reply *PingReply) {
    value, exist := c.workers[args.PID]
    if exist { // update worker info
        value.alive = true
        value.lastPing = time.Now().Unix()
        c.workers[args.PID] = value
        return
    } else { // register worker
        c.workers[args.PID] = worker{
            workerID: args.PID,
            alive: true,
            lastPing: time.Now().Unix(),
        }
    }
}


func (c *Coordinator) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) {
    if c.Done() {
        return
    }

    var wker worker
    if v, ok := c.workers[args.PID]; ok {
        wker = v
    } else { // the worker has not ping once(not register)
        return
    }

    println(args.PID, "try to fetch task.")

    // try to get map task
    if c.mapComplete < len(c.taskStates) {
        for i := 0; i < len(c.taskStates); i ++ {
            if c.taskStates[i].phase != waitingForMap {
                continue
            }

            if v, ok := c.taskStates[i]; ok {
                v.phase     = mapping
                v.workerID  = args.PID

                wker.taskID = v.id
                c.workers[args.PID] = wker
                reply.task = v.task

                c.taskStates[i] = v
            }
            return
        }

    // try to get reduce task
    } else if c.reduceComplete < len(c.taskStates) {
        for i := 0; i < len(c.taskStates); i ++ {
            if c.taskStates[i].phase != waitingForReduce {
                continue
            }

            if v, ok := c.taskStates[i]; ok {
                v.phase     = reducing
                v.workerID  = args.PID

                wker.taskID = v.id
                c.workers[args.PID] = wker
                reply.task = v.task

                c.taskStates[i] = v
            }
            return
        }

    // all tasks done
    } else {
        c.done = true
    }
}


func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) {
    // TODO: check task phase:
    //  map: update params of reduce task(task.key, task.values)
    //       update c.intermediate, c.mapComplete
    //  reduce: update c.wordsCount && c.reduceComplete, check c.done
}


//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

    // if c.done, write wordsCount into file

    return c.done
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


// TODO
func (c *Coordinator) schedule() {
    // polling to check if worker timeout

    for {
        for wokerID, worker := range c.workers {
            if time.Now().Unix() - worker.lastPing > timeoutSeconds {
                taskID := c.workers[wokerID].taskID

                value := c.taskStates[taskID]
                if value.phase == mapping {
                    value.phase = waitingForMap
                } else if value.phase == reducing {
                    value.phase = waitingForReduce
                }
                c.taskStates[taskID] = value

                delete(c.workers, wokerID)
            }
        }

        time.Sleep(scheduleInterval)
    }
}


//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, _nReduce int) *Coordinator {
    c := Coordinator{}

    nReduce = _nReduce

    // init Coordinator
    for i := 0; i < nReduce; i ++ {
        // NOTE: rebuild read file part if run in real machine
        file, err := os.Open(files[i])
        if err != nil {
            panic(err)
        }
        content, err := io.ReadAll(file)
        if err != nil {
            panic(err)
        }

        c.taskStates[i] = taskState{
            task : task{
                id: i,
                filename: files[i],
                content: string(content),
            },
            phase: waitingForMap,
        }
    }


    c.server()
    c.schedule()
    return &c
}
