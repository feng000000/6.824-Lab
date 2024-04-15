package mr

import "log"
import "net"
import "net/http"
import "net/rpc"
import "os"
import "io"
import "time"
import "strconv"
import "sort"


type taskPhase int

type task struct {
    id          int         // task unique ID
    filename    string      // param for mapTask
    content     string      // param for mapTask
    key         string      // param for reduceTask (TODO: update when recude task is added)
    values      []KeyValue  // param for reduceTask (TODO: update when recude task is added)
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
    // taskID      int
    task        task
}

type Coordinator struct {
    nReduce         int
    workers         map[int]worker      // workerID -> worker/taskID
    taskStates      map[int]taskState   // taskID -> task/workerID

    intermediate    []KeyValue          // results of map()
    wordsCount      map[string]int      // results of reduce()

    mapComplete     int                 // number of map complete
    reduceComplete  int                 // number of reduce complete
    done            bool                // true if all tasks completed
}

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
        reply.Flag = false
        return
    }

    var wker worker
    if v, ok := c.workers[args.PID]; ok {
        wker = v
    } else { // the worker has not ping once(not register)
        reply.Flag = false
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

                wker.task = v.task
                reply.Task = v.task
                reply.Flag = true

                c.workers[args.PID] = wker
                c.taskStates[i] = v
            } else {
                reply.Flag = false
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

                wker.task = v.task
                reply.Task = v.task
                reply.Flag = true

                c.workers[args.PID] = wker
                c.taskStates[i] = v
            } else {
                reply.Flag = false
            }
            return
        }

    // all tasks done
    } else {
        c.done = true
    }
}


func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) {
    //  map: update c.intermediate, c.mapComplete
    if args.Phase == mapping {
        if v, ok := c.taskStates[args.Task.id]; !ok || v.phase != mapping {
            reply.Flag = false
            return
        } else {
            v.phase = waitingForReduce
            c.taskStates[args.Task.id] = v
        }

        c.mapComplete ++
        // TODO: write intermediate into file
        c.intermediate = append(c.intermediate, args.MapResult)
        if v, ok := c.workers[args.PID]; ok {
            v.task = task{}
            c.workers[args.PID] = v
        }

        // add nReduce reduce task(task.key, task.values)
        if c.mapComplete == len(c.taskStates) {
            newTaskStates := make(map[int]taskState)
            sort.Sort(SortKey(c.intermediate))
            for i := 0; i < len(c.intermediate); i ++ {
                taskID := ihash(c.intermediate[i].Key)
                v := taskState{}
                v.phase = waitingForReduce
                v.task.id = taskID
                v.task.key = c.intermediate[i].Key
                j := i + 1
                for j < len(c.intermediate) {
                    // TODO
                    if c.intermediate[j].Key == c.intermediate[i].Key {
                        v.task.values = append(v.task.values, c.intermediate[j])
                        j ++
                    } else {
                        break
                    }
                }
                i = j - 1

                newTaskStates[taskID] = v
            }
            c.taskStates = newTaskStates
        }

    //  reduce: update c.wordsCount && c.reduceComplete, check c.done
    } else if args.Phase == reducing {
        if v, ok := c.taskStates[args.Task.id]; !ok || v.phase != reducing {
            reply.Flag = false
            return
        } else {
            v.phase = completed
            c.taskStates[args.Task.id] = v
        }

        c.reduceComplete ++
        if v, ok := c.wordsCount[args.RecudeResult.Key]; ok {
            add_v, err := strconv.Atoi(args.RecudeResult.Value)
            if err != nil {
                reply.Flag = false
                return
            }

            v += add_v
            c.wordsCount[args.RecudeResult.Key] = v
        }
        if c.reduceComplete == c.nReduce {
            c.done = true
        }
    }

    reply.Flag = true
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
                taskID := c.workers[wokerID].task.id

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

    c.nReduce = _nReduce

    // init Coordinator
    for i := 0; i < len(files); i ++ {
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
