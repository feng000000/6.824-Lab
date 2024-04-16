package mr

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	// "sort"
	"strconv"
	"time"
)


type taskPhase int

type task struct {
    ID          int         // task unique ID
    TaskType    string      // "map" task or "reduce" task
    Filename    string      // param for mapTask
    Content     string      // param for mapTask
    Values      []KeyValue  // param for reduceTask
}

type taskState struct {
    task
    phase       taskPhase
    // workerID    int         // unnecessary
}

type worker struct {
    workerID    int
    alive       bool
    lastPing    int64
    // taskID      int
    task        task
}

type Coordinator struct {
    nMap            int
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


// (change the identifer(PID) if workers are on differnt machine
func (c *Coordinator) Ping(args *PingArgs, reply *PingReply) error {
    if v, ok := c.workers[args.PID]; ok { // update worker info
        v.alive = true
        v.lastPing = time.Now().Unix()
        c.workers[args.PID] = v
    } else { // register worker
        c.workers[args.PID] = worker{
            workerID: args.PID,
            alive: true,
            lastPing: time.Now().Unix(),
        }
    }
    return nil
}


func (c *Coordinator) FetchTask(
    args *FetchTaskArgs,
    reply *FetchTaskReply,
) error {
    if c.Done() {
        return errors.New("done")
    }

    var wker worker
    if v, ok := c.workers[args.PID]; ok {
        wker = v
    } else { // the worker did not ping once(not register)
        return errors.New("worker did not register")
    }

    println(args.PID, "fetch task.")

    for key, v := range c.taskStates {
        if v.phase != waitingForMap && v.phase != waitingForReduce {
            continue
        }

        // only assign reduce task when all map tasks are done
        if v.phase == waitingForReduce && c.mapComplete < c.nMap {
            continue
        }

        // update c.taskStates[key].task
        v.phase += 1
        c.taskStates[key] = v

        // update c.workers[args.PID].task
        wker.task = v.task
        c.workers[args.PID] = wker

        reply.Task = v.task

        println("fetch map task success, taskID:", v.task.ID, "\n\n")
        return nil
    }

    return errors.New("none task")
}


func (c *Coordinator) CompleteTask(
    args *CompleteTaskArgs,
    reply *CompleteTaskReply,
) error {
    // map task
    if args.Phase == mapping {
        println(args.PID, "try to complete map task", args.Task.ID)
        if v, ok := c.taskStates[args.Task.ID]; !ok || v.phase != mapping {
            return errors.New("task finished")
        } else {
            v.phase = waitingForReduce
            c.taskStates[args.Task.ID] = v
        }

        c.mapComplete ++
        // TODO: write intermediate into file
        c.intermediate = append(c.intermediate, args.MapResult...)
        if v, ok := c.workers[args.PID]; ok {
            v.task = task{}
            c.workers[args.PID] = v
        }

        // add nReduce reduce task(task.key, task.values)
        if c.mapComplete == len(c.taskStates) {
            newTaskStates := make(map[int]taskState)
            // sort.Sort(SortKey(c.intermediate))
            for i := 0; i < len(c.intermediate); i ++ {
                // taskID := ihash(c.intermediate[i].Key)
                taskID := ihash(c.intermediate[i].Key) % c.nReduce

                ts := taskState{}
                if v, ok := newTaskStates[taskID]; ok {
                    ts = v
                }

                ts.phase = waitingForReduce
                ts.task.ID = taskID
                ts.task.TaskType = "reduce"
                ts.task.Values = append(ts.task.Values, c.intermediate[i])

                // ts.phase = waitingForReduce
                // ts.task.ID = taskID
                // ts.task.TaskType = "reduce"
                // ts.task.Key = c.intermediate[i].Key
                // println("reduce task key", ts.task.Key)
                // j := i
                // for j < len(c.intermediate) {
                //     if c.intermediate[j].Key != c.intermediate[i].Key {
                //         break
                //     }
                //     ts.task.Values = append(ts.task.Values, c.intermediate[i])
                //     j ++
                // }
                // i = j - 1

                newTaskStates[taskID] = ts
            }
            c.taskStates = newTaskStates

            // println("             add reduce job:", len(c.taskStates))
            // println("c.taskStates ")
            // for key, v := range c.taskStates {
            //     println("key:", key, "phase:", v.phase, " taskType:", v.task.TaskType)
            // }
        }

    // reduce task
    } else if args.Phase == reducing {
        // println(args.PID, "try to complete reduce task", args.Task.ID)

        if v, ok := c.taskStates[args.Task.ID]; !ok || v.phase != reducing {
            return errors.New("coordinator error")
        } else {
            v.phase = completed
            c.taskStates[args.Task.ID] = v
        }

        for _, item := range args.RecudeResult {
            add_v, err := strconv.Atoi(item.Value)
            if v, ok := c.wordsCount[item.Key]; ok {
                if err != nil {
                    return err
                }

                v += add_v
                c.wordsCount[item.Key] = v
            } else {
                c.wordsCount[item.Key] = add_v
            }

        }

        c.reduceComplete ++
        if c.reduceComplete == c.nReduce {
            println("write file", len(c.wordsCount))
            ofile, err := os.Create("./mr-out-1")
            if err != nil {
                panic(err)
            }
            for k, v := range c.wordsCount {
                fmt.Fprintf(ofile, "%v %v\n", k, v)
            }

            c.done = true
        }
    }

    println("[COUNT]:", "mapComplete", c.mapComplete, "\n",
            "reduceTask length:", len(c.taskStates), "\n" ,
            "reduceComplete:", c.reduceComplete)

    println("complete", args.Task.ID, "success\n\n")
    return nil
}


//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

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


// polling to check if worker timeout
func (c *Coordinator) schedule() {
    println("start coordinator schedule")
    for {
        for wokerID, worker := range c.workers {
            if time.Now().Unix() - worker.lastPing < timeoutSeconds {
                continue
            }

            taskID := c.workers[wokerID].task.ID
            value := c.taskStates[taskID]
            if value.phase == mapping {
                value.phase = waitingForMap
            } else if value.phase == reducing {
                value.phase = waitingForReduce
            }
            c.taskStates[taskID] = value

            println("delete", wokerID)
            delete(c.workers, wokerID)
        }

        time.Sleep(scheduleInterval)
    }
}


//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
    c := Coordinator{
        nMap:           len(files),
        nReduce:        nReduce,
        workers:        make(map[int]worker),
        taskStates:     make(map[int]taskState),
        intermediate:   make([]KeyValue, 0),
        wordsCount:     make(map[string]int),
    }


    // add map task
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
                ID: i,
                TaskType: "map",
                Filename: files[i],
                Content: string(content),
            },
            phase: waitingForMap,
        }
    }

    go c.schedule()

    c.server()
    return &c
}
