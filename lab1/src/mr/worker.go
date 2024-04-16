package mr

import "fmt"
import "hash/fnv"
import "log"
import "net/rpc"
import "os"
import "time"
import "sort"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
    Key   string
    Value string
}

// for sorting by key.
type SortKey []KeyValue

// for sorting by key.
func (a SortKey) Len() int           { return len(a) }
func (a SortKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
    h := fnv.New32a()
    h.Write([]byte(key))
    return int(h.Sum32() & 0x7fffffff)
}


func workerSchudule(
    mapf func(string, string) []KeyValue,
    reducef func(string, []string) string,
) {
    for {
        println("try to fetch task.")

        args := FetchTaskArgs{PID: os.Getpid()}
        reply := FetchTaskReply{}

        ok := call("Coordinator.FetchTask", &args, &reply)
        if !ok {
            continue
        }

        task := &reply.Task
        completeArgs := CompleteTaskArgs{
            PID: os.Getpid(),
            Task: *task,
        }
        completeReply := CompleteTaskReply{}


        if task.TaskType == "map" {
            completeArgs.Phase = mapping
            completeArgs.MapResult = mapf(task.Filename, task.Content)

        } else if task.TaskType == "reduce" {
            completeArgs.Phase = reducing

            sort.Sort(SortKey(task.Values))
            println("length:", len(task.Values))
            for i := 0; i < len(task.Values); i ++{
                var values []string

                j := i
                for j < len(task.Values) {
                    // println("j:", j)
                    if task.Values[j].Key != task.Values[i].Key {
                        break
                    }
                    values = append(values, task.Values[j].Value)
                    j ++
                }

                // println("append", i, "-", j)

                res := KeyValue{
                    Key: task.Values[i].Key,
                    Value: reducef(task.Values[i].Key, values),
                }
                completeArgs.RecudeResult = append(completeArgs.RecudeResult, res)
                i = j - 1
            }
        }

        println("try to complete task.")

        call("Coordinator.CompleteTask", &completeArgs, &completeReply)

        time.Sleep(scheduleInterval)
    }
}


//
// main/mrworker.go calls this function.
//
func Worker(
    mapf func(string, string) []KeyValue,
    reducef func(string, []string) string,
) {
    // Your worker implementation here.

    // check if coordinator is alive
    pid := os.Getpid()
    exitCh := make(chan struct{})
    go func() {
        for {
            args := PingArgs{PID: pid}
            reply := PingReply{}
            ok := call("Coordinator.Ping", &args, &reply)
            if !ok {
                close(exitCh)
            }

            time.Sleep(scheduleInterval)
        }
    }()

    go workerSchudule(mapf, reducef)

    // exitCh will blocking unless coordinator(master) is not alive
    <- exitCh
}


//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
