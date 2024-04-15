package mr

import "fmt"
import "hash/fnv"
import "log"
import "net/rpc"
import "os"
import "time"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
    Key   string
    Value string
}


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
    h := fnv.New32a()
    h.Write([]byte(key))
    return int(h.Sum32() & 0x7fffffff)
}


// TODO
func workerSchudule(
    mapf func(string, string) []KeyValue,
    reducef func(string, []string) string,
) {
    for {
        // TODO: get task
        //       do task






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
        args := PingArgs{PID: pid}
        reply := PingReply{}
        ok := call("Coordinator.Ping", &args, &reply)
        if !ok || !reply.Flag {
            close(exitCh)
        }
    }()

    // do mapf or reducef
    go workerSchudule(mapf, reducef)

    // exitCh will blocking unless coordinator(master) is not alive
    <- exitCh


}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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