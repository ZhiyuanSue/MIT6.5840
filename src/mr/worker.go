package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	askenv := AskEnv{}
	envreply := AskEnvReply{}
	ok := call("Coordinator.GetEnv",&askenv,&envreply)
	if ok{
		fmt.Printf("nReduce %v \nfiles %v\n",envreply.NReduce,envreply.Files)
		fmt.Printf("my worker id is %v\n",envreply.WorkerId)
	} else {
		fmt.Println("connect to server error,have finished all work\n")
		return
	}
	// Your worker implementation here.
	asktask := AskTaskArgs{}
	reply := AskTaskReply{}
	asktask.AskType = ask_type_ask
	asktask.WorkerId = envreply.WorkerId
	asktask.FinWorkId = -1
	asktask.HaveFinishWork = false
	for{
		ok := call("Coordinator.AssignTask",&asktask,&reply)
		if ok {
			var WorkType work_type
			if reply.WorkId == -1{
				WorkType = work_type_wait
			} else if reply.WorkId == -2{
				WorkType = work_type_finish
			} else if reply.WorkId>=0 && reply.WorkId < len(envreply.Files){
				WorkType = work_type_map
			} else if reply.WorkId>=len(envreply.Files) && reply.WorkId<(len(envreply.Files)+envreply.NReduce){
				WorkType = work_type_reduce
			}
			fmt.Printf("worktype is %v work id is %v",WorkType,reply.WorkId)

			switch WorkType {
			case work_type_map:
				fmt.Printf("get a map task and id is %v\n",reply.WorkId)
				asktask.AskType=ask_type_fin
				asktask.WorkerId = envreply.WorkerId
				asktask.HaveFinishWork = true
				asktask.FinWorkId = reply.WorkId
			case work_type_reduce:
				fmt.Printf("get a reduce task and id is %v\n",reply.WorkId)
				asktask.AskType=ask_type_fin
				asktask.WorkerId = envreply.WorkerId
				asktask.HaveFinishWork = true
				asktask.FinWorkId = reply.WorkId
			case work_type_wait:
				fmt.Printf("get a wait task\n")
				asktask.AskType = ask_type_ask
				asktask.WorkerId = envreply.WorkerId
				asktask.FinWorkId = -1
				asktask.HaveFinishWork = false
			case work_type_finish:
				fmt.Printf("get a finish task\n")
				return
			default:
				fmt.Printf("some error have happened from server,exit\n")
				return
			}
		} else {
			fmt.Println("connect to server error,have finished all work\n")
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
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
