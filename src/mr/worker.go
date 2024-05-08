package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "sort"

//
// Map functions return a slice of KeyValue.
//
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


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
func Map(mapf func(string,string) []KeyValue,
	filename string,
	WorkId int,
	nReduce int,	
){
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := [][]KeyValue{}
	kva := mapf(filename, string(content))
	for index :=0;index < nReduce;index++{
		intermediate=append(intermediate,[]KeyValue{})
	}
	for i :=0;i<len(kva);i++{
		key := kva[i].Key
		index := ihash(key) % nReduce
		intermediate[index]=append(intermediate[index],kva[i])
	}
	/*TODO:before we try to write in the file,we need to check whether the worker for this work is me*/
	for index :=0;index < nReduce;index++{
		oname :="mr-"+strconv.Itoa(WorkId)+"-"+strconv.Itoa(index)
		// fmt.Printf("%v\n",oname)
		ofile, _ := os.Create(oname)
		/*TODO: change to a json file format*/
		enc := json.NewEncoder(ofile)
  		for i :=0; i < len(intermediate[index]); i++ {
    		err :=enc.Encode(intermediate[index][i])
			if err != nil {
				log.Fatal(err)
			}
		}
		ofile.Close()
	}
}
func Reduce(reducef func(string,[]string) string,
	files []string,
	WorkId int,
	nReduce int){
	intermediate := []KeyValue{}
	sorted_intermediate := []KeyValue{}
	oname := "mr-out-"+strconv.Itoa(WorkId-len(files))
	ofile, _ := os.Create(oname)
	// fmt.Printf("output %v\n",oname)
	for index :=0;index < len(files);index++{
		iname := "mr-"+strconv.Itoa(index)+"-"+strconv.Itoa(WorkId-len(files))
		// fmt.Printf("%v\n",iname)
		ifile, _ := os.Open(iname)
		dec := json.NewDecoder(ifile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
			  break
			}
			intermediate=append(intermediate,kv)
		}
		ifile.Close()
	}
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		sorted_intermediate=append(sorted_intermediate,KeyValue{intermediate[i].Key,strconv.Itoa(j-i)})
		i = j
	}
	for i :=0;i<len(sorted_intermediate);i++{
		fmt.Fprintf(ofile, "%v %v\n", sorted_intermediate[i].Key, sorted_intermediate[i].Value)
	}
	ofile.Close()
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
		// fmt.Printf("nReduce %v \nfiles %v\n",envreply.NReduce,envreply.Files)
		// fmt.Printf("my worker id is %v\n",envreply.WorkerId)
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
			if reply.WorkId == -1{
				// fmt.Printf("get a wait task\n")
				asktask.AskType = ask_type_ask
				asktask.WorkerId = envreply.WorkerId
				asktask.FinWorkId = -1
				asktask.HaveFinishWork = false
			} else if reply.WorkId == -2{
				// fmt.Printf("get a finish task\n")
				return
			} else if reply.WorkId>=0 && reply.WorkId < len(envreply.Files){
				/*map task*/
				fmt.Printf("get a map task and id is %v\n",reply.WorkId)
				Map(mapf,envreply.Files[reply.WorkId],reply.WorkId,envreply.NReduce)
				asktask.AskType=ask_type_fin
				asktask.WorkerId = envreply.WorkerId
				asktask.HaveFinishWork = true
				asktask.FinWorkId = reply.WorkId
			} else if reply.WorkId>=len(envreply.Files) && reply.WorkId<(len(envreply.Files)+envreply.NReduce){
				/*reduce task*/
				fmt.Printf("get a reduce task and id is %v\n",reply.WorkId)
				Reduce(reducef,envreply.Files,reply.WorkId,envreply.NReduce)
				asktask.AskType=ask_type_fin
				asktask.WorkerId = envreply.WorkerId
				asktask.HaveFinishWork = true
				asktask.FinWorkId = reply.WorkId
			}else{
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
