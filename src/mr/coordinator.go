package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type work struct{
	have_assigned bool
	have_finished bool
	assigned_worker int
	assign_start_time int
}

type coor_state int
const (
	coor_state_map	coor_state = 0
	coor_state_reduce coor_state = 1
	coor_state_finish	coor_state = 2
)

type Coordinator struct {
	// Your definitions here.
	CoorState coor_state
	files []string
	nReduce int
	worker_num	int
	work_pool	[]work
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetEnv(ask *AskEnv,reply *AskEnvReply) error{
	reply.Files=c.files
	reply.NReduce=c.nReduce
	reply.WorkerId=c.worker_num
	c.worker_num++
	return nil
}
func (c *Coordinator) AssignTask(args *AskTaskArgs, reply *AskTaskReply) error{
	fmt.Printf("get a ask requset type %v finworkid %v havefinishwork %v\n",args.AskType,args.FinWorkId,args.HaveFinishWork)
	/*update work state*/
	if args.HaveFinishWork == true{
		c.work_pool[args.FinWorkId].have_finished=true
	}
	/*update the coordinator state*/
	if c.CoorState == coor_state_map{
		finish_state := true
		for i :=0; i<len(c.files);i++{
			if c.work_pool[i].have_finished==false{
				finish_state= false
				break
			}			
		}
		if finish_state == true{
			c.CoorState = coor_state_reduce
		}
	}else if c.CoorState == coor_state_reduce{
		finish_state := true
		for i :=len(c.files); i<(len(c.files) + c.nReduce);i++{
			if c.work_pool[i].have_finished==false{
				finish_state= false
				break
			}
		}
		if finish_state == true{
			c.CoorState = coor_state_finish
		}
	}
	// fmt.Printf("current state is %v\n",c.CoorState)
	/*don't use else if*/
	if c.CoorState == coor_state_finish{
		reply.WorkId = -2
		return nil
	}
	/*
		check whether all the task have assigned
		if so send a wait
		not a finish
	*/
	not_assigned_task := -1	/*the same as the work_type_wait judgement*/
	if c.CoorState == coor_state_map{
		for i :=0; i<len(c.files);i++{
			if c.work_pool[i].have_assigned==false{
				not_assigned_task=i
				break
			}
		}
	}else if c.CoorState == coor_state_reduce{
		for i :=len(c.files); i<(len(c.files) + c.nReduce);i++{
			if c.work_pool[i].have_assigned==false{
				not_assigned_task=i
				break
			}
		}
	}
	reply.WorkId=not_assigned_task
	if not_assigned_task!=-1{
		c.work_pool[not_assigned_task].have_assigned=true
		c.work_pool[not_assigned_task].assigned_worker=args.WorkerId
	}
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.CoorState == coor_state_finish{
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	/*
		按照我的理解，我们总共需要有len(files)+nReduce个任务需要分配给worker
		然后需要完成了前len(files)个任务之后，才能完成后面的nReduce（中间会有State的转换）
		所以我认为，直接生成一个work数组，也没必要区分map work和reduce work增加复杂度
		除此之外，是否assign给另一个worker，需要记录每个任务开始分配的时间，然后每次收到task询问新的工作
		就扫描一遍，看看是否存在超时任务，然后每个任务都记录了自己被赋予了哪一个worker
		但是需要考虑的事情是，假如某个任务试图写入mr-X-Y，需要查询当前这个work是不是已经赋予给了别人，即这个任务的worker id是不是自己
		如果是，那么写入，否则就不写入。
	*/
	c.CoorState =  coor_state_map
	c.files = files
	c.nReduce = nReduce
	c.worker_num=0
	for i :=0; i< len(files);i++ {
		var tmp_work work
		tmp_work.have_assigned=false
		tmp_work.have_finished=false
		tmp_work.assigned_worker=-1
		tmp_work.assign_start_time=0
		c.work_pool = append(c.work_pool,tmp_work)
	}
	for i :=len(files); i<(len(files) + nReduce);i++{
		var tmp_work work
		tmp_work.have_assigned=false
		tmp_work.have_finished=false
		tmp_work.assigned_worker=-1
		tmp_work.assign_start_time=0
		c.work_pool = append(c.work_pool,tmp_work)
	}
	fmt.Printf("finish init server\n")
	c.server()
	return &c
}
