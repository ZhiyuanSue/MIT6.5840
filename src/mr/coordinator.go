package mr

// import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "fmt"
import "sync"

type work struct{
	have_assigned bool
	have_finished bool
	assigned_worker int
	assign_start_time time.Time
}

type coor_state int
const (
	coor_state_map	coor_state = 0
	coor_state_reduce coor_state = 1
	coor_state_finish	coor_state = 2
)

type Coordinator struct {
	// Your definitions here.
	coor_lock sync.Mutex
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
	c.coor_lock.Lock()
	c.worker_num++
	c.coor_lock.Unlock()
	return nil
}
func (c *Coordinator) WorkCurWorker(ask *AskWorkCurWorker, reply *AskWorkCurWorkerReply) error{
	c.coor_lock.Lock()
	if c.work_pool[ask.WorkId].assigned_worker != ask.WorkerId{
		reply.Res = false
	}else{
		reply.Res = true
	}
	c.coor_lock.Unlock()
	return nil
}

func (c *Coordinator) AssignTask(args *AskTaskArgs, reply *AskTaskReply) error{
	// fmt.Printf("get a ask requset type %v finworkid %v havefinishwork %v\n",args.AskType,args.FinWorkId,args.HaveFinishWork)
	// if args.HaveFinishWork == true{
	// 	fmt.Printf("get a finish work info %v from worker %v\n",args.FinWorkId,args.WorkerId)
	// }
	/*update work state*/
	c.coor_lock.Lock()
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
			}else if c.work_pool[i].have_finished == false{
				/*have assigned but not finished, check time*/
				curr_time :=time.Now()
				timeout,_	:=time.ParseDuration("10s")
				deadline := c.work_pool[i].assign_start_time.Add(timeout)
				if curr_time.After(deadline){
					not_assigned_task=i
					// fmt.Printf("find a work %v outof deadline state is map ask task is %v\n",i,args.WorkerId)
					break
				}
			}
		}
	}else if c.CoorState == coor_state_reduce{
		for i :=len(c.files); i<(len(c.files) + c.nReduce);i++{
			if c.work_pool[i].have_assigned==false{
				not_assigned_task=i
				break
			}else if c.work_pool[i].have_finished == false{
				/*have assigned but not finished, check time*/
				curr_time :=time.Now()
				timeout,_	:=time.ParseDuration("10s")
				deadline := c.work_pool[i].assign_start_time.Add(timeout)
				if curr_time.After(deadline){
					not_assigned_task=i
					// fmt.Printf("find a work %v outof deadline state is reduce ask task is %v\n",i,args.WorkerId)
					break
				}
			}
		}
	}
	reply.WorkId=not_assigned_task
	if not_assigned_task!=-1{
		c.work_pool[not_assigned_task].have_assigned=true
		c.work_pool[not_assigned_task].assigned_worker=args.WorkerId
		c.work_pool[not_assigned_task].assign_start_time=time.Now()
		// fmt.Printf("assign the work %v to worker %v\n",not_assigned_task,args.WorkerId)
	}
	c.coor_lock.Unlock()
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
		tmp_work.assign_start_time=time.Now()
		c.work_pool = append(c.work_pool,tmp_work)
	}
	for i :=len(files); i<(len(files) + nReduce);i++{
		var tmp_work work
		tmp_work.have_assigned=false
		tmp_work.have_finished=false
		tmp_work.assigned_worker=-1
		tmp_work.assign_start_time=time.Now()
		c.work_pool = append(c.work_pool,tmp_work)
	}
	fmt.Printf("finish init server,file len is %v\n",len(files))
	c.server()
	return &c
}
