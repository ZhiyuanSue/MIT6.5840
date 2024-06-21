package shardctrler


import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

import "sync/atomic"
import "time"

import "bytes"
import "fmt"


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	client_max_request_id	map[int64]int64
	client_chan	map[int64]chan Op
	dead    int32 // set by Kill()
	maxraftstate int // snapshot if log grows this big

	configs []Config // indexed by config num
	persister *raft.Persister
}

type optype int
const (
	op_none optype = iota
	op_join
	op_leave
	op_move
	op_query
)
type Op struct {
	// Your data here.
	OpType optype
	
	Client_id int64
	Request_id int64
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	// fmt.Printf("get a putAppend request\n")
	if sc.killed(){
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}else{
		_,isLeader := sc.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			return 
		}
	}
	op := Op{
		OpType:op_join,
		Client_id:args.Client_id,
		Request_id:args.Request_id,
	}
	index,_,isLeader:=sc.rf.Start(op)
	if !isLeader{	// we still need to check whether this is still leader
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return 
	}

	sc.mu.Lock()
	
	timer := time.NewTimer(100*time.Millisecond)

	client_ch,exist := sc.client_chan[int64(index)]
	if !exist{
		sc.client_chan[int64(index)]=make(chan Op,1)
	}
	client_ch = sc.client_chan[int64(index)]
	sc.mu.Unlock()
	select {
	case op :=<- client_ch:
		if op.Client_id == args.Client_id && op.Request_id == args.Request_id{
			reply.Err = OK
			reply.WrongLeader = false
			return
		}else{
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
	reply.Err = ErrWrongLeader
	reply.WrongLeader = true
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	// fmt.Printf("get a putAppend request\n")
	if sc.killed(){
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}else{
		_,isLeader := sc.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			return 
		}
	}
	op := Op{
		OpType:op_leave,
		Client_id:args.Client_id,
		Request_id:args.Request_id,
	}
	index,_,isLeader:=sc.rf.Start(op)
	if !isLeader{	// we still need to check whether this is still leader
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return 
	}

	sc.mu.Lock()
	
	timer := time.NewTimer(100*time.Millisecond)

	client_ch,exist := sc.client_chan[int64(index)]
	if !exist{
		sc.client_chan[int64(index)]=make(chan Op,1)
	}
	client_ch = sc.client_chan[int64(index)]
	sc.mu.Unlock()
	select {
	case op :=<- client_ch:
		if op.Client_id == args.Client_id && op.Request_id == args.Request_id{
			reply.Err = OK
			reply.WrongLeader = false
			return
		}else{
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
	reply.Err = ErrWrongLeader
	reply.WrongLeader = true
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	// fmt.Printf("get a putAppend request\n")
	if sc.killed(){
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}else{
		_,isLeader := sc.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			return 
		}
	}
	op := Op{
		OpType:op_move,
		Client_id:args.Client_id,
		Request_id:args.Request_id,
	}
	index,_,isLeader:=sc.rf.Start(op)
	if !isLeader{	// we still need to check whether this is still leader
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return 
	}

	sc.mu.Lock()
	
	timer := time.NewTimer(100*time.Millisecond)

	client_ch,exist := sc.client_chan[int64(index)]
	if !exist{
		sc.client_chan[int64(index)]=make(chan Op,1)
	}
	client_ch = sc.client_chan[int64(index)]
	sc.mu.Unlock()
	select {
	case op :=<- client_ch:
		if op.Client_id == args.Client_id && op.Request_id == args.Request_id{
			reply.Err = OK
			reply.WrongLeader = false
			return
		}else{
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
	reply.Err = ErrWrongLeader
	reply.WrongLeader = true
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// fmt.Printf("get a putAppend request\n")
	if sc.killed(){
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}else{
		_,isLeader := sc.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
			return 
		}
	}
	op := Op{
		OpType:op_query,
		Client_id:args.Client_id,
		Request_id:args.Request_id,
	}
	index,_,isLeader:=sc.rf.Start(op)
	if !isLeader{	// we still need to check whether this is still leader
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return 
	}

	sc.mu.Lock()
	
	timer := time.NewTimer(100*time.Millisecond)

	client_ch,exist := sc.client_chan[int64(index)]
	if !exist{
		sc.client_chan[int64(index)]=make(chan Op,1)
	}
	client_ch = sc.client_chan[int64(index)]
	sc.mu.Unlock()
	select {
	case op :=<- client_ch:
		if op.Client_id == args.Client_id && op.Request_id == args.Request_id{
			reply.Err = OK
			reply.WrongLeader = false
			return
		}else{
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		}
	case <-timer.C:
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
	}
	reply.Err = ErrWrongLeader
	reply.WrongLeader = true
}


// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}
func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	
	// Your code here.
	sc.client_chan = make(map[int64]chan Op)
	sc.client_max_request_id = make(map[int64]int64)
	sc.persister = persister
	sc.InstallSnapshot(sc.rf.LastIncludeIndex,sc.persister.ReadSnapshot())

	go sc.recv_msg_from_raft()

	return sc
}
func (sc *ShardCtrler)recv_msg_from_raft(){
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()
	for !sc.killed(){
		// fmt.Printf("recv msg for loop\n")
		select{
		case m	:= <- sc.applyCh :
			if m.CommandValid{	// after use the snapshot ,the apply msg might be a snapshot
				op := m.Command.(Op)
				// judge the request id
				sc.mu.Lock()
				request_id,exist := sc.client_max_request_id[op.Client_id]
				sc.mu.Unlock()
				if !exist || op.Request_id > request_id {
					// no such a request id means this client have not sent a request
					// or current request is the newest
					sc.mu.Lock()
					// update the request id
					sc.client_max_request_id[op.Client_id] = op.Request_id
					// only put and append need to record in the map
					if op.OpType == op_join{

					}else if op.OpType == op_leave{

					}else if op.OpType == op_move{

					}else if op.OpType == op_query{

					}
					sc.mu.Unlock()
				}
				sc.mu.Lock()
				client_ch,exist := sc.client_chan[int64(m.CommandIndex)]
				if !exist{
					sc.client_chan[int64(m.CommandIndex)]=make(chan Op,1)
				}
				client_ch = sc.client_chan[int64(m.CommandIndex)]
				// generate the snapshot
				if sc.maxraftstate != -1 && sc.persister.RaftStateSize() >= sc.maxraftstate*4/5 {
					// only the leader need to do this
					// fmt.Printf("need to use snapshot %v\n",sc.persister.RaftStateSize())
					// fmt.Printf("leader gen snapshot index %v\n",m.CommandIndex)
					s := sc.Generate_Snapshot()
					go sc.rf.Snapshot(m.CommandIndex,s)
				}
				sc.mu.Unlock()
				client_ch <- op
			}else if m.SnapshotValid{
				//a follower's snapshot have been applied
				sc.InstallSnapshot(m.SnapshotIndex,m.Snapshot)
			}
			
		case <-timer.C:
			_,isleader := sc.rf.GetState()
			if !isleader{
				continue
			}
		}
	}
}
func (sc *ShardCtrler) Generate_Snapshot() []byte{
	// just like persister
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(sc.client_max_request_id)
	s := w.Bytes()
	return s
}
func (sc *ShardCtrler) InstallSnapshot(index int, data []byte){
	// the same as the persister,just copy and change is ok
	if data == nil || len(data) < 1{
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var client_max_request_id	map[int64]int64
	if	d.Decode(&client_max_request_id) != nil{
			fmt.Printf("install snapshot decode fail\n")
	}else{
		sc.client_max_request_id =client_max_request_id
	}
}
