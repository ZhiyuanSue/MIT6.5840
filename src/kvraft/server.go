package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	// "fmt"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type optype int
const (
	op_none optype = iota
	op_get
	op_put
	op_append
)
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string
	OpType optype
	
	Client_id int64
	Request_id int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	client_max_request_id	map[int64]int64
	client_chan	map[int64]chan Op
	kvdata	map[string]string
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// fmt.Printf("get a get requset\n")
	if kv.killed(){
		reply.Err = ErrWrongLeader
		return
	}else{
		_,isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return 
		}
	}
	op := Op{
		Key:args.Key,
		OpType:op_get,

	}
	_,_,isLeader:=kv.rf.Start(op)
	if !isLeader{	// we still need to check whether this is still leader
		reply.Err = ErrWrongLeader
		return 
	}
	// wait for a while
	ms := 100
	time.Sleep(time.Duration(ms) * time.Millisecond)

	kv.mu.Lock()
	client_ch,exist := kv.client_chan[op.Client_id]
	if !exist{
		kv.client_chan[op.Client_id]=make(chan Op,1)
	}
	client_ch = kv.client_chan[op.Client_id]
	kv.mu.Unlock()
	select{
	case op := <- client_ch :
		if op.Client_id == args.Client_id && op.Request_id == args.Request_id{
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.kvdata[args.Key]
			kv.mu.Unlock()
			return
		}
	}
	reply.Err = ErrWrongLeader
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// fmt.Printf("get a putAppend request\n")
	if kv.killed(){
		reply.Err = ErrWrongLeader
		return
	}else{
		_,isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return 
		}
	}
	op_type := op_none
	if args.Op == "Put"{
		op_type = op_put
	}else if (args.Op == "Append"){
		op_type = op_append
	}else{
		reply.Err = ErrWrongRequest
		return
	}
	op := Op{
		Key:args.Key,
		Value:args.Value,
		OpType:op_type,

	}
	_,_,isLeader:=kv.rf.Start(op)
	if !isLeader{	// we still need to check whether this is still leader
		reply.Err = ErrWrongLeader
		return 
	}

	kv.mu.Lock()
	client_ch,exist := kv.client_chan[op.Client_id]
	if !exist{
		kv.client_chan[op.Client_id]=make(chan Op,1)
	}
	client_ch = kv.client_chan[op.Client_id]
	kv.mu.Unlock()
	select {
	case op := <- client_ch:
		if op.Client_id == args.Client_id && op.Request_id == args.Request_id{
			reply.Err = OK
			return
		}
	}
	reply.Err = ErrWrongLeader
	reply.Err = OK
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.client_chan = make(map[int64]chan Op)
	kv.kvdata = make(map[string]string)
	kv.client_max_request_id = make(map[int64]int64)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.recv_msg_from_raft()
	// fmt.Printf("%v finish start\n",kv.me)
	return kv
}
func (kv *KVServer)recv_msg_from_raft(){
	for !kv.killed(){
		for m := range kv.applyCh {
			op := m.Command.(Op)
			// judge the request id
			kv.mu.Lock()
			request_id,exist := kv.client_max_request_id[op.Client_id]
			if !exist || op.Request_id > request_id {
				// no such a request id means this client have not sent a request
				// or current request is the newest

				// update the request id
				kv.client_max_request_id[op.Client_id] = op.Request_id
				// only put and append need to record in the map
				if op.OpType == op_put{
					kv.kvdata[op.Key] = op.Value
				}else if op.OpType == op_append{
					kv.kvdata[op.Key] += op.Value
				}
			}
			client_ch,exist := kv.client_chan[op.Client_id]
			if !exist{
				kv.client_chan[op.Client_id]=make(chan Op,1)
			}
			client_ch = kv.client_chan[op.Client_id]
			kv.mu.Unlock()
			client_ch <- op
		}
	}
}