package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
	"6.5840/labgob"
	"6.5840/labrpc"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term	int
	Command		interface{}
}

type RaftState int
const (
	RaftLeader RaftState = 0
	RaftFollower	RaftState = 1
	RaftCandidate	RaftState = 2
)
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	internal_mu	sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state     RaftState
	RecvHeartBeat	bool
	CollectVote	int
	// Persistent state on all servers
	currentTerm	int
	VotedFor	int
	Log	[]LogEntry
	// Volatile state on all servers
	CommitIndex	int
	LastApplied	int
	// Volatile state on leaders
	NextIndex	[]int
	MatchIndex	[]int

	ApplyCh	chan ApplyMsg

	// Seems need to add the snapshot part
	SnapShot []byte
	LastIncludeIndex int
	LastIncludeTerm int
	apply_msg_slice []ApplyMsg
}

func (rf *Raft) Lock(pos string){
	// fmt.Printf("<S%v> Lock at %v\n",rf.me,pos)
	rf.mu.Lock()
}
func (rf *Raft) Unlock(pos string){
	// fmt.Printf("<S%v> Unlock at %v\n",rf.me,pos)
	rf.mu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	isleader = false
	rf.Lock("GetState")
	if rf.state == RaftLeader{
		isleader = true
	}
	term = rf.currentTerm
	rf.Unlock("GetState")

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.VotedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.Log)
	e.Encode(rf.LastIncludeIndex)
	e.Encode(rf.LastIncludeTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.SnapShot)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// fmt.Printf("<%v> read a persist\n",rf.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var VotedFor int
	var currentTerm int
	var Log []LogEntry
	var LastIncludeIndex int
	var LastIncludeTerm int
	if	d.Decode(&VotedFor) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&Log) != nil ||
		d.Decode(&LastIncludeIndex)!= nil ||
		d.Decode(&LastIncludeTerm)!= nil {
		fmt.Printf("<%v> Persist decode fail\n",rf.me)
	} else {
		rf.VotedFor = VotedFor
		rf.currentTerm = currentTerm
		rf.Log = Log
		rf.LastIncludeIndex = LastIncludeIndex
		rf.LastIncludeTerm = LastIncludeTerm
	}
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludeIndex int
	LastIncludeTerm int
	Offset int	
		// as Hint says that:Send the entire snapshot in a single InstallSnapshot RPC. 
		// Don't implement Figure 13's offset mechanism for splitting up the snapshot.
		// So I think offset and done field is useless here
	Data []byte
	Done bool
}
type InstallSnapshotReply struct {
	Term int
}

// as it seems a index map like A->B,also called f(x),so B->A is f^-1(y)
// where the A is the original index ,and B is the trimmed index
func (rf *Raft) index_map_f(index_A int) int {
	index_B := index_A - rf.LastIncludeIndex
	return index_B
}
func (rf *Raft) index_map_f_1(index_B int) int{
	index_A := index_B + rf.LastIncludeIndex
	return index_A
}
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// fmt.Printf("<S%v> get a snapshot request %v\n",rf.me,index)
	// rf.PrintLogEntries()
	rf.Lock("Snapshot")
	if (index <= rf.CommitIndex && index > rf.LastIncludeIndex && rf.index_map_f(index) < len(rf.Log)){
		// fmt.Printf("the commitIndex is %v\n",rf.CommitIndex)
		rf.LastIncludeTerm = rf.Log[rf.index_map_f(index)].Term
		rf.Log = rf.Log[rf.index_map_f(index):]
		rf.LastIncludeIndex=index
		rf.SnapShot = snapshot
		if rf.LastApplied < index {
			rf.LastApplied = index
		}
	}
	rf.persist()
	// rf.PrintLogEntries()
	
	rf.Unlock("Snapshot")
}
func (rf *Raft) sendSnapShot(server int){
	rf.Lock("sendSnapShot")
	// rf.PrintLogEntries()
	ok := false
	reply := InstallSnapshotReply{}
	if rf.killed() == false && rf.state == RaftLeader{
		args := InstallSnapshotArgs{
			Term	:	rf.currentTerm,
			LeaderId	:	rf.me,
			LastIncludeIndex	:	rf.LastIncludeIndex,
			LastIncludeTerm	:	rf.LastIncludeTerm,
			Offset	:	0,
			Data	:	rf.SnapShot,
			Done	:	true,
		}
		
		rf.Unlock("sendSnapShot")
		ok = rf.peers[server].Call("Raft.ReplySnapShot", &args, &reply)
	}else{
		rf.Unlock("sendSnapShot")
	}
	rf.Lock("sendSnapShot")
	if ok && rf.killed() == false && rf.state == RaftLeader{
		if(reply.Term > rf.currentTerm){
			rf.currentTerm=reply.Term
			rf.state = RaftFollower
			rf.RecvHeartBeat = true	// as the new leader's heartbeat
			rf.VotedFor = -1
			rf.persist()
		}else{
			// install the snapshot successfully, update the nextindex
			// fmt.Printf("send the snapshot successfully and rf.LastIncludeIndex is %v\n",rf.LastIncludeIndex)
			rf.NextIndex[server] = rf.LastIncludeIndex
		}	
	}
	rf.Unlock("sendSnapShot")
}

func (rf *Raft) ReplySnapShot(args *InstallSnapshotArgs,reply *InstallSnapshotReply){
	// This function sometimes is like the AppendEntries, it must update something ,such as the recv heart beat flag
	rf.Lock("ReplySnapShot")
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		rf.Unlock("ReplySnapShot")
		return
	}else if args.Term == rf.currentTerm{

	}else if args.Term > rf.currentTerm{
		//change the state,but remember that the persist should be later after all the snapshot installed
		rf.currentTerm=args.Term
		rf.state = RaftFollower
	}
	rf.RecvHeartBeat = true
	// 2/ 3/ 4/ 5/ the offset is ignored
	// start from 6/

	//6/ if existing log entry has same index and term
	have_log_entry:=false
	if rf.index_map_f(args.LastIncludeIndex)>=0 && rf.index_map_f(args.LastIncludeIndex) <= len(rf.Log)-1 {
		if rf.Log[rf.index_map_f(args.LastIncludeIndex)].Term == args.LastIncludeTerm{
			have_log_entry=true
		}
	}
	if have_log_entry{
		rf.Log= rf.Log[rf.index_map_f(args.LastIncludeIndex):]
	}else{
		// clear the rf.Log
		rf.Log= make([]LogEntry,0)
	}
	//update
	rf.SnapShot = args.Data
	rf.LastIncludeIndex = args.LastIncludeIndex
	rf.LastIncludeTerm = args.LastIncludeTerm

	if rf.CommitIndex < args.LastIncludeIndex{
		rf.CommitIndex = args.LastIncludeIndex
	}
	if rf.LastApplied < args.LastIncludeIndex{
		rf.LastApplied = args.LastIncludeIndex
	}
	rf.persist()
	//reply
	reply.Term = rf.currentTerm
	// send the applymsg
	rf.Unlock("ReplySnapShot")
	rf.SendApplyMsg(true)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term	int
	CandidateId	int
	LastLogIndex	int
	LastLogTerm	int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term	int
	VoteGranted	bool
}
// heartbeat
type AppendEntriesArgs struct {
	Term	int
	LeaderId	int
	PrevLogIndex	int
	PrevLogTerm	int
	Entries 	[]LogEntry
	LeaderCommit int
}
type AppendEntriesReply	struct	{
	Term	int
	Success	bool
	XTerm 	int //term in the conflicting entry (if any)
    XIndex	int //index of first entry with that term (if any)
    XLen	int //log length
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// fmt.Printf("<%v:T%v> get a vote request from %v\n",rf.me,rf.currentTerm,args.CandidateId)
	rf.Lock("RequestVote")
	if args.Term<rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.VoteGranted=false
		rf.Unlock("RequestVote")
		return
	}else if args.Term >rf.currentTerm{
		// for the new term, VotedFor must be null in this term,vote it
		rf.currentTerm=args.Term
		rf.persist()
		rf.state=RaftFollower
		Last_Log_Index := rf.index_map_f_1(len(rf.Log))-1
		Last_Log_Term := rf.LastIncludeTerm
		if rf.index_map_f(Last_Log_Index) > -1{
			Last_Log_Term = rf.Log[len(rf.Log)-1].Term
		}
		// fmt.Printf("@@@@@@<%v> vote request from %v term %v > server %v term %v\n",rf.me,args.CandidateId,args.Term,rf.me,rf.currentTerm)
		// fmt.Printf("@@@@@@<%v> last log term is %v args last term index %v\n",rf.me,Last_Log_Term,args.LastLogTerm)
		// fmt.Printf("@@@@@@<%v> last log index is %v args last log index %v\n",rf.me,Last_Log_Index,args.LastLogIndex)
		if args.LastLogTerm > Last_Log_Term || (Last_Log_Index<=args.LastLogIndex && Last_Log_Term==args.LastLogTerm){
			rf.VotedFor=args.CandidateId
			rf.RecvHeartBeat=true	// let this vote as the new leader's first heartbeat
			reply.Term=args.Term
			reply.VoteGranted=true
			// fmt.Printf("send the vote to %v\n",args.CandidateId)
			rf.persist()
			rf.Unlock("RequestVote")
			return
		}
	}else if args.Term == rf.currentTerm{
		// fmt.Printf("go into the args term equal with rf vote for %v candidateid %v\n",rf.VotedFor,args.CandidateId)
		// if the term is equal to current term ,generally the vote for must not be -1
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId{
			Last_Log_Index := rf.index_map_f_1(len(rf.Log))-1
			Last_Log_Term := rf.LastIncludeTerm
			if rf.index_map_f(Last_Log_Index) > -1{
				Last_Log_Term = rf.Log[len(rf.Log)-1].Term
			}
			// fmt.Printf("@@@@@@ vote request from %v term %v == server %v term %v\n",args.CandidateId,args.Term,rf.me,rf.currentTerm)
			// fmt.Printf("@@@@@@ last log term is %v args last term index %v\n",Last_Log_Term,args.LastLogTerm)
			// fmt.Printf("@@@@@@ last log index is %v args last log index %v\n",Last_Log_Index,args.LastLogIndex)
			if args.LastLogTerm > Last_Log_Term || (Last_Log_Index<=args.LastLogIndex && Last_Log_Term==args.LastLogTerm){
				rf.currentTerm=args.Term
				rf.state=RaftFollower
				rf.VotedFor=args.CandidateId
				rf.RecvHeartBeat=true	// let this vote as the new leader's first heartbeat
				reply.Term=args.Term
				reply.VoteGranted=true
				rf.persist()
				rf.Unlock("RequestVote")
				return
			}
		}
	}
	reply.Term=rf.currentTerm
	reply.VoteGranted=false
	rf.Unlock("RequestVote")
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

func (rf *Raft) sendRequestVoteAll(){	
	// update the infos
	rf.Lock("sendRequestVoteAll")
	// write new info
	rf.currentTerm+=1
	// fmt.Printf("%v start vote term %v\n",rf.me,rf.currentTerm);
	rf.RecvHeartBeat=true
	rf.state=RaftCandidate
	rf.CollectVote=1	// vote to me
	rf.VotedFor = rf.me
	rf.persist()
	// read info from the rf,for the information might change
	cur_Term := rf.currentTerm
	Last_Log_Index := rf.index_map_f_1(len(rf.Log)-1)
	Last_Log_Term := rf.LastIncludeTerm
	if rf.index_map_f(Last_Log_Index) > -1{
		Last_Log_Term = rf.Log[len(rf.Log)-1].Term
	}

	for i :=0;i<len(rf.peers);i++{
		if i!=rf.me{
			args := RequestVoteArgs{
				Term	:cur_Term,
				CandidateId	:	rf.me,
				LastLogIndex :	Last_Log_Index,
				LastLogTerm	:	Last_Log_Term,
			}
			go rf.CollectVoteRes(i,&args)	// we have to send all the infos to other server ,so we must use other thread ,not a sequence way
		}
	}
	rf.Unlock("sendRequestVoteAll")
}
func (rf *Raft) CollectVoteRes(server int, args *RequestVoteArgs){
	reply := RequestVoteReply{}
	// fmt.Printf("$$$$$$ %v try to collect vote from %v\n",rf.me,server)
	ok	:= rf.sendRequestVote(server,args,&reply)
	if !ok {
		return
	}
	// fmt.Printf("$$$$$$ %v got a vote from %v\n",rf.me,server)
	// if reply.VoteGranted == false{
	// 	fmt.Printf("this vote is fault\n");
	// }
	rf.Lock("CollectVoteRes")
	if rf.state == RaftFollower || args.Term != rf.currentTerm{	// the state has already been changed
		rf.Unlock("CollectVoteRes")
		return
	}
	if reply.Term > rf.currentTerm{	// is old
		rf.currentTerm=reply.Term
		// rf.VotedFor = -1
		rf.state=RaftFollower
		// and the vote granted must be false
		rf.persist()
		rf.Unlock("CollectVoteRes")
		return
	}
	if reply.VoteGranted == false{
		rf.Unlock("CollectVoteRes")
		return
	}
	if rf.CollectVote > len(rf.peers)/2 {	// already be voted as leader and no need for another heartbeat thread
		rf.Unlock("CollectVoteRes")
		return
	}
	rf.CollectVote +=1
	if rf.CollectVote > len(rf.peers)/2 {
		// fmt.Printf("me %v is voted for raft leader with collect %v term %v\n",rf.me,rf.CollectVote,rf.currentTerm)
		rf.state = RaftLeader
		// after every election，
		// the nextIndex[] initialized with the leader's lastlogindex + 1
		Next_Log_Index := rf.index_map_f_1(len(rf.Log))
		for i:=0;i<len(rf.peers);i++ {
			rf.NextIndex[i]=Next_Log_Index
			rf.MatchIndex[i]=0
		}
		// the matchIndex[] initialized with all 0
		// fmt.Printf("leader is %v\n",rf.me)
	}
	rf.Unlock("CollectVoteRes")
	go rf.sendHeartBeatsAll()
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func PrintHeartBeatsFrameArgs(args AppendEntriesArgs){
	fmt.Printf("====== ====== ======\n")
	fmt.Printf("\tTerm\t%v\n",args.Term)
	fmt.Printf("\tLeader\t%v\n",args.LeaderId)
	fmt.Printf("\tPrevLogIndex\t%v\n",args.PrevLogIndex)
	fmt.Printf("\tPrevLogTerm\t%v\n",args.PrevLogIndex)
	fmt.Printf("\tEntry len\t%v\n",len(args.Entries))
	fmt.Printf("\tLeader commit\t%v\n",args.LeaderCommit)
	fmt.Printf("====== ====== ======\n")
}
func PrintHeartBeatsFrameReply(reply AppendEntriesReply){
	fmt.Printf("====== ====== ======\n")
	fmt.Printf("\tTerm\t%v\n",reply.Term)
	fmt.Printf("\tSuccess\t%v\n",reply.Success)
	fmt.Printf("\tXTerm\t%v\n",reply.XTerm)
	fmt.Printf("\tXIndex\t%v\n",reply.XIndex)
	fmt.Printf("\tXLen\t%v\n",reply.XLen)
	fmt.Printf("====== ====== ======\n")
}
func (rf *Raft) PrintLogEntries(){
	fmt.Printf("====== ====== ======\n")
	fmt.Printf("<S%v:LogLen%v:Cmt%v:Term%v>\n\t",rf.me,rf.index_map_f_1(len(rf.Log)),rf.CommitIndex,rf.currentTerm)
	// start:=0
	fmt.Printf("<LII:%v|LIT:%v>\n",rf.LastIncludeIndex,rf.LastIncludeTerm)
	start:=len(rf.Log)-10
	if start < 0{
		start = 0
	}
	for i:=start;i<len(rf.Log);i++{
		fmt.Printf("[T%v:I%v:C%v]",rf.Log[i].Term,rf.index_map_f_1(i),rf.Log[i].Command)
	}
	fmt.Printf("\n")
	fmt.Printf("====== ====== ======\n")
}
// HeartBeats
func (rf *Raft) sendHeartBeatsAll_one_round(){
	// fmt.Printf("###### leader log len is %v\n",len(rf.Log))
	for i :=0;i<len(rf.peers);i++{
		// fmt.Printf("start set heart beats to all servers\n")
		if i!=rf.me && rf.state == RaftLeader{
			i_th_Pre_Log_Index := rf.NextIndex[i]-1
			i_th_Pre_Log_Term := rf.LastIncludeTerm
			// fmt.Printf("prev log index is %v,last include index is %v\n",i_th_Pre_Log_Index,rf.LastIncludeIndex)
			if rf.index_map_f(i_th_Pre_Log_Index) >= -1{
				if rf.index_map_f(i_th_Pre_Log_Index) > -1 {
					i_th_Pre_Log_Term=rf.Log[rf.index_map_f(i_th_Pre_Log_Index)].Term
				}
				var new_entries []LogEntry
				// fmt.Printf("###### leader %v len is %v think %v prev is %v\n",rf.me,len(rf.Log),i,rf.NextIndex[i]-1)
				if len(rf.Log)-1 >= rf.index_map_f(i_th_Pre_Log_Index)+1{
					new_entries = rf.Log[rf.index_map_f(i_th_Pre_Log_Index)+1:]
				}
				args := AppendEntriesArgs{
					Term:	rf.currentTerm,
					LeaderId:	rf.me,
					PrevLogIndex:	i_th_Pre_Log_Index,
					PrevLogTerm:	i_th_Pre_Log_Term,
					Entries:	new_entries,
					LeaderCommit:	rf.CommitIndex,
				}
				
				// if len(new_entries)>0{
				// 	fmt.Printf("====== send append to server %v prev log index is %v\n",i,i_th_Pre_Log_Index)
				// 	PrintHeartBeatsFrameArgs(args)
				// }
				go rf.sendHeartBeat(i,&args)	// also use other threads to deal with the 
			}else{	//the rf.index_map_f(i_th_Pre_Log_Index) < -1 means the position is trimmed
				// fmt.Printf("<L%v:T%v> send the snapshot to server %v,the i_th_Pre_Log_Index is %v,index mapped is %v\n",rf.me,rf.currentTerm,i,i_th_Pre_Log_Index,rf.index_map_f(i_th_Pre_Log_Index))
				go rf.sendSnapShot(i)
			}
		}
	}
}
func (rf *Raft) sendHeartBeatsAll(){
	send_heart_beat_count := 0
	// time should be the same as the ticker
	// rf.PrintLogEntries()
	for rf.killed() == false {
		// fmt.Printf("%v send heartbeat\n",rf.me);
		rf.Lock("sendHeartBeatsAll")
		if rf.state != RaftLeader{
			rf.Unlock("sendHeartBeatsAll")
			break
		}
		rf.sendHeartBeatsAll_one_round()
		rf.Unlock("sendHeartBeatsAll")
		
		ms := 100	//heartbeat RPCs no more than ten times per second.
		time.Sleep(time.Duration(ms) * time.Millisecond)
		send_heart_beat_count++
	}
}
func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs){
	// fmt.Printf("send heart beat to %v\n",server)
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server,args,&reply)
	if !ok {
		return
	}
	// fmt.Printf("====== %v receive a message from %v\n",rf.me,server)
	// PrintHeartBeatsFrameReply(reply)
	rf.Lock("sendHeartBeat")
	// check self state
	if rf.state != RaftLeader || rf.currentTerm!=args.Term {
		// self might be elected as a leader at another term
		// so we also need to check the term
		rf.Unlock("sendHeartBeat")
		return
	}
	
	if reply.Term > rf.currentTerm {
		// fmt.Printf("leader %v receive a msg and change to follower reply term is %v current term is %v\n",rf.me,reply.Term,rf.currentTerm)
		rf.currentTerm =  reply.Term
		rf.VotedFor = -1
		rf.state = RaftFollower
		rf.persist()
		rf.Unlock("sendHeartBeat")
		return
		// if he is not the leader ,cannot continue change the commit
	}else if reply.Term < rf.currentTerm{
		//ignore,will this case happen???
		fmt.Printf("unexpected reply.Term < rf.currentTerm\n")
	}else if reply.Term == rf.currentTerm{
		if reply.Success == true{
			rf.MatchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.NextIndex[server] = rf.MatchIndex[server] + 1
			// fmt.Printf("******server %v success,and args.PrevLogIndex is %v len %v\n",server,args.PrevLogIndex,len(args.Entries))
			// fmt.Printf("<%v:%v> rf match is %v rf next is %v\n",rf.me,server,rf.MatchIndex[server],rf.NextIndex[server])
		} else if reply.Success == false{
			// seems need retry???
			if reply.XTerm == -1{
				// fmt.Printf("reply's xlen is %v\n",reply.XLen)
				rf.NextIndex[server] = reply.XLen	// no conflict, is that possible???
				// we update the new nextindex , and we do no't update ,but update it at next round 
			} else {
				// Actually, the index at follower must exist in real log, as it must commited
				// but might not in the leader, for it might be trimmed,so this 
				conflict_idx := rf.NextIndex[server] - 1
				for rf.index_map_f(conflict_idx) > 0 && rf.Log[rf.index_map_f(conflict_idx)].Term > reply.XTerm{
					conflict_idx--
				}
				if rf.index_map_f(conflict_idx)>=0 && rf.Log[rf.index_map_f(conflict_idx)].Term == reply.XTerm{
					rf.NextIndex[server] = conflict_idx + 1
				} else {
					rf.NextIndex[server] = reply.XIndex
				}
			}
			rf.Unlock("sendHeartBeat")
			return
		}
	}
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	// use two for loop: 
	// outer decrese the N to test
	// inner check all the match index
	for N := rf.index_map_f_1(len(rf.Log))-1 ; N > rf.CommitIndex ; N--{
		total_num := 0
		for i:=0;i<len(rf.peers);i++{
			if i==rf.me{
				total_num++
			}else{
				if rf.MatchIndex[i]>=N && rf.index_map_f(N) > -1 && rf.Log[rf.index_map_f(N)].Term ==rf.currentTerm{
					total_num++
				}
			}
			if total_num > len(rf.peers)/2{
				rf.CommitIndex=N
				break
			}
		}
	}
	// need to send Apply Msg to ApplyCh
	rf.Unlock("sendHeartBeat")
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
// the heart beat is the same as the append entry,but just the log entries are empty
func (rf *Raft) AppendEntries(args *AppendEntriesArgs,reply *AppendEntriesReply){
	// fmt.Printf("%v receive heart beat term %v cur term %v leader id is %v\n",rf.me,args.Term, rf.currentTerm,args.LeaderId)
	rf.Lock("AppendEntries")
	reply.XTerm=-1
	reply.XIndex=-1
	reply.XLen=rf.index_map_f_1(len(rf.Log))
	if args.Term < rf.currentTerm{
		// fmt.Printf("args term unequal to rf current term\n")
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.Unlock("AppendEntries")
		return
	}else if args.Term == rf.currentTerm{
		
	}else if args.Term > rf.currentTerm{
		rf.currentTerm=args.Term
		// rf.VotedFor = -1
		rf.state= RaftFollower
		rf.persist()
	}
	rf.RecvHeartBeat=true
	// add reply false if log doesn't contain any entry at prevLogIndex whose term matches prevLogTerm
	// if the prevLogIndex have log
	if args.PrevLogIndex >= rf.index_map_f_1(len(rf.Log)){	// e.g. only one log,the len is 1,and the prevlogindex must be 0,it must less than len
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.Unlock("AppendEntries")
		// fmt.Printf("###### server %v here return false args.prev index %v len %v\n",rf.me,args.PrevLogIndex,len(rf.Log))
		return
	}
	// if have the log but the log term is not match
	// actually, the follower's log is trimmed but not committed case will never happen
	// I mean no need to check the rf.Log[rf.index_map_f(args.PrevLogIndex)].Term is exist or not
	if rf.index_map_f(args.PrevLogIndex) < -1 {
		// fmt.Printf("<S%v> the rf.index_map_f(args.PrevLogIndex) <map-|%v:no_map-|%v> is less then 0, not in consider\n",rf.me,rf.index_map_f(args.PrevLogIndex),args.PrevLogIndex)
		// rf.PrintLogEntries()
		reply.Term=rf.currentTerm
		reply.Success = false
		// we think it is a old package and do nothing
		reply.XTerm = rf.LastIncludeTerm
		reply.XIndex = rf.LastIncludeIndex
		rf.Unlock("AppendEntries")
		return
	}else if rf.index_map_f(args.PrevLogIndex) == -1 {
		if args.PrevLogTerm != rf.LastIncludeTerm{
			// remember that the args.PervLogIndex might be -1
			reply.Term = rf.currentTerm
			reply.Success = false
			// meet a problem that the len is larger then the leader,hance the return len might be larger then the nextindex[server]
			reply.XTerm = rf.LastIncludeTerm
			reply.XIndex= args.PrevLogIndex
			rf.Unlock("AppendEntries")
			// fmt.Printf("###### server %v here return false args.prev term %v rf term %v\n",rf.me,args.PrevLogTerm,rf.Log[args.PrevLogIndex].Term)
			return
		}
	}else if args.PrevLogTerm != rf.Log[rf.index_map_f(args.PrevLogIndex)].Term{
		// remember that the args.PervLogIndex might be -1
		reply.Term = rf.currentTerm
		reply.Success = false
		// meet a problem that the len is larger then the leader,hance the return len might be larger then the nextindex[server]
		reply.XTerm = rf.Log[rf.index_map_f(args.PrevLogIndex)].Term
		var xindex int
		for conflict_idx := args.PrevLogIndex; rf.index_map_f(conflict_idx) >= 0 && rf.Log[rf.index_map_f(conflict_idx)].Term == rf.Log[rf.index_map_f(args.PrevLogIndex)].Term;conflict_idx--{
			xindex = conflict_idx
		}
		reply.XIndex= xindex
		rf.Unlock("AppendEntries")
		// fmt.Printf("###### server %v here return false args.prev term %v rf term %v\n",rf.me,args.PrevLogTerm,rf.Log[args.PrevLogIndex].Term)
		return
	}
	
	// prev check is for the log before the prev_log_index
	// the following is used for the logs after the next_log_index
	if len(args.Entries)!=0{
		// fmt.Printf("<%v> have receve a log append request with len %v,args.PrevLogIndex is %v\n",rf.me,len(args.Entries),args.PrevLogIndex)
		// check whether there have any conflict
		next_log_index := args.PrevLogIndex + 1
		conflict_idx := -1
		have_cmp := false
		for i:=0 ; next_log_index + i < rf.index_map_f_1(len(rf.Log)) && i < len(args.Entries) ; i++{
			have_cmp = true
			if rf.Log[rf.index_map_f(next_log_index + i)].Term != args.Entries[i].Term{
				// fmt.Printf("conflict at %v\n",next_log_index+i)
				conflict_idx = i
				break
			}
		}
		// fmt.Printf("conflict idx is %v cmp is %v\n",conflict_idx,have_cmp)
		// delete the existing entry and all that follow it
		if have_cmp && conflict_idx!=-1{
			rf.Log = rf.Log[:rf.index_map_f(next_log_index)+conflict_idx]
		}
		if conflict_idx != -1 {
			rf.Log = append(rf.Log,args.Entries[conflict_idx:]...)
		}else if have_cmp == false{
			conflict_idx = 0
			rf.Log = append(rf.Log,args.Entries[conflict_idx:]...)
		}else if have_cmp == true{
			if args.PrevLogIndex+len(args.Entries) >= rf.index_map_f_1(len(rf.Log)){
				// fmt.Printf("we append\n")
				conflict_idx = 0
				rf.Log = rf.Log[:rf.index_map_f(next_log_index)]
				rf.Log = append(rf.Log,args.Entries[conflict_idx:]...)
			}
		}
		rf.persist()
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// fmt.Printf("leader commit is %v rf commit is %v len log is %v\n",args.LeaderCommit,rf.CommitIndex,len(rf.Log))
	if args.LeaderCommit > rf.CommitIndex{
		if args.LeaderCommit >= rf.index_map_f_1(len(rf.Log))-1{
			rf.CommitIndex = rf.index_map_f_1(len(rf.Log)) -1
		} else if args.LeaderCommit < rf.index_map_f_1(len(rf.Log)) - 1{
			rf.CommitIndex = args.LeaderCommit
		}
		// not the raft required but the lab required: send to the ApplyCh
		// as the commit index might change
	}
	// rf.PrintLogEntries()
	rf.Unlock("AppendEntries")
}
func (rf *Raft) SendApplyMsg(sendsnapshot bool){
	var apply_msg_slice []ApplyMsg
	rf.mu.Lock()
	if sendsnapshot{
		// fmt.Printf("<%v> send snapshot with index %v\n",rf.me,rf.LastIncludeIndex)
		new_apply_msg := ApplyMsg{
			SnapshotValid	:	true,
			Snapshot	:	rf.SnapShot,
			SnapshotIndex	:	rf.LastIncludeIndex,
			SnapshotTerm	:	rf.LastIncludeTerm,
		}
		apply_msg_slice = append(apply_msg_slice,new_apply_msg)
	}else{
		// fmt.Printf("server %v have a commit %v last applied %v\n",rf.me, rf.CommitIndex,rf.LastApplied)
		cmt_Index:=rf.CommitIndex
		if cmt_Index < rf.LastIncludeIndex{
			cmt_Index = rf.LastIncludeIndex
		}
		Last_Applied:=rf.LastApplied
		if Last_Applied < rf.LastIncludeIndex{
			Last_Applied = rf.LastIncludeIndex
		}
		// rf.PrintLogEntries()
		for cmt_Index > Last_Applied && rf.index_map_f_1(len(rf.Log)) > Last_Applied && rf.index_map_f_1(len(rf.Log)) > cmt_Index {
			Last_Applied += 1
			cmd := rf.Log[rf.index_map_f(Last_Applied)].Command
			new_apply_msg := ApplyMsg{
				CommandValid	: true,
				Command			: cmd,
				CommandIndex	: Last_Applied,
			}
			// fmt.Printf("<%v> commit msg term %v index %v command %v\n",rf.me,rf.Log[rf.index_map_f(Last_Applied)].Term,Last_Applied,cmd)
			apply_msg_slice=append(apply_msg_slice,new_apply_msg)	
		}
	}
	rf.mu.Unlock()
	for i:=0;i<len(apply_msg_slice);i++ {
		if apply_msg_slice[i].SnapshotValid{
			rf.mu.Lock()
			Last_Applied := rf.LastApplied
			rf.mu.Unlock()
			if apply_msg_slice[i].SnapshotIndex >= Last_Applied{
				rf.ApplyCh <- apply_msg_slice[i]
			}
		}else {
			rf.mu.Lock()
			if apply_msg_slice[i].CommandIndex != rf.LastApplied + 1{
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()
			// fmt.Printf("<%v> real commit msg index %v command %v\n",rf.me,apply_msg_slice[i].CommandIndex,apply_msg_slice[i].Command)
			rf.ApplyCh <- apply_msg_slice[i]
			rf.mu.Lock()
			if apply_msg_slice[i].CommandIndex != rf.LastApplied + 1{
				rf.mu.Unlock()
				continue
			}
			rf.LastApplied = apply_msg_slice[i].CommandIndex
			rf.mu.Unlock()
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.Lock("Start")
	if rf.killed() || rf.state != RaftLeader{
		isLeader = false
		rf.Unlock("Start")
		return index, term, isLeader
	}
	rf.Log = append(rf.Log,LogEntry{
		Term:rf.currentTerm,
		Command:command,
	})
	rf.sendHeartBeatsAll_one_round()
	index = rf.index_map_f_1(len(rf.Log))-1
	term = rf.currentTerm
	// fmt.Printf("###### after start the leader %v log len is %v term is %v command is %v\n",rf.me,index,term,command)
	rf.persist()
	rf.Unlock("Start")

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
func (rf *Raft) ticker() {
	ticker_count := 0
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		if ticker_count%50==0{
			rf.mu.Lock()
			if rf.state == RaftFollower || rf.state == RaftCandidate{
				if rf.RecvHeartBeat == false {
					// fmt.Printf("server %v find a timeout \n",rf.me)
					go rf.sendRequestVoteAll()
				}else{
					rf.RecvHeartBeat = false
				}
			}
			rf.mu.Unlock()
		}
		// rf.PrintLogEntries()
		ticker_count++
		rf.SendApplyMsg(false)
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 3 + (rand.Int63() % 4)
		// the 2A has a warning of "warning: term changed even though there were no failures"
		// if I use 10 times per second, the heartbeat should be 100 ms
		// and only the time of election timeout larger then the 100 ms, that the timeout never happen
		// so the ms must always larger then 100, so I change the 50-350 to 150-350 ms
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = RaftFollower
	rf.currentTerm=0
	rf.VotedFor = -1
	rf.RecvHeartBeat=false
	rf.ApplyCh = applyCh
	rf.Log=append(rf.Log,LogEntry{})
	rf.Log[0].Term = 0
	rf.CommitIndex= 0
	rf.LastApplied=0

	rf.LastIncludeIndex=0
	rf.LastIncludeTerm=0
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.CommitIndex=rf.LastIncludeIndex
	rf.LastApplied=rf.LastIncludeIndex
	rf.SnapShot = persister.ReadSnapshot()
	for i:=0;i<len(peers);i++{
		rf.NextIndex=append(rf.NextIndex,rf.index_map_f_1(len(rf.Log)))
		rf.MatchIndex=append(rf.MatchIndex,0)
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	ms := 350
	time.Sleep(time.Duration(ms) * time.Millisecond)


	return rf
} 
// 解题逻辑
// 首先当然是按照paper的表格抄数据结构啦
// 2A
// 对于2A，其实就是考虑他三种状态的转换，最开始的时候啥都没有，那就都是一个follower，（所以先在初始化的时候做一些设定）
// 那么follower需要转换为candidate，必须要检测到超时事件，那么先去按照这个逻辑去填入ticker的代码
// 随后他会转换为candidate，这时候，他需要去发起投票，注意一个事情，就是，不能假定任何网络都是完好的，所以需要在发送的时候，是不能上锁的。
// 增加向所有其他server发起投票的函数，并根据返回值做相应的处理
// 随后继续增加收到投票之后的一个处理函数，通过传入的投票参数，决定返回值
// 当满足一定情况的时候，就会转变成为leader
// 对于leader，需要考虑的是，不停的发起心跳（其实这时候不发起心跳是能够跑过2A的测试的，只不过每次timeout都会导致新的vote罢了。
// 增加心跳，这里需要考虑一个事情，就是心跳的函数和发送log entry的函数是一样的，所以，需要留出这部分的代码留到2B实现，这是一个简单的方法。
// 然后再增加回应心跳的函数就行了
// 总体而言是按照一个简单的状态转换的逻辑去实现即可
// 2B
// 2B的实现，首先需要弄一个start，大概的逻辑是，如果是leader就增加entry，否则直接返回false
// 随后需要在上面的发log entry的函数和回应的部分增加添加log entry的部分。

// 2B 遇到的一个bug，在测试的时候，有的时候能过，有的时候不能，我仔细debug之后，得出的结论是：
// 每次在发现不匹配的时候我向后回退了1格，但是，如果log非常长，这需要一定的时间来让他能够回退到匹配的时候，但是如果这个时候，发生了重新选举
// 由于他需要重新设定nextindex，也就是说，这时候会导致，重新开始倒数
// 不巧的是我碰到的测例，就是会发生这个事情，看起来，他有个leader back up quickly over incorrect follower logs
// 这个测例一直过不去，就是这样的原因。
// 我的理解是，他需要立即重试。
// 我增加了快速重试的部分，虽然上面说的leader back up还是挂了
// 另一个bug是数组超过限制，原因是，我弄错了conflict
// 他有两个需要比较的部分，第一个是向前比较，向前比较只有term信息，没有log entry，所以只能配一下term
// 如果这个配不上，就得回退，不能增加，因为缺少旧的log
// 另一个地方在于，向后比较，如果有匹配的就继续添加，如果不匹配就向后的用新的覆盖。
// 所以需要增加向前比较term的部分。然后再更新了xlen什么的东西之后，再返回false
// 然后前者需要更新xterm什么的，后者其实是success的路径，不需要更新xterm（因为跑不到）

// 另一个bug
// 记录一下testbackup的情况

// 开始所有的都提交了一个
// 把leader1 和leader+1分到一个组，剩下三个一个组
// 向leader1提交一大堆，显然这些得不到三个确认，都是未确认的。
// 让剩下三个组一个集群，这时候会重新选举
// 向新的集群提交一大堆
// 在新的集群中，又试图down掉一个server
// 在新的只有两个的集群中继续提交一大堆，显然他们无法被commit
// 试图down掉所有的server
// 然后把之前断掉链接的三个都加进来，也就是一个leader1和他的follower，以及leader2手下down掉的那个server
// 这时候，试图提交50个，看上去一定会成功
// 显然这时候leader是leader1, 他会继续发心跳包，收到了false，去快速回退，但是他会发现自己的term很低，于是重新变成follower
// 不过，问题来了，这新加的50个log，他会发给目前认为是leader的所有server，也就是leader1，然后它term很低，会被term高的那个覆盖掉。
// 所以理论上，收到一个新的追加请求的时候，应该立即发心跳包去试图更新自己的状态？？？
// 全都连回来，会把最后的都提交上去。

// 我遇到的bug是在试图收集所有的vote信息的时候
// 我一开始加了一个判断，是否是ok，或者被投票==false，如果失败就扔掉
// 但是问题来了，如果按照这个逻辑，那么后面根据投票的false结果更新自己的term的事情就做不了了
// 所以需要把这个投票==false的逻辑放在后面。
// 但是即便如此，这个测例仍然存在问题，不过，已经有一定概率能够成功了

// 继续修bug，观察到一个现象，能够成功的里面，追加的log，是有最新的，例如term31,32左右（反正挺大的），但是失败的，无一例外的，最后的log是term4的
// 我修改了对应的查看leader的部分，可以确定的是，他是否有及时的选出新的leader，很可能是能否成功的原因
// 于是继续查看他vote的过程
// 看上去是因为当term相等的时候，需要判定vote for 是否为-1或者为candidate ID，但是在这一步，始终没能正确的投票出去
// 注意到，这时候需要所有三个仍然存活的节点都同意投票给他才可以。

// 最后我加了一个设计，也就是每次投票之后，查看自己是否是leader，如果不是，就清除掉投给自己的票。

// 2C
// 2C其实按照给定的注释做就好了
// 但是我还是遇到了几个bug，一个是数组越界，反正就是少了检查，反正他log肯定不可能小于0，所以换个检查也能过
// 另一个bug，还是一样的问题，不知道为什么，没有能够选出leader。
// 我发现了问题所在，就是说，他如果raft收到了一个vote请求，term更大的，无论如何，都应该更新他的term，而不是只有满足条件判断之后，才更新

// 但是还是过不去，额，emmm简单来说figure 8 unreliable他偶尔会挂掉,我查找资料之后发现这是一个很常见的问题。
// 我查到了一个记录：https://github.com/skywircL/MIT6.824/blob/master/docs/%E9%80%9A%E5%85%B3MIT6.5840(6.824)Lab2%20Raft%E7%9A%84%E6%AD%A3%E7%A1%AE%E5%A7%BF%E5%8A%BF.md
// 我声明，我没有抄袭，但是我确实参考了一些资料来调试bug。
// 这个他说过不了figure 8 unreliable 是因为性能不好，并发比较差。。。
// 他说异步提交即可。。。听上去可行
// 我试图在ticker里面增加msgcommit，但是，2B测试性能反而更差了，最开始我以为是由于commit占用太多时间
// 然后我增加了一个counter，就每三次ticker才提交，变得更差了
// 所以我觉得应该是更低一些。所以设置了，更低的速率就发一个，不过好像还是过不去。
// 另一个说法是，需要在恢复过来的时候再等几秒钟，让重连的认清一下状况。。。但是并没有什么用

// 观察到的一个现象，我在start函数那里做了一个打印，而每次发送心跳（leader）或者触发ticker检查（非leader）的时候，我增加了一次打印
// 首先，增加这个打印之后，他触发fail的次数明显少了，我在第15次才触发了一次fail。（我不确定是否为运气问题）
// 第二个，在fail之前的情况，是他start之后打印次数明显比前面正常的情况多了。就是说，在这之前，一直都是很快就确认了我已经提交正确且成功了，但是现在，即使我的集群，四个节点，或者三个节点，查看记录，都已经出现了出错的那条记录，按道理来说，这时候，集群已经达成了共识，就是说我这个记录已经提交上去了，但是，他会报错。
// 我继续查看到底是哪里出错了，figure8里面的大循环貌似并没有出错，反倒是，在大循环结束之后，需要将所有的节点都连上的时候，出现了问题。
// 看上去是快恢复的逻辑出现了问题。
// 而且我也确实观察到一个现象，就是掉线的几个节点，落后的进度非常的多，说明在快速恢复的代码上存在问题。

// 另外我在试图打印的时候，还碰到了index有问题，确认为，没有把打印函数放在锁的内容里面了，但是同时也发现，applymsg发送函数也没有放在锁的内容里面。所以，我也更改了这个内容。

// 6.8: 终于发现了问题所在，append entries的时候，xindex指的是xterm的起始的index的位置，因此结束for循环的条件不应该为跟rf.currentTerm比较是否相等，而是应该跟prevlogindex的term相比较。
// 目前，大概几十次的重试运行Figure8 unreliable均没有问题。
// 另外，我修复了一个问题，就是心跳true和false的设置的问题，在AppendEntries函数中，如果一个follower落后很多，当他收到来自于leader的信息的时候，他一定会认为收到了心跳
// 而在我原先的实现中，他如果落后了挺多，就会处理了xterm的逻辑之后立马就返回，而心跳位的位置放在后面。
// 这会导致，如果一个follower落后了太多，他会一直没有处理心跳，从而触发选举逻辑。
// 解决办法是，将这个心跳的判断放在之前。

// 网上找了个脚本，https://gist.github.com/JJGO/0d73540ef7cc2f066cb535156b7cbdab
// 在这里，使用python并行运行，可以大幅度的减少我的测试时间
// 我测试了1000次之后，发现造成了5个错误。
// 分别是3次apply error，一次send apply的时候，有index out range，还有一次则依然是之前那个figure 8 unreliable

// 总之，我认为，他的apply应该还是有问题的。于是我试图减少sendapply的频率，这在第二轮的测试中，减少了apply error
// 所以可以认为，之前有问题，就是因为，单纯的因为，频率太高了，但是仍然出现了index out range
// 最后我简单粗暴地把每5次apply，每25次timeout给改成了24次apply，25次timeout，每次大概睡6+rand in 8我认为原因还是出在我没有使用两个定时函数，导致两者不是互质然后容易同时发生，同时发生的时候，容易寄了。
// 但是测试这么多次，仍然不能说明他是bug free的，我认为这里面还是有问题。
// 在使用上述的配置的时候，剩下的问题只有index out range。额，再说吧
// 2D
// 首先我先来添加相应的数据结构，主要是新的installsnapshot rpc，同时，看上去，需要在rf结构体里面增加对应的内容
// 然后我遇到的一个问题在于他死锁了，查看一堆资料之后，发现，上面把applymsg全都放到锁里面不合适，因此，我修改了sendapplymsg的方式。
// 修复一些边界条件后，可以正常跑，但是现在遇到的问题是，会产生timeout 10m的情况。而且就算能过，这个性能也非常的慢。大概需要跑到360s更多。而我看到的其他的实现会少将近100s
// 因此继续检查。
// 另一个怀疑的对象是，由于第一次都运行成功了，但是第二次可能存在锁死的情况。这是另一个怀疑对象。毕竟是同一个程序跑的。
// 我检查了输出，发现不是，而是在某一个时刻之后，他没能正确的选出leader。这应该是在选leader的逻辑中，因为snapshot的问题，导致没能成功。
// 我继续查看了for循环，因为按照逻辑，锁死这件事情本身并不会发生，另一种可能是，在上锁之后，有个for循环死循环了
// 就是Append entries中的conflict_idx的地方出了问题，这也符合逻辑，也就是说，并不总是会运行到这一步。但是偶尔会遇到，所以造成了死锁的问题。

// OK，总体基本测试是能过的，但是仍然会有apply error，也就是顺序出现问题的情况。
// 因为我的时间不是很够了，我得先做3A去了。
// 不过，我还是记录一下我测试之后出现问题的测例以及出现的问题。我会运行足够多的次数，基本会在具体测例出现的错误都覆盖住
// 另一个我遵循的原则是，同时测试的线程数量设为32（我的电脑是16个核心，弄成两倍）
// 因为在我之前200个并行的时候，确实存在无法达成一致，但是问题在于，他有很高的CPU占用率和磁盘读写率，也占用了很多的内存，肯定是被置换出去了。
// 而这种情况下，导致他无法正常按时agree，那不是非常正常的事情嘛？？？因为在磁盘上，加上调度不过来，很容易就挂了
// 因此后续的一个优化是，减少他吃的资源数目，同时运行时间不变（TODO）

// install snapshots (disconnect+unreliable)
// 以及install snapshots (unreliable+crash)
// 经常出现apply error: server x apply out of order, expected index x, got x
// unreliable churn
// Figure 8 (unreliable) apply error: commit index= x server=a y != server=b z
// TestBasicAgree2B (2.00s) config.go:594: one(100) failed to reach agreement(确定发生了，所以打算从这个开始修)

// 我复制一段log吧
// ====== ====== ======
// leader is 1
// ====== ====== ======
// ====== ====== ======
// ###### leader 1 len is 1 think 0 prev is 0
// ###### leader 1 len is 1 think 2 prev is 0
// send heart beat to 2
// Test (2B): basic agreement ...
// ###### after start the leader 1 log len is 1 term is 1 command is 100
// leader is 0
// ====== ====== ======
// <S0:LogLen1:Cmt0:Term2>
// 	<LII:0|LIT:0>
// [T0:I0:C<nil>]
// ====== ====== ======
// ###### leader 0 len is 1 think 1 prev is 0
// ###### leader 0 len is 1 think 2 prev is 0

// 在这里本来leader是1，但是因为启动的时候，也许做了一些事情，导致0超时了
// 然后还没来得及把自己的log复制给其他人，leader地位就变成了0
// 这个从逻辑上来说，没有问题，完全可行。
// 那问题来了，如何避免这个事情呢。最开始一个简单的想法是，加个等待时间，等心跳函数复制过去，看看是否自己还是leader。
// 但是会有问题。
// 对于2D中遇到的问题，我查看了一下，就是提交给slice的，index是没有问题的。有问题的，是从slice提交给applych的
// 也就是说，由于ticker不停地乱序go sendapplymsg，他们本身是锁定的，所以给slice的，是没问题的。但是后面一段没锁定的，则不是
// 大概是说，比如ticker的从99发到了104，都发完了，这时候，来个snapshot，说只到99，接下来要100，然后接下来肯定发105，那这不就出错了嘛
// 所以只需要提交snapshot的时候检查一下当前自己提交到哪里了就行。
// 而，虽然提交的时候没有锁定，但是查看的是lastapplied的，这个是递增的。所以如果检查的点不对，之后只可能更大，那就够了
// 对于C则不是这样，C中的错误，他本身给slice的就有问题。
// C的问题貌似是这样的，就是figure8的问题，我在收到了False的reply之后，没有返回，而是继续回到判定Append 到majority的逻辑中去。
// 好吧，C的根本问题其实是我Appendentries这里，向后添加log的代码出现了问题。
// 具体来说是这样的，如果先有一个长的log，再有一个短的log append请求，而且都对的上号，我原先的处理方式，会把他从prev开始变成短的log
// 但是实际上不能这样截断，这有可能是个落后的。

// 最后的最后，其实我实测，在2B和2D中，640次仍然各有一次，会发生问题，而且都是基础测例出错。
// 2B 的修复：这个修复其实很简单，我看了一下输出的log，他出错的原因是，在开始，去leader那边交了一个，但是这时候因为开始是一堆人同时timeout的，所以这时候没有任何一个leader。
// 这个时候会导致第一个leader选出来，但是start之后，立马又被新的leader给覆盖了。
// 这时候你说是否是一致的，这肯定是一致的，但是这个一致是，没有任何提交的一致，而不是会有一个提交的一致。
// 所以他一看一直没有提交，那就完蛋了。
// 所以解决这个问题的办法其实很简单，在开头make的时候加个timedelay即可，让他在开始，能够达成一致说我们已经有一个leader了，大家都很稳定了