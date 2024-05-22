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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	// "fmt"
	//	"6.5840/labgob"
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

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	isleader = false
	rf.mu.Lock()
	if rf.state == RaftLeader{
		isleader = true
	}
	term = rf.currentTerm
	rf.mu.Unlock()

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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term<rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.VoteGranted=false
		rf.mu.Unlock()
		return
	}else if args.Term >rf.currentTerm{
		// for the new term, VotedFor must be null in this term,vote it
		Last_Log_Index := len(rf.Log)-1
		Last_Log_Term := -1
		if Last_Log_Index != -1{
			Last_Log_Term = rf.Log[len(rf.Log)-1].Term
		}
		if Last_Log_Index<=args.LastLogIndex && Last_Log_Term==args.LastLogTerm{
			rf.currentTerm=args.Term
			rf.state=RaftFollower
			rf.VotedFor=args.CandidateId
			rf.RecvHeartBeat=true	// let this vote as the new leader's first heartbeat
			reply.Term=args.Term
			reply.VoteGranted=true
			rf.mu.Unlock()
			return
		}
	}else if args.Term == rf.currentTerm{
		// if the term is equal to current term ,generally the vote for must not be -1
		if rf.VotedFor == args.CandidateId{
			Last_Log_Index := len(rf.Log)-1
			Last_Log_Term := -1
			if Last_Log_Index != -1{
				Last_Log_Term = rf.Log[len(rf.Log)-1].Term
			}
			if Last_Log_Index<=args.LastLogIndex && Last_Log_Term==args.LastLogTerm{
				rf.currentTerm=args.Term
				rf.state=RaftFollower
				rf.VotedFor=args.CandidateId
				rf.RecvHeartBeat=true	// let this vote as the new leader's first heartbeat
				reply.Term=args.Term
				reply.VoteGranted=true
				rf.mu.Unlock()
				return
			}
		}
	}
	reply.Term=rf.currentTerm
	reply.VoteGranted=false
	rf.mu.Unlock()
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
	// fmt.Printf("%v start vote term %v\n",rf.me,rf.currentTerm);
	// update the infos
	rf.mu.Lock()
	// write new info
	rf.currentTerm+=1
	rf.RecvHeartBeat=true
	rf.state=RaftCandidate
	rf.CollectVote=1	// vote to me
	rf.VotedFor = rf.me
	// read info from the rf,for the information might change
	cur_Term := rf.currentTerm
	Last_Log_Index := len(rf.Log)-1
	Last_Log_Term := -1
	if Last_Log_Index != -1{
		Last_Log_Term = rf.Log[len(rf.Log)-1].Term
	}
	rf.mu.Unlock()

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
}
func (rf *Raft) CollectVoteRes(server int, args *RequestVoteArgs){
	reply := RequestVoteReply{}
	ok	:= rf.sendRequestVote(server,args,&reply)
	if !ok || reply.VoteGranted == false {
		return
	}
	rf.mu.Lock()
	if rf.state == RaftFollower || args.Term != rf.currentTerm{	// the state has already been changed
		rf.mu.Unlock()
		return
	}
	if reply.Term > rf.currentTerm{	// is old
		rf.currentTerm=reply.Term
		rf.state=RaftFollower
		// and the vote granted must be false
		rf.mu.Unlock()
		return
	}
	if rf.CollectVote > len(rf.peers)/2 {	// already be voted as leader and no need for another heartbeat thread
		rf.mu.Unlock()
		return
	}
	rf.CollectVote +=1
	if rf.CollectVote > len(rf.peers)/2 {
		// fmt.Printf("me %v is voted for raft leader with collect %v term %v\n",rf.me,rf.CollectVote,rf.currentTerm)
		rf.state = RaftLeader
		// after every election，
		// the nextIndex[] initialized with the leader's lastlogindex + 1
		Next_Log_Index := len(rf.Log)
		for i:=0;i<len(rf.peers);i++ {
			rf.NextIndex[i]=Next_Log_Index
			rf.MatchIndex[i]=0
		}
		// the matchIndex[] initialized with all 0
	}
	rf.mu.Unlock()
	go rf.sendHeartBeatsAll()
}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
// HeartBeats
func (rf *Raft) sendHeartBeatsAll(){
	// time should be the same as the ticker
	for rf.killed() == false && rf.state == RaftLeader {
		// fmt.Printf("%v send heartbeat\n",rf.me);
		var Prev_Log_Index []int
		rf.mu.Lock()
		cur_Term := rf.currentTerm

		for i:=0;i<len(rf.NextIndex);i++{
			Prev_Log_Index = append(Prev_Log_Index,rf.NextIndex[i]-1)
		}
		commit_idx := rf.CommitIndex
		rf.mu.Unlock()

		for i :=0;i<len(rf.peers);i++{
			if i!=rf.me{
				i_th_Pre_Log_Index := Prev_Log_Index[i]
				i_th_Pre_Log_Term := -1
				if i_th_Pre_Log_Index != -1{
					i_th_Pre_Log_Term=rf.Log[i_th_Pre_Log_Index].Term
				}
				var new_entries []LogEntry
				if len(rf.Log)-1 >= Prev_Log_Index[i]+1{
					new_entries = rf.Log[Prev_Log_Index[i]+1:]
				}
				args := AppendEntriesArgs{
					Term:	cur_Term,
					LeaderId:	rf.me,
					PrevLogIndex:	i_th_Pre_Log_Index,
					PrevLogTerm:	i_th_Pre_Log_Term,
					Entries:	new_entries,
					LeaderCommit:	commit_idx,
				}
				go rf.sendHeartBeat(i,&args)	// also use other threads to deal with the 
			}
		}

		ms := 100	//heartbeat RPCs no more than ten times per second.
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs){
	// fmt.Printf("send heart beat to %v\n",server)
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server,args,&reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	// check self state
	if rf.state != RaftLeader || rf.currentTerm!=args.Term {
		// self might be elected as a leader at another term
		// so we also need to check the term
		rf.mu.Unlock()
		return
	}
	
	if reply.Term > rf.currentTerm {
		rf.currentTerm =  reply.Term
		rf.state = RaftFollower
	}else if reply.Term < rf.currentTerm{
		//ignore,will this case happen???
		
	}else if reply.Term == rf.currentTerm{
		
	}
	rf.mu.Unlock()
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
// the heart beat is the same as the append entry,but just the log entries are empty
func (rf *Raft) AppendEntries(args *AppendEntriesArgs,reply *AppendEntriesReply){
	// fmt.Printf("%v receive heart beat term %v cur term %v leader id is %v\n",rf.me,args.Term, rf.currentTerm,args.LeaderId)
	rf.mu.Lock()
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}else if args.Term == rf.currentTerm{
		// if empty log,just a heart beat
		// else is 2B work
	}else if args.Term > rf.currentTerm{
		rf.currentTerm=args.Term
		rf.state= RaftFollower
		//if empty log, a heart beat from new leader

		// else is 2B work
	}
	rf.RecvHeartBeat=true
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.mu.Unlock()
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
	rf.mu.Lock()
	if rf.killed() || rf.state != RaftLeader{
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}
	rf.Log = append(rf.Log,LogEntry{
		Term:rf.currentTerm,
		Command:command,
	})
	index = len(rf.Log)-1
	term = rf.currentTerm
	rf.mu.Unlock()

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
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
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
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 150 + (rand.Int63() % 200)	
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
	rf.currentTerm=-1
	rf.VotedFor=-1
	rf.CommitIndex=-1
	rf.LastApplied=-1
	rf.RecvHeartBeat=false
	for i:=0;i<len(peers);i++{
		rf.NextIndex=append(rf.NextIndex,0)
		rf.MatchIndex=append(rf.MatchIndex,0)
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


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