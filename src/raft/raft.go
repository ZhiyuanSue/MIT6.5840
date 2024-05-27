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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.VotedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.Log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var VotedFor int
	var currentTerm int
	var Log []LogEntry
	if	d.Decode(&VotedFor) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&Log) != nil {
		fmt.Printf("<%v> Persist decode fail\n",rf.me)
	} else {
		rf.VotedFor = VotedFor
		rf.currentTerm = currentTerm
		rf.Log = Log
	}
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
	XTerm 	int //term in the conflicting entry (if any)
    XIndex	int //index of first entry with that term (if any)
    XLen	int //log length
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// fmt.Printf("<%v:T%v> get a vote request from %v\n",rf.me,rf.currentTerm,args.CandidateId)
	rf.mu.Lock()
	if args.Term<rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.VoteGranted=false
		rf.mu.Unlock()
		return
	}else if args.Term >rf.currentTerm{
		// for the new term, VotedFor must be null in this term,vote it
		rf.currentTerm=args.Term
		rf.persist()
		rf.state=RaftFollower
		Last_Log_Index := len(rf.Log)-1
		Last_Log_Term := -1
		if Last_Log_Index != -1{
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
			rf.mu.Unlock()
			return
		}
	}else if args.Term == rf.currentTerm{
		// fmt.Printf("go into the args term equal with rf vote for %v candidateid %v\n",rf.VotedFor,args.CandidateId)
		// if the term is equal to current term ,generally the vote for must not be -1
		if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId{
			Last_Log_Index := len(rf.Log)-1
			Last_Log_Term := -1
			if Last_Log_Index != -1{
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
	// update the infos
	rf.mu.Lock()
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
	Last_Log_Index := len(rf.Log)-1
	Last_Log_Term := -1
	if Last_Log_Index != -1{
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
	rf.mu.Unlock()
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
	rf.mu.Lock()
	if rf.state == RaftFollower || args.Term != rf.currentTerm{	// the state has already been changed
		rf.mu.Unlock()
		return
	}
	if reply.Term > rf.currentTerm{	// is old
		rf.currentTerm=reply.Term
		rf.VotedFor = -1
		rf.state=RaftFollower
		// and the vote granted must be false
		rf.persist()
		rf.mu.Unlock()
		return
	}
	if reply.VoteGranted == false{
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
		// fmt.Printf("leader is %v\n",rf.me)
	}
	rf.mu.Unlock()
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
	fmt.Printf("<S%v:LogLen%v:Cmt%v:Term%v>\n\t",rf.me,len(rf.Log),rf.CommitIndex,rf.currentTerm)
	for i:=0;i<len(rf.Log);i++{
		fmt.Printf("[T%v:C%v]",rf.Log[i].Term,rf.Log[i].Command)
	}
	fmt.Printf("\n")
	fmt.Printf("====== ====== ======\n")
}
// HeartBeats
func (rf *Raft) sendHeartBeatsAll(){
	// time should be the same as the ticker
	// rf.PrintLogEntries()
	for rf.killed() == false && rf.state == RaftLeader {
		// fmt.Printf("%v send heartbeat\n",rf.me);
		var Prev_Log_Index []int
		rf.mu.Lock()
		cur_Term := rf.currentTerm

		for i:=0;i<len(rf.NextIndex);i++{
			Prev_Log_Index = append(Prev_Log_Index,rf.NextIndex[i]-1)
		}
		commit_idx := rf.CommitIndex
		// fmt.Printf("###### leader log len is %v\n",len(rf.Log))

		for i :=0;i<len(rf.peers);i++{
			// fmt.Printf("start set heart beats to all servers\n")
			if i!=rf.me && rf.state == RaftLeader{
				i_th_Pre_Log_Index := Prev_Log_Index[i]
				i_th_Pre_Log_Term := -1
				// fmt.Printf("prev log index is %v\n",i_th_Pre_Log_Index)
				if i_th_Pre_Log_Index != -1{
					i_th_Pre_Log_Term=rf.Log[i_th_Pre_Log_Index].Term
				}
				var new_entries []LogEntry
				// fmt.Printf("###### leader %v len is %v think %v prev is %v\n",rf.me,len(rf.Log),i,Prev_Log_Index[i])
				if len(rf.Log)-1 >= i_th_Pre_Log_Index+1{
					new_entries = rf.Log[i_th_Pre_Log_Index+1:]
				}
				args := AppendEntriesArgs{
					Term:	cur_Term,
					LeaderId:	rf.me,
					PrevLogIndex:	i_th_Pre_Log_Index,
					PrevLogTerm:	i_th_Pre_Log_Term,
					Entries:	new_entries,
					LeaderCommit:	commit_idx,
				}
				
				// if len(new_entries)>0{
				// 	fmt.Printf("====== send append to server %v prev log index is %v\n",i,i_th_Pre_Log_Index)
				// 	PrintHeartBeatsFrameArgs(args)
				// }
				go rf.sendHeartBeat(i,&args)	// also use other threads to deal with the 
			}
		}
		rf.mu.Unlock()
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
	// fmt.Printf("====== %v receive a message from %v\n",rf.me,server)
	// PrintHeartBeatsFrameReply(reply)
	rf.mu.Lock()
	// check self state
	if rf.state != RaftLeader || rf.currentTerm!=args.Term {
		// self might be elected as a leader at another term
		// so we also need to check the term
		rf.mu.Unlock()
		return
	}
	
	if reply.Term > rf.currentTerm {
		// fmt.Printf("leader %v receive a msg and change to follower reply term is %v current term is %v\n",rf.me,reply.Term,rf.currentTerm)
		rf.currentTerm =  reply.Term
		rf.VotedFor = -1
		rf.state = RaftFollower
		rf.persist()
		rf.mu.Unlock()
		return
		// if he is not the leader ,cannot continue change the commit
	}else if reply.Term < rf.currentTerm{
		//ignore,will this case happen???
		
	}else if reply.Term == rf.currentTerm{
		if reply.Success == true{
			rf.MatchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.NextIndex[server] = rf.MatchIndex[server] + 1
			// fmt.Printf("******server %v success,and args.PrevLogIndex is %v len %v\n",server,args.PrevLogIndex,len(args.Entries))
			// fmt.Printf("******rf match is %v rf next is %v\n",rf.MatchIndex[server],rf.NextIndex[server])
		} else if reply.Success == false{
			// seems need retry???
			if reply.XTerm == -1{
				// fmt.Printf("reply's xlen is %v\n",reply.XLen)
				rf.NextIndex[server] = reply.XLen	// no conflict, is that possible???
			} else {
				conflict_idx := rf.NextIndex[server] - 1
				for conflict_idx > 0 && rf.Log[conflict_idx].Term > reply.XTerm{
					conflict_idx--
				}
				if conflict_idx>=0 && rf.Log[conflict_idx].Term == reply.XTerm{
					rf.NextIndex[server] = conflict_idx + 1
				} else {
					rf.NextIndex[server] = reply.XIndex
				}
			}
		}
	}
	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N
	// use two for loop: 
	// outer decrese the N to test
	// inner check all the match index
	for N := len(rf.Log)-1 ; N > rf.CommitIndex ; N--{
		total_num := 0
		for i:=0;i<len(rf.peers);i++{
			if i==rf.me{
				total_num++
			}else{
				if rf.MatchIndex[i]>=N && rf.Log[N].Term ==rf.currentTerm{
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
	rf.SendApplyMsg()
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
	reply.XTerm=-1
	reply.XIndex=-1
	reply.XLen=len(rf.Log)
	if args.Term < rf.currentTerm{
		// fmt.Printf("args term unequal to rf current term\n")
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}else if args.Term == rf.currentTerm{
		
	}else if args.Term > rf.currentTerm{
		rf.currentTerm=args.Term
		rf.VotedFor = -1
		rf.state= RaftFollower
		rf.persist()
	}
	// add reply false if log doesn't contain any entry at prevLogIndex whose term matches prevLogTerm
	// if the prevLogIndex have log
	if args.PrevLogIndex >= len(rf.Log){	// e.g. only one log,the len is 1,and the prevlogindex must be 0,it must less than len
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		// fmt.Printf("###### server %v here return false args.prev index %v len %v\n",rf.me,args.PrevLogIndex,len(rf.Log))
		return
	}
	// if have the log but the log term is not match
	if args.PrevLogIndex >= 0 && args.PrevLogTerm != rf.Log[args.PrevLogIndex].Term{
		// remember that the args.PervLogIndex might be -1
		reply.Term = rf.currentTerm
		reply.Success = false
		// meet a problem that the len is larger then the leader,hance the return len might be larger then the nextindex[server]
		reply.XTerm = rf.Log[args.PrevLogIndex].Term
		var xindex int
		for conflict_idx := args.PrevLogIndex; rf.Log[conflict_idx].Term == rf.currentTerm;conflict_idx--{
			xindex = conflict_idx
		}
		reply.XIndex= xindex
		rf.mu.Unlock()
		// fmt.Printf("###### server %v here return false args.prev term %v rf term %v\n",rf.me,args.PrevLogTerm,rf.Log[args.PrevLogIndex].Term)
		return
	}
	rf.RecvHeartBeat=true
	// prev check is for the log before the prev_log_index
	// the following is used for the logs after the next_log_index
	if len(args.Entries)!=0{
		// fmt.Printf("have receve a log append request with len %v\n",len(args.Entries))
		// check whether there have any conflict
		next_log_index := args.PrevLogIndex + 1
		conflict_idx := 0
		for i:=0 ; next_log_index + i < len(rf.Log) && i < len(args.Entries) ; i++{
			if rf.Log[next_log_index + i].Term != args.Entries[i].Term{
				// fmt.Printf("conflict at %v\n",next_log_index+i)
				conflict_idx = i
				break
			}
		}
		// 	fmt.Printf("conflict idx is %v\n",conflict_idx)
		// delete the existing entry and all that follow it
		rf.Log = rf.Log[:next_log_index+conflict_idx]
		rf.Log = append(rf.Log,args.Entries[conflict_idx:]...)
		rf.persist()
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// fmt.Printf("leader commit is %v rf commit is %v len log is %v\n",args.LeaderCommit,rf.CommitIndex,len(rf.Log))
	if args.LeaderCommit > rf.CommitIndex{
		if args.LeaderCommit >= len(rf.Log)-1{
			rf.CommitIndex = len(rf.Log) -1
		} else if args.LeaderCommit < len(rf.Log) - 1{
			rf.CommitIndex = args.LeaderCommit
		}
		// not the raft required but the lab required: send to the ApplyCh
		// as the commit index might change
		rf.SendApplyMsg()
	}
	rf.mu.Unlock()
}
func (rf *Raft) SendApplyMsg(){
	// fmt.Printf("server %v have a commit %v last applied %v\n",rf.me, rf.CommitIndex,rf.LastApplied)
	// rf.PrintLogEntries()
	for rf.CommitIndex > rf.LastApplied {
		rf.LastApplied += 1
		new_apply_msg := ApplyMsg{
			CommandValid	: true,
			Command			: rf.Log[rf.LastApplied].Command,
			CommandIndex	: rf.LastApplied,
		}
		rf.ApplyCh <- new_apply_msg
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
	// fmt.Printf("###### after start the leader %v log len is %v term is %v\n",rf.me,index,term)
	rf.persist()
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
		// rf.PrintLogEntries()
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
	rf.currentTerm=0
	rf.VotedFor=-1
	rf.RecvHeartBeat=false
	rf.ApplyCh = applyCh
	rf.Log=append(rf.Log,LogEntry{})
	rf.Log[0].Term = 0
	rf.CommitIndex= 0
	rf.LastApplied=0
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for i:=0;i<len(peers);i++{
		rf.NextIndex=append(rf.NextIndex,len(rf.Log))
		rf.MatchIndex=append(rf.MatchIndex,0)
	}
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