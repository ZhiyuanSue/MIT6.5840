package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type ask_type int
const (
	ask_type_ask	ask_type = 0
	ask_type_fin	ask_type = 1
)
type AskTaskArgs struct {
	AskType ask_type
	FinWorkId	int
	HaveFinishWork	bool
	WorkerId int
}
type work_type int
const (
	work_type_map	work_type = 0
	work_type_reduce	work_type = 1
	work_type_wait	work_type = 2
	work_type_finish	work_type = 3
)
type AskTaskReply struct {
	WorkId int
}
type AskEnv struct{
}
type AskEnvReply struct{
	Files []string
	NReduce int
	WorkerId	int
}

type AskWorkCurWorker struct{
	WorkId int
	WorkerId int
}
type AskWorkCurWorkerReply struct{
	Res bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
