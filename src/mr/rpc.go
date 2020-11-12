package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"


// Add your RPC definitions here.

type AskArgs struct {
	MachineId int
}

type AskReply struct {
	State int	//0 map 1 reduce 2无任务等待0-1之间的过程
	Filename string	//任务文件名,如果是reduce任务，不需要文件名，因为要对所有key哈希相同的文件做合并
	TaskNumber int	//任务号
	NMap int
	NReduce int
	MachineId int
}

type FinishArgs struct {
	State int	//0: finish map, 1: finish reduce
	TaskNumber int
}

type FinishReply struct {
	State int	//0: 未完成, 1: 完成mapreduce
}
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
