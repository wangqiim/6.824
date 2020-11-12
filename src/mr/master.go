package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
//import "strconv"
// import "fmt"


type Master struct {
	// Your definitions here.
	State int	//0:map 1:reduce 3:完成 
	NMap int	//最大并行map的个数，哈希的个数
	NReduce int	//最大并行reduce的个数，哈希的个数
	MapTask map[int]*Task	//map任务
	ReduceTask map[int]*Task	//reduce任务
	//MachineNum弃用，因为使用到os.Rename覆盖文件，我写到后面意识到是单机，文件可以直接覆盖不用考虑不同机器产生的输出文件会多余
	MachineNum int	//用来给机器分配machine id 从1开始分配 
	Mu sync.Mutex	//只能有一个worker访问
}

type Task struct {
	Filename string	//应该打开的文件名,如果是reduce任务，此项不必须
	//MachineId弃用，因为使用到os.Rename覆盖文件，我写到后面意识到是单机，文件可以直接覆盖不用考虑不同机器产生的输出文件会多余
	MachineId int 	//执行该任务的机器id
	State int	// 0 表示未做，1表示正在做,2表示完成
	Runtime int	//已经运行的时间，每次始终滴答，该记录+1
}

//更新所有task的Runtime+1，如果到达10则认为该机器挂了，需要一个新的机器去完成
func (m *Master)TimeTick() {
	m.Mu.Lock()
	if (m.State == 0) {
		for taskNumber, task := range(m.MapTask) {
			if (task.State == 1) {
				m.MapTask[taskNumber].Runtime = m.MapTask[taskNumber].Runtime + 1
				if (m.MapTask[taskNumber].Runtime >= 10) {	//超过10个始终滴答，默认认为该任务的主机已经挂了
					m.MapTask[taskNumber].State = 0
				}
			}
		}
	} else if (m.State == 1) {
		for taskNumber, task := range(m.ReduceTask) {	//遍历taskNumber
			if (task.State == 1) {
				m.ReduceTask[taskNumber].Runtime = m.ReduceTask[taskNumber].Runtime + 1
				if (m.ReduceTask[taskNumber].Runtime >= 10) {	
					m.ReduceTask[taskNumber].State = 0
				}
			}
		}
	}
	m.Mu.Unlock()
}

func (m *Master)UpdateMasterState() {
	for _, task := range(m.MapTask) {
		//fmt.Println("MapTask.State: ", task.State)
		if (task.State == 0 || task.State == 1) {
			m.State = 0	//处于map阶段
			return
		}
	}
	for _, task := range(m.ReduceTask) {
		if (task.State == 0 || task.State == 1) {
			m.State = 1	//处于reduce阶段
			return
		}
	}
	m.State = 2
}


// Your code here -- RPC handlers for the worker to call.
// 请求任务,返回的state，0:map 1:reduce 2:空转 3:完成
// reply.Filename=""则左右任务被接受了但是还没完全完成
func (m *Master) AskTask(args *AskArgs, reply *AskReply) error {
	m.Mu.Lock()
	// fmt.Println("m.State", m.State)
	reply.State = 2
	reply.NMap = m.NMap
	reply.NReduce = m.NReduce
	if (args.MachineId == 0) {	//分配机器号
		m.MachineNum = m.MachineNum + 1
		reply.MachineId = m.MachineNum
	} else {
		reply.MachineId = args.MachineId
	}
	if (m.State == 0) {	//map
		for taskNumber, task := range(m.MapTask) {
			if (task.State == 0) {
				reply.State = 0
				reply.Filename = task.Filename
				reply.TaskNumber = taskNumber
				m.MapTask[taskNumber].State = 1	//正在做
				break
			}
		}
	} else if (m.State == 1) {	//reduce
		// fmt.Println("m.ReduceTask: ", m.ReduceTask)
		for taskNumber, task := range(m.ReduceTask) {
			// fmt.Println("taskNumber, task =", taskNumber, task)
			if (task.State == 0) {
				reply.State = 1
				reply.TaskNumber = taskNumber
				m.ReduceTask[taskNumber].State = 1	//正在做
				break
			}
		}
	} else {
	}
	m.Mu.Unlock()
	return nil;
}

/*
* master收到FinishTask请求时更新当前master的状态
* 当master的状态是2，mapreduce全部完成时，reply的State置为0终止worker
*/
func (m *Master) FinishTask(args FinishArgs, reply *FinishReply) error {
	m.Mu.Lock()
	//println("master called FinishTask")
	reply.State = 0
	if (args.State == 0) {
		m.MapTask[args.TaskNumber].State = 2	//完成
		m.UpdateMasterState()
		// fmt.Print(m.State)
	} else if (args.State == 1) {
		m.ReduceTask[args.TaskNumber].State = 2
		m.UpdateMasterState()
		if (m.State == 2) {	//所有任务都已经完成
			reply.State = 1
		}
	}
	m.Mu.Unlock()
	return nil;
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	//注册rpc使用http服务
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()

	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	//ret := false
	// Your code here.
	var ret bool;
	if (m.State == 2) {
		ret = true
	} else {
		ret = false
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	mapTask := make(map[int]*Task)
	reduceTask := make(map[int]*Task)
	for i, filename := range(files) {
		mapTask[i] = &Task{Filename: filename, MachineId: 0, State: 0, Runtime: 0}
	}
	for j := 0; j < nReduce; j++ {
		//mr-x-y  范围: x[0,7] y[0, 9], file是统一格式，因此不存它，work自己去拼
		//filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
		reduceTask[j] = &Task{Filename: "", MachineId: 0, State: 0, Runtime: 0}
	}
	m := Master{State: 0, NMap: len(files), NReduce: nReduce, MapTask: mapTask, ReduceTask: reduceTask, MachineNum: 0, Mu: sync.Mutex{}}
	m.server()
	return &m
}