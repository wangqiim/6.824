package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
//import "time"
import "sort"
//
// Map functions return a slice of KeyValue.
//
// 
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
// 将reduce任务分成10份，方便master进行管理？
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
/**
 *	1.rpc请求任务，判断是map还是reduce
 *	2.如果是map，请求key string, val string,（在本机读取k-v)执行map，执行完成将任务存储到磁盘上，通知master。
 *	3.如果是reduce，请求key，在磁盘上读取reduce任务并且执行，执行完成后通知master。
*/
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	// uncomment to send the Example RPC to the master.
	//CallExample()
	machineId := 0
	for {
		//睡眠一秒，再接下一个任务
		// time.Sleep(time.Second)
		args := AskArgs{MachineId: machineId}
		reply := AskReply{}
		CallAskTask(&args, &reply)
		machineId = reply.MachineId
		taskNumber := reply.TaskNumber
		//根据reply.State绝对做map还是reduce还是循环等待还是退出（全部完成）
		if (reply.State == 0) {//map
			//fmt.Println("正在做map ", reply.Filename)
			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open mapTask %v", reply.Filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()
			kva := mapf(reply.Filename, string(content))
			//写进mr-taskNumber-y文件中
			WriteMapOutput(kva, taskNumber,reply.NReduce)
		} else if (reply.State == 1) {//reduce
			//fmt.Println("正在做reduce ", reply.Filename)
			//读取json格式的mapoutput文件
			intermediate := []KeyValue{}
			nmap := reply.NMap
			for i := 0; i < nmap; i++ {
				mapOutFilename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(taskNumber)
				inputFile, err := os.OpenFile(mapOutFilename, os.O_RDONLY, 0777)
				if err != nil {
					log.Fatalf("cannot open reduceTask %v", reply.Filename)
				}
				dec := json.NewDecoder(inputFile)
				for {
					var kv []KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv...)
				}
			}
			//排序写入reduceUutput文件,参考提供的mrsequential代码
			sort.Sort(ByKey(intermediate))
			oFilename := "mr-out-" + strconv.Itoa(taskNumber)
			tmpReduceOutFile, err := ioutil.TempFile("", "mr-reduce-*")
			//reduceOutputFile, err := os.OpenFile(oFilename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			//reduceOutputFile, _ := os.OpenFile("mr-out-0", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				//合并value进字符串数组
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tmpReduceOutFile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			tmpReduceOutFile.Close()
			os.Rename(tmpReduceOutFile.Name(), oFilename)
		} else if (reply.State == 2) {	//空转
			continue
		} else if (reply.State == 3) {	//完成
			break
		}
		finishargs := FinishArgs{State: reply.State, TaskNumber: taskNumber}
		finishreply := FinishReply{}
		CallFinishTask(&finishargs, &finishreply)
		if (finishreply.State == 1) {
			break
		}
	}
}

/**
*	把KeyValue数组s中的每个KeyValue按照Key哈希以后取余nReduce决定存入的文件
* 	创建一个buf,防止一次写一个频繁的开关文件
**/
func WriteMapOutput(kva []KeyValue, taskNumber int, nReduce int) bool{
	buf := make([][]KeyValue, nReduce)
	for _, key_val := range(kva) {
		no := (ihash(key_val.Key)) % nReduce
		buf[no] = append(buf[no], key_val)
	}
	for no, key_val_nums := range(buf) { //json格式写入map的输出文件
		mapOutFilename := "mr-" + strconv.Itoa(taskNumber) + "-" + strconv.Itoa(no)
		tmpMapOutFile, error := ioutil.TempFile("", "mr-map-*")
		if error != nil {
			log.Fatalf("cannot open tmpMapOutFile")
		}
		//outFile, _ := os.OpenFile(mapOutFilename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0777)
		enc := json.NewEncoder(tmpMapOutFile)
		err := enc.Encode(key_val_nums)
		if err != nil {
			//fmt.Printf("write wrong!\n")
			return false
		}
		tmpMapOutFile.Close()
		os.Rename(tmpMapOutFile.Name(), mapOutFilename)
	}
	return true
}


func CallAskTask(args *AskArgs, reply *AskReply) {
	call("Master.AskTask", &args, &reply)
	//fmt.Println(reply)
}

func CallFinishTask(args *FinishArgs, reply *FinishReply) {
	call("Master.FinishTask", &args, &reply)
	//fmt.Println(reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	//远程调用Master.Example(args, reply)
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}
