package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0


const (
	OpPut 	 = "Put"
	OpAppend = "Append"
	OpGet	 = "Get"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Index	int	//call start() return Index, Term
	Term	int
	Key 	string
	Value 	string
	OpType	string	//Put, Append, Get
	ClientId 	int64
	SeqId 		int64
}

// 等待Raft提交期间的Op上下文, 用于唤醒阻塞的RPC
type OpContext struct {
	op Op
	committed 	chan struct{}

	wrongLeader bool 	// 因为index位置log的term不一致, 说明leader换过了
	// ignored 	bool		// 因为req id过期, 导致该日志被跳过执行

	// Get操作的结果
	keyExist 	bool
	value 		string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db 		map[string]string	//应用层数据库
	reqMap	map[int]*OpContext	//log index -> 请求上下文
	seqMap	map[int64]int64 	//查看每个client的日志提交到哪个位置了。	
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{}
	op.Key		= args.Key
	op.OpType	= OpGet
	op.SeqId	= args.SeqId
	op.ClientId	= args.ClientId
	var isLeader bool
	op.Index, op.Term, isLeader = kv.rf.Start(op)
	if (!isLeader) {
		reply.Err = ErrWrongLeader 
		return
	}
	//DPrintf("[KVserver]Key=%v, value=%s, op=%v", op.Key, op.Value, op.Op)
	opCtx := &OpContext{
		op: op,
		committed: make(chan struct{})}
	kv.mu.Lock()
	kv.reqMap[op.Index] = opCtx
	kv.mu.Unlock()

	select {
	case <-opCtx.committed:
		if (opCtx.wrongLeader == true) { // 同样index位置的term不一样了, 说明leader变了，需要client向新leader重新写入
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			if !opCtx.keyExist { 	// key不存在
				reply.Err = ErrNoKey
			} else {
				reply.Value = opCtx.value	// 返回值
			}
		}
	case <-time.After(2 * time.Second):
		reply.Err = ErrWrongLeader
        DPrintf("[KVserver]Get(%v) timeout 2s", args.Key)
	}
	//DPrintf("[%d] Get RPC, Key=%v, reply.Value=%v, reply.Err=%v", kv.me, args.Key, reply.Value, reply.Err)
	func() {
		//超时或处理完毕则清理上下文
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if opContext, ok := kv.reqMap[op.Index]; ok {
			if (opContext == opCtx) {
				delete(kv.reqMap, op.Index)
			}
		}
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{}
	op.Key = args.Key
	op.Value = args.Value
	op.OpType = args.Op
	op.ClientId = args.ClientId
	op.SeqId = args.SeqId

	var isLeader bool
	op.Index, op.Term, isLeader = kv.rf.Start(op)
	if (isLeader == false) {
		reply.Err = ErrWrongLeader 
		return
	}
	//DPrintf("[%d] PutAppend RPC,OpType=%v, Key=%v, Value=%v", kv.me, args.Op, args.Key, args.Value)
	opCtx := &OpContext{
		op: op,
		committed: make(chan struct{}),
	}
	kv.mu.Lock()
	kv.reqMap[op.Index] = opCtx
	kv.mu.Unlock()
	select {
	case <-opCtx.committed:	// 如果提交了
		if opCtx.wrongLeader {	// 同样index位置的term不一样了, 说明leader变了，需要client向新leader重新写入
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
	case <-time.After(2 * time.Second):	
		reply.Err = ErrWrongLeader
        DPrintf("[KVserver]%v(%v, %v) timeout 2s", args.Op, args.Key, args.Value)
	}
	//DPrintf("[%d] PutAppend RPC,OpType=%v, Key=%v, Value=%v, reply.Err=%v", kv.me, args.Op, args.Key, args.Value, reply.Err)
	func() {
	//超时或者已经处理，则清理上下文
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if one, ok := kv.reqMap[op.Index]; ok {
			if (one == opCtx) {
				delete(kv.reqMap, op.Index)
			}
		}
	}()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyLoop() {
	for !kv.killed() {
		applyMsg  := <-kv.applyCh
		op := applyMsg.Command.(Op)
		index := applyMsg.CommandIndex
		term := applyMsg.CommandTerm
		//DPrintf("[%d] applyloop,index=%v,OpType=%v,Key=%v,Value=%v", kv.me, index, op.OpType, op.Key, op.Value)
		func() {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			//DPrintf("[%v]kv.reqMap=%v", kv.me, kv.reqMap)
			opCtx, existOpt := kv.reqMap[index]
			prevSeq, existSeq := kv.seqMap[op.ClientId]
			
			//分析了一下，实际上请求号顶多相等，并不会op.SeqId < prevSeq的情况。
			if (!existSeq || op.SeqId > prevSeq) {
				kv.seqMap[op.ClientId] = op.SeqId
			}

			if (op.OpType == OpPut || op.OpType == OpAppend) {
				if (!existSeq || op.SeqId > prevSeq) {	//第一次记录该客户端，或者请求号是递增的
					if (op.OpType == OpPut) {
						kv.db[op.Key] = op.Value
					} else {
						if _, exist := kv.db[op.Key]; exist {
							kv.db[op.Key] += op.Value
						} else {
							kv.db[op.Key] = op.Value
						}
					}
				}
			}

			if (existOpt == true) {	// 存在等待结果的RPC, 那么判断状态是否与写入时一致
				if opCtx.op.Term != term {
					opCtx.wrongLeader = true
				}
				if (op.OpType == OpGet) {
					opCtx.value, opCtx.keyExist = kv.db[op.Key]
				} else if (op.OpType == OpPut || op.OpType == OpAppend) {
					//可能是重复请求的，则老的rpc肯定已经断了，此处无需处理
				}
				close(opCtx.committed)
			}
		}()
	}
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.reqMap = make(map[int]*OpContext)
	kv.seqMap = make(map[int64]int64)

	// You may need initialization code here.
	go kv.applyLoop()

	return kv
}
