package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = 0

const (
	OP_TYPE_PUT    = "Put"
	OP_TYPE_APPEND = "Append"
	OP_TYPE_GET    = "Get"
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
	Type	string	//Put, Append, Get
	ClientId 	int64
	SeqId 		int64
}

// 等待Raft提交期间的Op上下文, 用于唤醒阻塞的RPC
type OpContext struct {
	op        *Op
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
	kvStore map[string]string  // kv存储
	reqMap	map[int]*OpContext	//log index -> 请求上下文
	seqMap	map[int64]int64 	//查看每个client的日志提交到哪个位置了。	
	lastAppliedIndex int // 已应用到kvStore的日志index
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := &Op{}
	op.Key		= args.Key
	op.Type	= OP_TYPE_GET
	op.SeqId	= args.SeqId
	op.ClientId	= args.ClientId
	var isLeader bool
	op.Index, op.Term, isLeader = kv.rf.Start(*op)
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
		if one, ok := kv.reqMap[op.Index]; ok {
			if (one == opCtx) {
				delete(kv.reqMap, op.Index)
			}
		}
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := &Op{}
	op.Key = args.Key
	op.Value = args.Value
	op.Type = args.Op
	op.ClientId = args.ClientId
	op.SeqId = args.SeqId

	var isLeader bool
	op.Index, op.Term, isLeader = kv.rf.Start(*op)
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
		select {
		case msg := <-kv.applyCh:
			// 如果是安装快照
			if !msg.CommandValid {
				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()
					if len(msg.Snapshot) == 0 {	// 空快照，清空数据
						kv.kvStore = make(map[string]string)
						kv.seqMap = make(map[int64]int64)
					} else {
						// 反序列化快照, 安装到内存
						r := bytes.NewBuffer(msg.Snapshot)
						d := labgob.NewDecoder(r)
						d.Decode(&kv.kvStore)
						d.Decode(&kv.seqMap)
					}
					// 已应用到哪个索引
					kv.lastAppliedIndex = msg.LastIncludedIndex
					//DPrintf("KVServer[%d] installSnapshot, kvStore[%v], seqMap[%v] lastAppliedIndex[%v]", kv.me, len(kv.kvStore), len(kv.seqMap), kv.lastAppliedIndex)
				}()
			} else { // 如果是普通log
				cmd := msg.Command
				index := msg.CommandIndex
				term := msg.CommandTerm

				func() {
					kv.mu.Lock()
					defer kv.mu.Unlock()

					// 更新已经应用到的日志
					kv.lastAppliedIndex = index

					// 操作日志
					op := cmd.(Op)

					opCtx, existOp := kv.reqMap[index]
					prevSeq, existSeq := kv.seqMap[op.ClientId]
					kv.seqMap[op.ClientId] = op.SeqId

					if existOp { // 存在等待结果的RPC, 那么判断状态是否与写入时一致
						if opCtx.op.Term != term {
							opCtx.wrongLeader = true
						}
					}

					// 只处理ID单调递增的客户端写请求
					if op.Type == OP_TYPE_PUT || op.Type == OP_TYPE_APPEND {
						if !existSeq || op.SeqId > prevSeq { // 如果是递增的请求ID，那么接受它的变更
							if op.Type == OP_TYPE_PUT { // put操作
								kv.kvStore[op.Key] = op.Value
							} else if op.Type == OP_TYPE_APPEND { // put-append操作
								if val, exist := kv.kvStore[op.Key]; exist {
									kv.kvStore[op.Key] = val + op.Value
								} else {
									kv.kvStore[op.Key] = op.Value
								}
							}
						}
					} else { // OP_TYPE_GET
						if existOp {
							opCtx.value, opCtx.keyExist = kv.kvStore[op.Key]
						}
					}
					//DPrintf("KVServer[%d] applyLoop, kvStore[%v]", kv.me, len(kv.kvStore))

					// 唤醒挂起的RPC
					if existOp {
						close(opCtx.committed)
					}
				}()
			}
		}
	}
}

func (kv *KVServer) snapshotLoop() {
	for !kv.killed() {
		var snapshot []byte
		var lastIncludedIndex int
		// 锁内dump snapshot
		func() {
			// 如果raft log超过了maxraftstate大小，那么对kvStore快照下来
			if kv.maxraftstate != -1 && kv.rf.ExceedLogSize(kv.maxraftstate) {	// 这里调用ExceedLogSize不要加kv锁，否则会死锁
				// 锁内快照，离开锁通知raft处理
				kv.mu.Lock()
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.kvStore)	// kv键值对
				e.Encode(kv.seqMap)	// 当前各客户端最大请求编号，也要随着snapshot走
				snapshot = w.Bytes()
				lastIncludedIndex = kv.lastAppliedIndex
				DPrintf("KVServer[%d] KVServer dump snapshot, snapshotSize[%d] lastAppliedIndex[%d]", kv.me, len(snapshot), kv.lastAppliedIndex)
				kv.mu.Unlock()
			}
		}()
		// 锁外通知raft层截断，否则有死锁
		if snapshot != nil {
			// 通知raft落地snapshot并截断日志（都是已提交的日志，不会因为主从切换截断，放心操作）
			kv.rf.TakeSnapshot(snapshot, lastIncludedIndex)
		}
		time.Sleep(10 * time.Millisecond)
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

	kv.applyCh = make(chan raft.ApplyMsg, 1)	// 至少1个容量，启动后初始化snapshot用
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStore = make(map[string]string)
	kv.reqMap = make(map[int]*OpContext)
	kv.seqMap = make(map[int64]int64)
	kv.lastAppliedIndex = 0

	//DPrintf("KVServer[%d] KVServer starts all Loops, maxraftstate[%d]", kv.me,  kv.maxraftstate)

	go kv.applyLoop()
	go kv.snapshotLoop()

	return kv
}