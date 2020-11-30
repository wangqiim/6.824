package shardmaster


import "../raft"
import "../labrpc"
import "sync"
import "../labgob"
import "log"
import "time"
// import "bytes"

const (
	OP_TYPE_JOIN    = "Join"
	OP_TYPE_LEAVE	= "Leave"
	OP_TYPE_MOVE    = "Move"
	OP_TYPE_QUERY	= "Query"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	reqMap	map[int]*OpContext	//log index -> 请求上下文
	seqMap	map[int64]int64 	//查看每个client的日志提交到哪个位置了。上下文	

	lastAppliedIndex int // 已应用的日志index
	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	Index	int	//call start() return Index, Term
	Term	int
	Args    interface{}	//Join/Leave/Move/QueryArgs
	Type	string		//Join/Leave/Move/Query
	ClientId 	int64
	SeqId 		int64
}

// 等待Raft提交期间的Op上下文, 用于唤醒阻塞的RPC
type OpContext struct {
	op        *Op
	committed 	chan struct{}

	wrongLeader bool 	// 因为index位置log的term不一致, 说明leader换过了
	// ignored 	bool		// 因为req id过期, 导致该日志被跳过执行

	//// Get操作的结果
	//keyExist 	bool
	config	Config
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := &Op{}
	op.Args		= *args
	op.Type		= OP_TYPE_JOIN
	op.SeqId	= args.SeqId
	op.ClientId	= args.ClientId

	reply.WrongLeader, _ = sm.templateHandler(op)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := &Op{}
	op.Args		= *args
	op.Type		= OP_TYPE_LEAVE
	op.SeqId	= args.SeqId
	op.ClientId	= args.ClientId

	reply.WrongLeader, _ = sm.templateHandler(op)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := &Op{}
	op.Args		= *args
	op.Type		= OP_TYPE_MOVE
	op.SeqId	= args.SeqId
	op.ClientId	= args.ClientId

	reply.WrongLeader, _ = sm.templateHandler(op)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := &Op{}
	op.Args		= *args
	op.Type		= OP_TYPE_QUERY
	op.SeqId	= args.SeqId
	op.ClientId	= args.ClientId

	reply.WrongLeader, reply.Config = sm.templateHandler(op)
}

//对Join/Leave/Move/Query四个RPC进行封装
func (sm *ShardMaster) templateHandler(op *Op) (bool, Config) {
	wrongLeader := true
	config := Config{}
	var isLeader bool
	op.Index, op.Term, isLeader = sm.rf.Start(*op)
	if (!isLeader) {
		return true, config
	}
	opCtx := &OpContext{
		op: op,
		committed: make(chan struct{})}
	sm.mu.Lock()
	sm.reqMap[op.Index] = opCtx
	sm.mu.Unlock()
	select {
	case <-opCtx.committed:
		if (opCtx.wrongLeader == true) { // 同样index位置的term不一样了, 说明leader变了，需要client向新leader重新写入
			wrongLeader = true
		} else {
			wrongLeader = false
			if opCtx.op.Type == OP_TYPE_QUERY {
				config = opCtx.config
				//DPrintf("ShardMaster[%d] templateHandler() query finish, opCtx.config = %v", sm.me, config)
			}
		}
	case <-time.After(2 * time.Second):
		wrongLeader = true
        DPrintf("[KVserver] timeout 2s")
	}
	func() {
		//超时或处理完毕则清理上下文
		sm.mu.Lock()
		defer sm.mu.Unlock()
		if one, ok := sm.reqMap[op.Index]; ok {
			if (one == opCtx) {
				delete(sm.reqMap, op.Index)
			}
		}
	}()
    return wrongLeader, config
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) doJoinLeaveMove(op *Op) {
	sm.configs = append(sm.configs, sm.configs[len(sm.configs) - 1])
	//生成最新的config
	config := &sm.configs[len(sm.configs) - 1]
	//Groups map[int][]string是浅拷贝，需要特别小心
	newGroups := make(map[int][]string)
	for k,v := range(config.Groups) {
		newGroups[k] = v
	}
	config.Groups = newGroups
	config.Num++
	switch op.Type{
	case OP_TYPE_JOIN:
		args := op.Args.(JoinArgs)
		for gid, serversList := range(args.Servers) {
			config.Groups[gid] = serversList
		}
		sm.loadBalance(config)
		//DPrintf("ShardMaster[%d] Join finish, config = %v", sm.me, sm.configs)
	case OP_TYPE_LEAVE:	
		args := op.Args.(LeaveArgs)
		for _, gid := range(args.GIDs) {
			delete(config.Groups, gid)	//删除gid
			for i := 0; i < NShards; i++ {	//将分配的Shards置为0
				if (config.Shards[i] == gid) {
					config.Shards[i] = 0
				}
			}
		}
		sm.loadBalance(config)
	case OP_TYPE_MOVE:
		args := op.Args.(MoveArgs)
		config.Shards[args.Shard] = args.GID
		//不需要负载均衡,否则可能导致Move失效
	}
}

//负载均衡
func (sm *ShardMaster) loadBalance(config *Config) {
	//1.遍历Shards，将0(未分配的)分配给负载最小的
	//2.判断负载最大的和最小的差值如果大于1，则从负载大的里拿一个shard到负载小的里
	//如果用key:gid, value:loadsize的平衡树能加快速度，这里我直接暴力遍历
	Groups_LoadSize := make(map[int]int)
	for gid,_ := range(config.Groups) {	//记录gid
		Groups_LoadSize[gid] = 0
	}
	if len(config.Groups) == 0 {	//无group直接退出
		return
	}
	for _, gid := range(config.Shards) { //记录负载量
		if (gid != 0) {
			Groups_LoadSize[gid]++
		}
	}
	for i := 0; i < NShards; i++ { //将未分配的shard分配出去
		if (config.Shards[i] == 0) {
			minGid,_,_,_ := getMinMax(&Groups_LoadSize)
			config.Shards[i] = minGid
			Groups_LoadSize[minGid]++
		}
	}
	minGid, minLoadSize, maxGid, maxLoadSize := getMinMax(&Groups_LoadSize)
	for maxLoadSize - minLoadSize > 1 {
		//DPrintf("debug minGid=%v, minLoadSize=%v, maxGid=%v, maxLoadSize=%v", minGid, minLoadSize, maxGid, maxLoadSize)
		for i := 0; i < NShards; i++ { //负载转移
			if (config.Shards[i] == maxGid) {
				config.Shards[i] = minGid
				break
			}
		}
		Groups_LoadSize[minGid]++
		Groups_LoadSize[maxGid]--
		minGid, minLoadSize, maxGid, maxLoadSize = getMinMax(&Groups_LoadSize)
	}
}

func getMinMax(Groups_LoadSize *map[int]int) (int, int, int, int) {
	minGid := -1
	minLoadSize := -1
	maxGid := -1
	maxLoadSize := -1
	for gid, loadSize := range(*Groups_LoadSize) {
		if (minGid == -1) {
			minGid = gid
			maxGid = gid
			minLoadSize = loadSize
			maxLoadSize = loadSize
		} else {
			if (loadSize < minLoadSize) {
				minGid = gid
				minLoadSize = loadSize
			} else if (loadSize > maxLoadSize) {
				maxGid = gid
				maxLoadSize = loadSize
			}
		}
	}
	return minGid, minLoadSize, maxGid, maxLoadSize
}

func (sm *ShardMaster) applyLoop() {
	for {
		select {
		case msg := <-sm.applyCh:
			// 如果是安装快照
			if !msg.CommandValid {
				//本lab目前不需要快照
				// func() {
				// 	sm.mu.Lock()
				// 	defer sm.mu.Unlock()
				// 	if len(msg.Snapshot) == 0 {	// 空快照，清空数据
				// 		sm.configs = make([]Config, 1)
				// 		sm.configs[0].Groups = map[int][]string{}
				// 		sm.seqMap = make(map[int64]int64)
				// 	} else {
				// 		// 反序列化快照, 安装到内存
				// 		r := bytes.NewBuffer(msg.Snapshot)
				// 		d := labgob.NewDecoder(r)
				// 		d.Decode(&sm.configs)
				// 		d.Decode(&sm.seqMap)
				// 	}
				// 	// 已应用到哪个索引
				// 	sm.lastAppliedIndex = msg.LastIncludedIndex
				// 	//DPrintf("KVServer[%d] installSnapshot, kvStore[%v], seqMap[%v] lastAppliedIndex[%v]", kv.me, len(kv.kvStore), len(kv.seqMap), kv.lastAppliedIndex)
				// }()
			} else { // 如果是普通log
				cmd := msg.Command
				index := msg.CommandIndex
				term := msg.CommandTerm

				func() {
					sm.mu.Lock()
					defer sm.mu.Unlock()

					// 更新已经应用到的日志
					sm.lastAppliedIndex = index

					// 操作日志
					op := cmd.(Op)

					opCtx, existOp := sm.reqMap[index]
					prevSeq, existSeq := sm.seqMap[op.ClientId]
					sm.seqMap[op.ClientId] = op.SeqId

					if existOp { // 存在等待结果的RPC, 那么判断状态是否与写入时一致
						if opCtx.op.Term != term {
							opCtx.wrongLeader = true
						}
					}

					// 只处理ID单调递增的客户端写请求
					if op.Type == OP_TYPE_JOIN || op.Type == OP_TYPE_LEAVE || op.Type == OP_TYPE_MOVE {
						if !existSeq || op.SeqId > prevSeq { // 如果是递增的请求ID，那么接受它的变更
							sm.doJoinLeaveMove(&op)
						}
					} else { // OP_TYPE_Query
						if existOp {
							//do some thing
							queryNum := op.Args.(QueryArgs).Num
							if (queryNum >= len(sm.configs) || queryNum == -1) {
								opCtx.config = sm.configs[len(sm.configs) - 1]
							} else {
								opCtx.config = sm.configs[queryNum]
							}
							//DPrintf("ShardMaster[%d] applyloop query finish, opCtx.config = %v", sm.me, opCtx.config)
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

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	//DPrintf("shardmaster[%d]StartServer", me)
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
    labgob.Register(LeaveArgs{})
    labgob.Register(MoveArgs{})
    labgob.Register(QueryArgs{})
	sm.applyCh = make(chan raft.ApplyMsg, 1)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.reqMap = make(map[int]*OpContext)
	sm.seqMap = make(map[int64]int64)
	sm.lastAppliedIndex = 0
	go sm.applyLoop()
	//go sm.snapshotLoop() 本lab似乎不需要拍快照

	return sm
}
