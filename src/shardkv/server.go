package shardkv


// import "../shardmaster"
import "../labrpc"
import "../raft"
import "sync"
import "../labgob"

import "../shardmaster"
import "log"
import "time"
import "bytes"


var Debug int = 0

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

	Shard	int
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	config 		shardmaster.Config
	masterClerk	*shardmaster.Clerk


	kvStore map[string]string  // kv存储
	reqMap	map[int]*OpContext	//log index -> 请求上下文
	seqMap	map[int64]int64 	//查看每个client的日志提交到哪个位置了。	
	lastAppliedIndex int // 已应用到kvStore的日志index

	toOutShards		map[int]map[int]map[string]string //cfg num -> (shard -> db) 为其他的集群提供数据库
	comeInShards    map[int]int     //shard->config number 通过该map判断是否去pull数据库
	myShards		map[int]bool    //to record which shard i can offer service

}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
		DPrintf("??????????????????????????")
		// Your code here.
		op := &Op{}
		op.Key		= args.Key
		op.Type	= OP_TYPE_GET
		op.SeqId	= args.SeqId
		op.ClientId	= args.ClientId
		op.Shard = args.Shard
		var isLeader bool
		op.Index, op.Term, isLeader = kv.rf.Start(*op)
		DPrintf("[KVserver]Key=%v, value=%s, op=%v", op.Key, op.Value, op)
		if (!isLeader) {
			reply.Err = ErrWrongLeader
			return
		}
		
		kv.mu.Lock()
		if isaccess, ok := kv.myShards[args.Shard]; ok == false || isaccess == false {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			return
		}
		DPrintf("[KVserver]Key=%v, value=%s, op=%v", op.Key, op.Value, op)
		opCtx := &OpContext{
			op: op,
			committed: make(chan struct{})}
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
			DPrintf("[ShardKVServer]Get(%v) timeout 2s", args.Key)
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := &Op{}
	op.Key = args.Key
	op.Value = args.Value
	op.Type = args.Op
	op.ClientId = args.ClientId
	op.SeqId = args.SeqId
	op.Shard = args.Shard

	var isLeader bool
	op.Index, op.Term, isLeader = kv.rf.Start(*op)
	if (isLeader == false) {
		reply.Err = ErrWrongLeader
		return
	}
	
	kv.mu.Lock()
	if isaccess, ok := kv.myShards[args.Shard]; ok == false || isaccess == false {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	//DPrintf("[%d] PutAppend RPC,OpType=%v, Key=%v, Value=%v", kv.me, args.Op, args.Key, args.Value)
	opCtx := &OpContext{
		op: op,
		committed: make(chan struct{}),
	}
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
			DPrintf("[ShardKVServer]%v(%v, %v) timeout 2s", args.Op, args.Key, args.Value)
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

func (kv *ShardKV) ShardMigration(args *MigrateArgs, reply *MigrateReply) {
    reply.Err, reply.Shard, reply.ConfigNum = ErrWrongLeader, args.Shard, args.ConfigNum
    if _, isLeader := kv.rf.GetState(); isLeader == false {
		return
	}
    kv.mu.Lock()
    defer kv.mu.Unlock()
    reply.Err = ErrWrongGroup
    if args.ConfigNum >= kv.config.Num {	//必须要在RPC服务端也poll到了配置并且记录了改变
		return
	}
    reply.Err, reply.ConfigNum, reply.Shard = OK, args.ConfigNum, args.Shard
	reply.KvStore, reply.SeqMap = kv.deepCopyDBAndDedupMap(args.ConfigNum,args.Shard)
	//DPrintf("RPC ShardMigration return -> reply=%v", reply)
}

func (kv *ShardKV) deepCopyDBAndDedupMap(config int,shard int) (map[string]string, map[int64]int64) {
    db2 := make(map[string]string)
	seqMap := make(map[int64]int64)
	//DPrintf("kv.toOutShard[%v][%v] = %v", config, shard, kv.toOutShards[config][shard])
    for k, v := range kv.toOutShards[config][shard] {
        db2[k] = v
    }
    for k, v := range kv.seqMap {
        seqMap[k] = v
    }
    return db2, seqMap
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) updateInAndOutDataShard(config shardmaster.Config) {
	//DPrintf("shardkv.gid[%d][%d] updateInAndOutDataShard config=%v", kv.gid, kv.me, config)
	kv.mu.Lock()
    defer kv.mu.Unlock()
    if config.Num <= kv.config.Num { 
        return
	}
    oldConfig, toOutShard := kv.config, kv.myShards
	kv.myShards, kv.config = make(map[int]bool), config
    for shard, gid := range config.Shards {
        if gid != kv.gid {
			continue
		}
		//特别的，对于第一个配置，不需要去别的集群拉数据
        if isaccess, ok := toOutShard[shard]; (ok && isaccess) || oldConfig.Num == 0 {
            kv.myShards[shard] = true
            delete(toOutShard, shard)
        } else {	//新配置需要去pull的数据
            kv.comeInShards[shard] = oldConfig.Num
        }
    }
    if len(toOutShard) > 0 {
        kv.toOutShards[oldConfig.Num] = make(map[int]map[string]string)
        for shard := range toOutShard {
            outDb := make(map[string]string)
            for k, v := range kv.kvStore {
                if key2shard(k) == shard {
                    outDb[k] = v
                    delete(kv.kvStore, k)
                }
            }
            kv.toOutShards[oldConfig.Num][shard] = outDb
        }
	}
	//DPrintf("shardkv.gid[%d][%d] finish updateInAndOutDataShard kv.toOutSHards=%v", kv.gid, kv.me, kv.toOutShards)
}

func (kv *ShardKV) updateDBWithMigrateData(migrationData MigrateReply) {
	//DPrintf("shardkv.gid[%d][%d] updateDBWithMigrateData reply=%v", kv.gid, kv.me, migrationData)
	kv.mu.Lock()
    defer kv.mu.Unlock()
    if migrationData.ConfigNum != kv.config.Num - 1 {
		return
	}
    delete(kv.comeInShards, migrationData.Shard)
    //this check is necessary?, to avoid use  kv.cfg.Num-1 to update kv.cfg.Num's shard
    if _, ok := kv.myShards[migrationData.Shard]; !ok {
        kv.myShards[migrationData.Shard] = true
        for k, v := range migrationData.KvStore {
            kv.kvStore[k] = v
        }
        for k, v := range migrationData.SeqMap {
            kv.seqMap[k] = Max(v,kv.seqMap[k])
        }
	}
}

func (kv *ShardKV) applyLoop() {
	for {
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

						d.Decode(&kv.config)
						d.Decode(&kv.toOutShards)
						d.Decode(&kv.comeInShards)
						d.Decode(&kv.myShards)
					}
					// 已应用到哪个索引
					kv.lastAppliedIndex = msg.LastIncludedIndex
					//DPrintf("KVServer[%d] installSnapshot, kvStore[%v], seqMap[%v] lastAppliedIndex[%v]", kv.me, len(kv.kvStore), len(kv.seqMap), kv.lastAppliedIndex)
				}()
			} else if config, ok := msg.Command.(shardmaster.Config) ;ok { // 是配置信息
				kv.updateInAndOutDataShard(config)
			} else if reply, ok := msg.Command.(MigrateReply) ; ok {
				kv.updateDBWithMigrateData(reply)
			} else if op, ok := msg.Command.(Op) ;ok {	//如果是普通日志
				//cmd := msg.Command
				index := msg.CommandIndex
				term := msg.CommandTerm

				func() {
					//在添加负载均衡之后要对提交的日志做doublecheck，解决更新配置时的错误
					if isaccess, ok := kv.myShards[op.Shard]; ok == false || isaccess == false {
						return
					}
					kv.mu.Lock()
					defer kv.mu.Unlock()
					// 更新已经应用到的日志
					kv.lastAppliedIndex = index
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

func (kv *ShardKV) snapshotLoop() {
	for {
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

				// 快照lab4B的新东西
				e.Encode(kv.config)
				e.Encode(kv.toOutShards)
				e.Encode(kv.comeInShards)
				e.Encode(kv.myShards)

				snapshot = w.Bytes()
				lastIncludedIndex = kv.lastAppliedIndex
				//DPrintf("KVServer[%d] KVServer dump snapshot, snapshotSize[%d] lastAppliedIndex[%d]", kv.me, len(snapshot), kv.lastAppliedIndex)
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

//轮询master获得新的配置，每次next升高1
func (kv *ShardKV) pollConfigLoop() {
	for {
		func() {
			_, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			if (isLeader == false || len(kv.comeInShards) > 0) {	//不是leader或还有shards对应的db没到其他集群去拉
				kv.mu.Unlock()
				return
			}
			next := kv.config.Num + 1
			kv.mu.Unlock()
			newconfig := kv.masterClerk.Query(next)
			//DPrintf("ShardKV[%v][%v] Query(%v) return newconfig=%v", kv.gid, kv.me, next, newconfig)
			if (next == newconfig.Num) {	//判断是否是新配置
				kv.rf.Start(newconfig)
			}
		}()
		time.Sleep(100 * time.Millisecond)	//文档推荐的睡眠时间
	}
}

func (kv *ShardKV) pullShardLoop() {
	for {
		func() {
			_, isLeader := kv.rf.GetState()
			kv.mu.Lock()
			if (isLeader == false || len(kv.comeInShards) == 0) {	//不是leader或还有shards对应的db没到其他集群去拉
				kv.mu.Unlock()
				return
			}
			var wait sync.WaitGroup
			for shard, Num := range kv.comeInShards {
				wait.Add(1)
				go func(shard int, Num int) {
					cfg := kv.masterClerk.Query(Num)
					defer wait.Done()
					args := MigrateArgs{shard, cfg.Num}
					gid := cfg.Shards[shard]
					for {
						for _, server := range cfg.Groups[gid] {
							srv := kv.make_end(server)
							reply := MigrateReply{}
							if ok := srv.Call("ShardKV.ShardMigration", &args, &reply); ok && reply.Err == OK {
								//DPrintf("call RPC ShardMigration)  get-> reply=%v", reply)
								kv.rf.Start(reply)
								return
							}
						}
					}
				}(shard, Num)
			}
			kv.mu.Unlock()
			wait.Wait()
		}()
		time.Sleep(100 * time.Millisecond)	//文档推荐的睡眠时间
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(MigrateArgs{})
    labgob.Register(MigrateReply{})
    labgob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.config = shardmaster.Config{}
	kv.masterClerk = shardmaster.MakeClerk(masters)	//构造一个master客户端用来poll configuration
	
	kv.kvStore = make(map[string]string)
	kv.reqMap = make(map[int]*OpContext)
	kv.seqMap = make(map[int64]int64)
	kv.lastAppliedIndex = 0

	kv.toOutShards	= make(map[int]map[int]map[string]string)
	kv.comeInShards	= make(map[int]int)
    kv.myShards 	= make(map[int]bool)
	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	//DPrintf("shardKVServer[%d] StartServer starts all Loops, maxraftstate[%d]", kv.me,  kv.maxraftstate)

	go kv.applyLoop()
	go kv.snapshotLoop()
	go kv.pollConfigLoop()
	go kv.pullShardLoop()
	
	return kv
}
