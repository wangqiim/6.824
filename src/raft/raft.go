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

import "sync"
import "sync/atomic"
import "time"
import "math/rand"
import "sort"
import "../labrpc"

import "bytes"
import "../labgob"

const LEADER = 0
const FOLLOWER = 1
const CANDIDATE = 2
const TIMEINTERVAL = 10	//10ms

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// 向application层安装快照
	Snapshot []byte
	LastIncludedIndex int
	LastIncludedTerm int
}


type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm	int
	votedFor 	int
	state		int
	log 		[]LogEntry

	commitIndex	int	//index of highest log entry known to "be committed"    initialized to 0
	lastApplied	int	//index of highest log entry applied to "state machine"   initialized to 0
	
	//仅leader，易失状态，成为leader时重置
	nextIndex	[]int	//initialized to leader last log index + 1
	matchIndex	[]int //	(initialized to leader

	electTimeTick		int	//0 -> electDuration
	heartbeatTick		int	//0 -> hearbeatDuration
	electDuration		int	//ms
	lastActiveTime    time.Time // 上次活跃时间（刷新时机：收到leader心跳、给其他candidates投票、请求其他节点投票）
	lastBroadcastTime time.Time // 作为leader，上次的广播时间

	lastIncludedIndex	int 	// snapshot最后1个logEntry的index，没有snapshot则为0
	lastIncludedTerm 	int 	// snapthost最后1个logEntry的term，没有snaphost则无意义
	applyCh 			chan ApplyMsg	// 应用层的提交队列
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int = rf.currentTerm
	var isleader bool = (rf.state == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// DPrintf("[%d] persist starts", rf.me)
	data := rf.raftStateForPersist()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) raftStateForPersist() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	// 为了snaphost多做了2个持久化的metadata
	d.Decode(&rf.lastIncludedIndex)
	d.Decode(&rf.lastIncludedTerm)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int		//candiate's term
	CandidateId 	int		//candiate requesting vote
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int		//currentTerm, for candiate to update itself
	VoteGranted	bool	//	true means candidate received vote
}

type AppendEntriesArgs struct {
	Term 			int
	LeaderId		int	//so follower can redirect clients

	PrevLogIndex	int 
	PrevLogTerm		int
	Entries			[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictIndex int
	ConflictTerm int
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Offset int
	Data []byte
	Done bool
}

type InstallSnapshotReply struct {
	Term int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("[%d] received request vote from %d", rf.me, args.CandidateId)
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

	//要么最新的日志任期比我新，如果任期相同 则日志至少和我一样多，我才给你投票
	if (rf.lastIndex() == 0 || args.LastLogTerm > rf.lastTerm() || 
		((args.LastLogTerm == rf.lastTerm()) && (args.LastLogIndex >= rf.lastIndex()))) {
			if (args.Term < rf.currentTerm) {
				return
			} else if (args.Term == rf.currentTerm){	//args.Term > rf.currentTerm
				if (rf.votedFor == -1) {
					rf.ConvToFollower(args.Term, args.CandidateId)
					reply.VoteGranted = true
				}
			} else {	//args.Term > rf.currentTerm
				rf.ConvToFollower(args.Term, args.CandidateId)
				reply.Term = args.Term
				reply.VoteGranted = true
			}
	} else {	//不接受你的日志
		if (args.Term <= rf.currentTerm) {
			return
		} else {	//args.Term > rf.currentTerm
			rf.ConvToFollower(args.Term, -1)
			reply.Term = args.Term
		}
	}
	if (reply.VoteGranted == true) {
		//DPrintf("[%d] vote to %d", rf.me, args.CandidateId)
	}
	rf.persist()
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("[%d] received AppendEntries from %d", rf.me, args.LeaderId)
	
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	if (args.Term < rf.currentTerm) {
		return
	} else {	//args.Term >= rf.currentTerm,高任期或同任期的leader
		rf.ConvToFollower(args.Term, args.LeaderId)
		rf.persist()
	}

	// 如果prevLogIndex在快照内，且不是快照最后一个log，那么只能从index=1开始同步了
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = 1
		return
	} else if args.PrevLogIndex == rf.lastIncludedIndex {	// prevLogIndex正好等于快照的最后一个log
		if args.PrevLogTerm != rf.lastIncludedTerm {	// 冲突了，那么从index=1开始同步吧
			reply.ConflictIndex = 1
			return
		}
		// 否则继续走后续的日志覆盖逻辑
	} else {	// prevLogIndex在快照之后，那么进一步判定
		if args.PrevLogIndex > rf.lastIndex() {	// prevLogIndex位置没有日志的case
			reply.ConflictIndex = rf.lastIndex() + 1
			return
		}
		// prevLogIndex位置有日志，那么判断term必须相同，否则false
		if rf.log[rf.index2LogPos(args.PrevLogIndex)].Term != args.PrevLogTerm {
			reply.ConflictTerm = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
			for index := rf.lastIncludedIndex + 1; index <= args.PrevLogIndex; index++ { // 找到冲突term的首次出现位置，最差就是PrevLogIndex
				if rf.log[rf.index2LogPos(index)].Term == reply.ConflictTerm {
					reply.ConflictIndex = index
					break
				}
			}
			return
		}
		// 否则继续走后续的日志覆盖逻辑
	}

	// 保存日志
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		logPos := rf.index2LogPos(index)
		if index > rf.lastIndex() {	// 超出现有日志长度，继续追加
			rf.log = append(rf.log, logEntry)
		} else { // 重叠部分
			if rf.log[logPos].Term != logEntry.Term {
				rf.log = rf.log[:logPos]         // 删除当前以及后续所有log
				rf.log = append(rf.log, logEntry) // 把新log加入进来
			} // term一样啥也不用做，继续向后比对Log
		}
	}
	rf.persist()

	// 更新提交下标
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.lastIndex() < rf.commitIndex {
			rf.commitIndex = rf.lastIndex()
		}
	}
	reply.Success = true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("RaftNode[%d] installSnapshot starts, rf.lastIncludedIndex[%d] rf.lastIncludedTerm[%d] args.lastIncludedIndex[%d] args.lastIncludedTerm[%d] logSize[%d]", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LastIncludedIndex, args.LastIncludedTerm, len(rf.log))

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > rf.currentTerm {
		rf.ConvToFollower(args.Term, args.LeaderId)
		rf.persist()
		// 继续向下走
	}

	// leader快照不如本地长，那么忽略这个快照
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	} else  {	// leader快照比本地快照长
		if args.LastIncludedIndex < rf.lastIndex() {	// 快照外还有日志，判断是否需要截断
			if rf.log[rf.index2LogPos(args.LastIncludedIndex)].Term != args.LastIncludedTerm {
				rf.log = make([]LogEntry, 0)	// term冲突，扔掉快照外的所有日志
			} else {	// term没冲突，保留后续日志
				leftLog := make([]LogEntry, rf.lastIndex() - args.LastIncludedIndex)
				copy(leftLog, rf.log[rf.index2LogPos(args.LastIncludedIndex)+1:])
				rf.log = leftLog
			}
		} else {
			rf.log = make([]LogEntry, 0)	// 快照比本地日志长，日志就清空了
		}
	}
	// 更新快照位置
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	// 持久化raft state和snapshot
	rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), args.Data)
	// snapshot提交给应用层
	rf.installSnapshotToApplication()
	//DPrintf("RaftNode[%d] installSnapshot ends, rf.lastIncludedIndex[%d] rf.lastIncludedTerm[%d] args.lastIncludedIndex[%d] args.lastIncludedTerm[%d] logSize[%d]", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LastIncludedIndex, args.LastIncludedTerm, len(rf.log))
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntriese(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server !!isn't the leader, returns false!!. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	// Your code here (2B)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (rf.state != LEADER) {
		return -1, -1 ,false
	}
	term = rf.currentTerm
	rf.log = append(rf.log, LogEntry{command, term})
	index = rf.lastIndex()
	//DPrintf("[%d] start agreement, index=%d, currentTerm = %d", rf.me, index, term)
	// DPrintf("[%d] start agreement, commitIndex=%d, lastApplied=%d",rf.me,rf.commitIndex,rf.lastApplied)
	rf.persist()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//如果投票给了别人，则压制一次选举心跳
func (rf *Raft) ConvToFollower(Term int, votedFor int) {
	rf.state = FOLLOWER
	rf.currentTerm = Term
	rf.votedFor = votedFor
	if (votedFor != -1) {
		rf.electTimeTick = 0
	}
}

func (rf *Raft) ConvToCandidate() {
	rf.state = CANDIDATE
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.electTimeTick = 0
}

func (rf *Raft) ConvToLeader() {
	rf.state = LEADER
	// fmt.Printf("[%d] len(rf.nextIndex) = %d\n", rf.me, len(rf.nextIndex))
	// fmt.Printf("[%d] len(rf.log) = %d\n", rf.me, len(rf.log))
	for i, _ := range(rf.nextIndex) {
		rf.nextIndex[i] = len(rf.log) + 1
	}
	for i, _ := range(rf.matchIndex) {
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) AttemptElection(electTerm int) {
	rf.mu.Lock()
	if (rf.state != CANDIDATE || rf.currentTerm != electTerm) { //double check
		rf.mu.Unlock()
		return;
	}
	//DPrintf("[%d] attempting an electing at term %d", rf.me, rf.currentTerm)
	voteResultChan := make(chan int, len(rf.peers))
	finishCount := 1	//已经完成的投票
	votes := 1
	term := rf.currentTerm	//Eliminate race
	lastlogindex := rf.lastIndex()
	lastLogTerm := rf.lastTerm()
	rf.mu.Unlock()
	
	for server, _ := range rf.peers {
		if (server == rf.me) {	// rf.peers is [0, 1, 2, 3, 4, 5] ??? right????
			continue
		}
		go func(server int) {
			voteGranted := rf.CallRequestVote(term, server, lastlogindex, lastLogTerm)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			//DPrintf("[%d] got vote from %d", rf.me, server)
			if (voteGranted == true) {
				votes++
			}
			finishCount++	//已经完成的投票
			voteResultChan<-1
		}(server)
	}
	for {
		select {
		case <-voteResultChan:
			rf.mu.Lock()
			if (rf.state != CANDIDATE || rf.currentTerm != term || finishCount == len(rf.peers)) {
				rf.mu.Unlock()
				return
			}
			if (votes > len(rf.peers) / 2) {
				rf.ConvToLeader()
				rf.persist()
				DPrintf("[%d] we got enough votes, we are now the leader (currentTerm = %d, state=%v)!", rf.me, rf.currentTerm, rf.state)
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}
}


func (rf *Raft) CallRequestVote(term, server, lastlogindex, lastLogTerm int) bool {
	// DPrintf("[%d] sending request vote to %d", rf.me, server)
	args := RequestVoteArgs{Term: 			term, 			CandidateId:	rf.me,
							LastLogIndex: 	lastlogindex, 	LastLogTerm:	lastLogTerm}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)
	//DPrintf("[%d] finish sending request vote to %d, ok = %v", rf.me, server, ok)
	if (ok == false) {	//sendRequestVote can't arrive
		return false
	}
	// ... preocess the reply
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (rf.currentTerm < reply.Term) {
		rf.ConvToFollower(reply.Term, -1)
		rf.persist()
		// init timer
	}
	return reply.VoteGranted;
}

//考虑通过在心跳包中发送附加AppendEntries数据
func (rf *Raft) Heartbeat(heartbeatTerm int) {
	rf.mu.Lock()
	if (heartbeatTerm != rf.currentTerm) {
		rf.mu.Unlock()
		return
	}
	//DPrintf("[%d] Heartbeat at term %d", rf.me, rf.currentTerm)
	//在所有节点中，日志长度在中间位置的那个节点，让所有节点的日志提交到该节点的日志位置
	for server, _ := range rf.peers {
		if (server == rf.me) {
			continue;
		}
		if rf.nextIndex[server] <= rf.lastIncludedIndex {	//安装快照
			rf.doInstallSnapshot(server)
			continue
		}
		//正常复制日志
		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = 0
		args.Entries = make([]LogEntry, 0)
		args.LeaderCommit = rf.commitIndex
		if args.PrevLogIndex == rf.lastIncludedIndex {
			args.PrevLogTerm = rf.lastIncludedTerm
		} else { // 否则一定是log部分
			args.PrevLogTerm = rf.log[rf.index2LogPos(args.PrevLogIndex)].Term
		}
		// fmt.Printf("[%d] state = %d,rf.nextIndex[%d] = %d", rf.me, rf.state, server, rf.nextIndex[server])
		args.Entries = append(args.Entries, rf.log[rf.index2LogPos(args.PrevLogIndex+1):]...)
		go rf.CallAppendEntries(server, &args) //process reply in proxy function
	}
	rf.mu.Unlock()
}

func (rf *Raft) updateCommitIndex() {
	// 更新commitIndex, 就是找中位数
	sortedMatchIndex := make([]int, 0)
	sortedMatchIndex = append(sortedMatchIndex, rf.lastIndex())
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
	}
	sort.Ints(sortedMatchIndex)
	newCommitIndex := sortedMatchIndex[len(rf.peers)/2]
	// 如果index属于snapshot范围，那么不要检查term了，因为snapshot的一定是集群提交的
	// 否则还是检查log的term是否满足条件
	if newCommitIndex > rf.commitIndex && (newCommitIndex <= rf.lastIncludedIndex || rf.log[rf.index2LogPos(newCommitIndex)].Term == rf.currentTerm) {
		rf.commitIndex = newCommitIndex
	}
	//DPrintf("RaftNode[%d] updateCommitIndex, commitIndex[%d] matchIndex[%v]", rf.me, rf.commitIndex, sortedMatchIndex)
}

func (rf *Raft) doInstallSnapshot(peerId int) {
	//DPrintf("RaftNode[%d] doInstallSnapshot starts, leaderId[%d] peerId[%d]\n", rf.me, rf.votedFor, peerId)
	args := InstallSnapshotArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.lastIncludedIndex
	args.LastIncludedTerm = rf.lastIncludedTerm
	args.Offset = 0
	args.Data = rf.persister.ReadSnapshot()
	args.Done = true

	reply := InstallSnapshotReply{}
	go func() {
		if rf.sendInstallSnapshot(peerId, &args, &reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// double check,如果不是rpc前的leader状态了，那么啥也别做了
			if rf.currentTerm != args.Term {
				return
			}
			if reply.Term > rf.currentTerm { // 变成follower
				rf.ConvToFollower(reply.Term, -1)
				rf.persist()
				return
			}
			rf.nextIndex[peerId] = rf.lastIndex() + 1	// 重新从末尾同步log（未经优化，但够用）
			rf.matchIndex[peerId] = args.LastIncludedIndex	// 已同步到的位置（未经优化，但够用）
			rf.updateCommitIndex()	// 更新commitIndex
			//DPrintf("RaftNode[%d] doInstallSnapshot ends, leaderId[%d] peerId[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]\n", rf.me, rf.votedFor, peerId, rf.nextIndex[peerId], rf.matchIndex[peerId], rf.commitIndex)
		}
	}()
}

func (rf *Raft) CallAppendEntries(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	//DPrintf("[%d] sending Append Entries to %d", rf.me, server)
	ok := rf.sendAppendEntriese(server, args, &reply)
	//DPrintf("[%d] finish Append Entries to %d, ok = %v", rf.me, server, ok)
	if (ok == false) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (rf.currentTerm != args.Term) {
		return
	}
	if (reply.Term > rf.currentTerm) {
		//DPrintf("[%d] leader convert to Follower", rf.me)
		rf.ConvToFollower(reply.Term, -1)
		rf.persist()
		return
	}
	if (reply.Success == true) {
		//保证多个包重复时的安全性
		if (args.PrevLogIndex + len(args.Entries) + 1 > rf.nextIndex[server]) {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		}
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		rf.updateCommitIndex()
		//在所有节点中，日志长度在中间位置的那个节点，让所有节点的日志提交到该节点的日志位置
	} else {
		//或许要处理一下丢包时的bug，防止在网络中传输过多东西
		// 回退优化，参考：https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
		if (reply.ConflictTerm == -1) {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			conflictTermIndex := -1
			for index := reply.ConflictIndex; index > rf.lastIncludedIndex; index-- {
				if (rf.log[rf.index2LogPos(index)].Term == reply.ConflictTerm) {
					conflictTermIndex = index
					break;
				}
			}
			if (conflictTermIndex != -1) {
				rf.nextIndex[server] = conflictTermIndex
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		}
		//DPrintf("[%d] receive false from %d,nextIndex = %d ", rf.me, server, rf.nextIndex[server])
	}
	rf.persist()
	return
}

func (rf *Raft) appendEntriesLoop() {
	for {
		time.Sleep(TIMEINTERVAL * time.Millisecond)
		rf.mu.Lock()
		if (rf.state == LEADER) {
			rf.heartbeatTick += TIMEINTERVAL
			if (rf.heartbeatTick == 100) {	//到点了，发心跳包
				rf.heartbeatTick = 0
				term := rf.currentTerm
				go rf.Heartbeat(term)
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) electionLoop() {
	for {
		time.Sleep(TIMEINTERVAL * time.Millisecond)
		rf.mu.Lock()
		if (rf.state != LEADER) {
			rf.electTimeTick += TIMEINTERVAL
			if (rf.electTimeTick >= rf.electDuration) {	//到点了，重新选举
				rf.electDuration = rand.Intn(15) * 10 + 150
				rf.ConvToCandidate()
				rf.persist()
				term := rf.currentTerm
				rf.mu.Unlock()
				rf.AttemptElection(term)
			} else {
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) applyLogLoop(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			appliedIndex := rf.index2LogPos(rf.lastApplied)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[appliedIndex].Command,
				CommandIndex: rf.lastApplied,
				CommandTerm:  rf.log[appliedIndex].Term,
			}
				
			applyCh <- msg
			//DPrintf("[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d], Len(rf.log)[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex, len(rf.log))
		}		
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	duration := rand.Intn(15) * 10 + 150	//[150,290] 10的倍数
	// duration := rand.Intn(150) + 150	//[150,300) 
//ntf("[%d] Make and duration is %d Millisecond", me, duration)
	rf := &Raft{peers:			peers, 
			persister:		persister, 
			me: 			me,
			dead:			0,
			currentTerm:	0,
			votedFor:		-1,
			state:			FOLLOWER,
			log:			make([]LogEntry, 0),
			commitIndex:	0,
			lastApplied:	0,
			nextIndex:		make([]int, len(peers)),
			matchIndex:		make([]int, len(peers)),
			electTimeTick:	0,
			heartbeatTick: 	0,
			electDuration:	duration,
			lastIncludedIndex:	0,
			lastIncludedTerm:	0,
			applyCh:			applyCh}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	// 向application层安装快照
	rf.installSnapshotToApplication()
	rf.persist()
	go rf.applyLogLoop(applyCh)
	go rf.electionLoop()
	go rf.appendEntriesLoop()
	return rf
}


func (rf *Raft) installSnapshotToApplication() {
	var applyMsg *ApplyMsg

	// 同步给application层的快照
	applyMsg = &ApplyMsg{
		CommandValid: false,
		Snapshot: rf.persister.ReadSnapshot(),
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm: rf.lastIncludedTerm,
	}
	// 快照部分就已经提交给application了，所以后续applyLoop提交日志后移
	rf.lastApplied = rf.lastIncludedIndex

	//DPrintf("RaftNode[%d] installSnapshotToApplication, snapshotSize[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]", rf.me,  len(applyMsg.Snapshot), applyMsg.LastIncludedIndex, applyMsg.LastIncludedTerm)
	rf.applyCh <- *applyMsg
	return
}

// 最后的index
func (rf *Raft) lastIndex() int {
	return rf.lastIncludedIndex + len(rf.log)
}

// 最后的term
func (rf *Raft) lastTerm() (lastLogTerm int) {
	lastLogTerm = rf.lastIncludedTerm	// for snapshot
	if len(rf.log) != 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	return
}

// 日志index转化成log数组下标
func (rf *Raft) index2LogPos(index int) (pos int) {
	return index - rf.lastIncludedIndex - 1
}

// 日志是否需要压缩
func (rf *Raft) ExceedLogSize(logSize int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	if rf.persister.RaftStateSize() >= logSize {
		return true
	}
	return false
}

// 保存snapshot，截断log
func (rf *Raft) TakeSnapshot(snapshot []byte, lastIncludedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 已经有更大index的snapshot了
	if lastIncludedIndex <= rf.lastIncludedIndex {
		return
	}

	// 快照的当前元信息
	DPrintf("RafeNode[%d] TakeSnapshot begins, IsLeader=%v snapshotLastIndex[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, rf.votedFor==rf.me, lastIncludedIndex, rf.lastIncludedIndex, rf.lastIncludedTerm)

	// 要压缩的日志长度
	compactLogLen := lastIncludedIndex - rf.lastIncludedIndex

	// 更新快照元信息
	rf.lastIncludedTerm = rf.log[rf.index2LogPos(lastIncludedIndex)].Term
	rf.lastIncludedIndex = lastIncludedIndex

	// 压缩日志
	afterLog := make([]LogEntry, len(rf.log) - compactLogLen)
	copy(afterLog, rf.log[compactLogLen:])
	rf.log = afterLog

	// 把snapshot和raftstate持久化
	rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), snapshot)

	DPrintf("RafeNode[%d] TakeSnapshot ends, IsLeader=%v] snapshotLastIndex[%d] lastIncludedIndex[%d] lastIncludedTerm[%d]",
		rf.me, rf.votedFor==rf.me, lastIncludedIndex, rf.lastIncludedIndex, rf.lastIncludedTerm)
}
