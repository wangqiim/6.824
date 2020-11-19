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
// import "fmt"

// import "bytes"
// import "../labgob"

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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] accept reqvote from [%d]", rf.me, args.CandidateId)
	// DPrintf("[%d] received request vote from %d", rf.me, args.CandidateId)
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	
	//要么最新的日志任期比我新，如果任期相同 则日志至少和我一样多，我才给你投票
	index := len(rf.log) - 1
	if (index == -1 || args.LastLogTerm > rf.log[index].Term || 
		((args.LastLogTerm == rf.log[index].Term) && (args.LastLogIndex >= index + 1))) {
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
	return
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
	index = len(rf.log)
	//DPrintf("[%d] start agreement, index=%d, currentTerm = %d", rf.me, index, term)
	// DPrintf("[%d] start agreement, commitIndex=%d, lastApplied=%d",rf.me,rf.commitIndex,rf.lastApplied)
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
func Make(	peers 		[]*labrpc.ClientEnd, 
			me 			int,
			persister 	*Persister, 
			applyCh 	chan ApplyMsg) *Raft {
	// DPrintf("applyCh = %v", <-applyCh)
	// Your initialization code here (2A, 2B, 2C).
	//rand.Seed(time.Now().UnixNano())
	duration := rand.Intn(15) * 10 + 150	//[150,290) 10的倍数
	// DPrintf("[%d] Make and duration is %d MilliSecond", me, duration)
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
				electDuration:	duration}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Run()
	go rf.applyLogLoop(applyCh)
	return rf
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

func (rf *Raft) Run() {
	go func(timeInc int) {
		for {
			rf.TimeTick(timeInc)
			time.Sleep(time.Duration(timeInc) * time.Millisecond)
		}
	}(TIMEINTERVAL)

	for {
		time.Sleep(time.Second)
	}
}

func (rf *Raft) TimeTick(timeInc int) {
	rf.mu.Lock()
	if (rf.state == LEADER) {
		rf.heartbeatTick += timeInc
		if (rf.heartbeatTick == 100) {	//到点了，发心跳包
			rf.heartbeatTick = 0
			term := rf.currentTerm
			rf.mu.Unlock()
			go rf.Heartbeat(term)
		} else {
			rf.mu.Unlock()
		}
	} else {	// CANIDATED || FOLLOWER
		rf.electTimeTick += timeInc
		if (rf.electTimeTick == rf.electDuration) {	//到点了，重新选举
			rf.ConvToCandidate()
			term := rf.currentTerm
			rf.mu.Unlock()
			go rf.AttemptElection(term)
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) AttemptElection(electTerm int) {
	rf.mu.Lock()
	if (rf.state != CANDIDATE || rf.currentTerm != electTerm) { //double check
		rf.mu.Unlock()
		return;
	}
	DPrintf("[%d] attempting an electing at term %d", rf.me, rf.currentTerm)
	votes := 1
	done := false
	term := rf.currentTerm	//Eliminate race
	lastlogindex := len(rf.log)
	lastLogTerm := 0
	if (len(rf.log) > 0) {
		lastLogTerm = rf.log[len(rf.log) - 1].Term
	}
	rf.mu.Unlock()
	for server, _ := range rf.peers {
		if (server == rf.me) {	// rf.peers is [0, 1, 2, 3, 4, 5] ??? right????
			continue
		}
		go func(server int) {
			voteGranted := rf.CallRequestVote(term, server, lastlogindex, lastLogTerm)
			if (voteGranted == false) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			votes++
			// DPrintf("[%d] got vote from %d", rf.me, server)
			if (done || votes <= len(rf.peers) / 2) {
				return
			}
			done = true
			if (rf.state != CANDIDATE || rf.currentTerm != term) {
				return
			}
			DPrintf("[%d] we got enough votes, we are now the leader (currentTerm = %d, state=%v)!", rf.me, rf.currentTerm, rf.state)
			rf.ConvToLeader()
		}(server)
	}
}

func (rf *Raft) CallRequestVote(term, server, lastlogindex, lastLogTerm int) bool {
	// DPrintf("[%d] sending request vote to %d", rf.me, server)
	args := RequestVoteArgs{Term: 			term, 			CandidateId:	rf.me,
							LastLogIndex: 	lastlogindex, 	LastLogTerm:	lastLogTerm}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)
	// DPrintf("[%d] finish sending request vote to %d, ok = %v", rf.me, server, ok)
	if (ok == false) {	//sendRequestVote can't arrive
		return false
	}
	// ... preocess the reply
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (rf.currentTerm < reply.Term) {
		rf.ConvToFollower(reply.Term, -1)
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
	for server, _ := range rf.peers {
		if (server == rf.me) {
			continue;
		}
		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = 0
		args.Entries = make([]LogEntry, 0)
		args.LeaderCommit = rf.commitIndex
		if (args.PrevLogIndex > 0) {	//log数组索引从0开始
			args.PrevLogTerm = rf.log[args.PrevLogIndex - 1].Term
		}
		// fmt.Printf("[%d] state = %d,rf.nextIndex[%d] = %d", rf.me, rf.state, server, rf.nextIndex[server])
		args.Entries = append(args.Entries, rf.log[rf.nextIndex[server] - 1:]...)
		go rf.CallAppendEntries(server, &args) //process reply in proxy function
	}
	rf.mu.Unlock()
}


func (rf *Raft) CallAppendEntries(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	//DPrintf("[%d] sending Append Entries to %d", rf.me, server)
	ok := rf.sendAppendEntriese(server, args, &reply)
	//DPrintf("[%d] finish Append Entries to %d, ok = %v", rf.me, server, ok)
	if (ok == false) {
		return
	}
	//do thing
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (reply.Term > rf.currentTerm) {
		DPrintf("[%d] leader convert to Follower", rf.me)
		rf.ConvToFollower(reply.Term, -1)
	}
	if (reply.Success == true) {
		rf.nextIndex[server] += len(args.Entries)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		//在所有节点中，日志长度在中间位置的那个节点，让所有节点的日志提交到该节点的日志位置
		sortedMatchIndex := make([]int, 0)
		sortedMatchIndex = append(sortedMatchIndex, len(rf.log))
		for server := 0; server < len(rf.peers); server++ {
			if server == rf.me {
				continue
			}
			sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[server])
		}
		sort.Ints(sortedMatchIndex)
		newCommitIndex := sortedMatchIndex[len(rf.peers) / 2]
		rf.commitIndex = newCommitIndex
	} else {
		DPrintf("[%d] receive false from %d,nextIndex = %d ", rf.me, server, rf.nextIndex[server])
		if (rf.nextIndex[server] > 2) {
			rf.nextIndex[server] -= 1
		}
	}
	return
}

func (rf *Raft) sendAppendEntriese(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//RPC 
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("[%d] received AppendEntries from %d", rf.me, args.LeaderId)
	
	reply.Term = rf.currentTerm
	reply.Success = false

	if (args.Term < rf.currentTerm) {
		return
	} else {	//args.Term >= rf.currentTerm,高任期或同任期的leader
		rf.ConvToFollower(args.Term, args.LeaderId)
	}
	if (len(rf.log) < args.PrevLogIndex) {
		return
	}
	
	//任期不匹配，则直接返回
	if (args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex - 1].Term != args.PrevLogTerm) {
		return
	}

	//截断本地日志
	rf.log = rf.log[:args.PrevLogIndex]
	rf.log = append(rf.log, args.Entries...)
	
	if (args.LeaderCommit > rf.commitIndex) {
		rf.commitIndex = args.LeaderCommit
	}
	reply.Success = true
}


func (rf *Raft) applyLogLoop(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied - 1].Command,
				CommandIndex: rf.lastApplied}
			//DPrintf("[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
		}		
		rf.mu.Unlock()
	}
}