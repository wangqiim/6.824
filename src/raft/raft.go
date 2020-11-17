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
import "../labrpc"

// import "bytes"
// import "../labgob"

const LEADER = 0
const FOLLOWER = 1
const CANDIDATE = 2
const TIMEINTERVAL = 1	//1ms

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
	log 		[]interface{}
	state		int

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
	DPrintf("[%d] received request vote from %d", rf.me, args.CandidateId)
	if (args.Term < rf.currentTerm) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if (args.Term > rf.currentTerm) {
		rf.ConvToFollower(args.Term, args.CandidateId)
		reply.Term = args.Term
		reply.VoteGranted = true
	} else { //canidate has vote self
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
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
	// Your code here (2B).
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
func Make (	peers 		[]*labrpc.ClientEnd, 
			me 			int,
			persister 	*Persister, 
			applyCh 	chan ApplyMsg) *Raft {
	// Your initialization code here (2A, 2B, 2C).
	//rand.Seed(time.Now().UnixNano())
	duration := rand.Intn(150) + 150	//[150,300)
	DPrintf("[%d] Make and duration is %d MilliSecond", me, duration)
	rf := &Raft{peers:			peers, 
				persister:		persister, 
				me: 			me,
				dead:			0,
				currentTerm:	0,
				votedFor:		-1,
				state:			FOLLOWER,
				electDuration:	duration}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Run()
	return rf
}

func (rf *Raft) ConvToFollower(Term int, votedFor int) {
	rf.state = FOLLOWER
	rf.currentTerm = Term
	rf.votedFor = votedFor
	rf.electTimeTick = 0
}

func (rf *Raft) ConvToCandidate() {
	rf.state = CANDIDATE
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.me
	rf.electTimeTick = 0
}

func (rf *Raft) ConvToLeader() {
	rf.state = LEADER
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
			rf.mu.Unlock()
			rf.Heartbeat()
		} else {
			rf.mu.Unlock()
		}
	} else {	// CANIDATED || FOLLOWER
		rf.electTimeTick += timeInc
		if (rf.electTimeTick == rf.electDuration) {	//到点了，重新选举
			rf.ConvToCandidate()
			rf.mu.Unlock()
			rf.AttemptElection()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) AttemptElection() {
	rf.mu.Lock()
	if (rf.state != CANDIDATE) { //double check
		rf.mu.Unlock()
		return;
	}
	DPrintf("[%d] attempting an electing at term %d", rf.me, rf.currentTerm)
	votes := 1
	done := false
	term := rf.currentTerm	//Eliminate race
	rf.mu.Unlock()
	for server, _ := range rf.peers {
		if (server == rf.me) {	// rf.peers is [0, 2, 3, 4, 5, 6] ??? right????
			continue
		}
		go func(server int) {
			voteGranted := rf.CallRequestVote(term, server)
			if (voteGranted == false) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			votes++
			DPrintf("[%d] got vote from %d", rf.me, server)
			if (done || votes <= len(rf.peers) / 2) {
				return
			}
			done = true
			if (rf.state != CANDIDATE || rf.currentTerm != term) {
				return
			}
			DPrintf("[%d] we got enough votes, we are now the leader (currentTerm = %d, state=%v)!", rf.me, rf.currentTerm, rf.state)
			rf.state = LEADER
		}(server)
	}
}

func (rf *Raft) CallRequestVote(term int, server int) bool {
	DPrintf("[%d] sending request vote to %d", rf.me, server)
	args := RequestVoteArgs{Term: term, CandidateId: rf.me}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)
	DPrintf("[%d] finish sending request vote to %d, ok = %v", rf.me, server, ok)
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

func (rf *Raft) Heartbeat() {
	rf.mu.Lock()
	DPrintf("[%d] Heartbeat at term %d", rf.me, rf.currentTerm)
	rf.heartbeatTick = 0
	rf.mu.Unlock()
	for server, _ := range rf.peers {
		if (server == rf.me) {	// rf.peers is [0, 2, 3, 4, 5, 6] ??? right????
			continue
		}
		go func(server int) {
			rf.CallAppendEntries(server)	//heartbeat dont care reply
		}(server)
	}
}

func (rf *Raft) CallAppendEntries(server int) bool {
	DPrintf("[%d] sending Append Entries to %d", rf.me, server)
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntriese(server, &args, &reply)
	DPrintf("[%d] finish Append Entries to %d, ok = %v", rf.me, server, ok)
	if (ok == false) {
		return false
	}
	//do thing
	rf.mu.Lock()
	if (reply.Term > rf.currentTerm && reply.Success == true) {
		rf.ConvToFollower(reply.Term, server)
	}
	rf.mu.Unlock()
	return reply.Success

}

func (rf *Raft) sendAppendEntriese(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//RPC 
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("[%d] received AppendEntries from %d", rf.me, args.LeaderId)
	defer rf.mu.Unlock()
	if (args.Term < rf.currentTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else if (args.Term == rf.currentTerm) {
		reply.Term = rf.currentTerm
		if (rf.votedFor == args.LeaderId) {
			rf.electTimeTick = 0
			reply.Success = true
		} else {		//应该不会进入这种情况，因为一个term只有一个leader，因此不会收到不是leader的AppendEntries
			reply.Success = false
		}
	} else {	//任期高,直接成为该节点的follower
		rf.ConvToFollower(args.Term, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
	}
}
