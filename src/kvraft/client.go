package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "time"
//import "sync/atomic"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	//每一个客户端单独串行发请求，并不需要上锁
	clientId 	int64
	seqId		int64
	leaderId	int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	ck.leaderId = 0
	ck.seqId	= 0
	ck.clientId = nrand()
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.ClientId = ck.clientId
	args.SeqId = ck.seqId
	ck.seqId++
	//DPrintf("Client[%d] start Get(), key = %s", ck.clientId, key)
	reply := GetReply{}	
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if (ok) {
			if (reply.Err == OK) {
				break;
			} else if (reply.Err == ErrNoKey) {
				return ""
			}
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(time.Millisecond)
	}
	// You will have to modify this function.
	DPrintf("Client[%d] Get(%s)=%v", ck.clientId, key, reply.Value)
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Key	= key
	args.Value	= value
	args.Op		= op
	args.ClientId = ck.clientId
	args.SeqId = ck.seqId
	ck.seqId++
	//DPrintf("Client[%d] start PutAppend(), key = %s,value = %s, op = %s", ck.clientId, key, value, op)
	reply := PutAppendReply{}
	
	for {
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if (ok && reply.Err == OK) {
			break;
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(time.Millisecond)
	}
	DPrintf("Client[%d] %v(%v, %v)", ck.clientId, op, key, value)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
