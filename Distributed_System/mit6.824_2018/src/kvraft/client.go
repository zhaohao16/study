package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "time"

// import "log"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu         sync.Mutex
	lastleader int
	id         int64
	seq        int64
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
	ck.id = nrand()
	ck.seq = 0
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

func (ck *Clerk) Call(svcMeth string, args interface{}, reply interface{}) bool {
	ch := make(chan bool, 1)
	go func() {
		ok := ck.servers[ck.lastleader].Call(svcMeth, args, reply)
		ch <- ok
	}()
	select {
	case ok := <-ch:
		return ok
	case <-time.After(500 * time.Millisecond):
		return false
	}
}

func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key: key,
	}

	for {
		reply := GetReply{}
		ok := ck.Call("KVServer.Get", &args, &reply)
		// log.Println("[client_Get] ck.lastleader:", ck.lastleader, "args:", args, "reply:", reply, "ok:", ok)
		if !ok || reply.WrongLeader {
			ck.mu.Lock()
			ck.lastleader = (ck.lastleader + 1) % len(ck.servers)
			ck.mu.Unlock()
			continue
		}
		// log.Println("[client_Get] ck.lastleader:", ck.lastleader, "args:", args, "reply:", reply, "ok:", ok)
		if len(reply.Err) != 0 {
			return ""
		}
		return reply.Value
	}
	return ""
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
	ck.mu.Lock()
	ck.seq++
	seq := ck.seq

	ck.mu.Unlock()
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Seq:   seq,
		ID:    ck.id,
	}

	for {
		reply := PutAppendReply{}
		ok := ck.Call("KVServer.PutAppend", &args, &reply)
		// log.Println("[client_PutAppend] ck.lastleader:", ck.lastleader, "args:", args, "reply:", reply, "ok:", ok)
		if !ok || reply.WrongLeader {
			ck.mu.Lock()
			ck.lastleader = (ck.lastleader + 1) % len(ck.servers)
			ck.mu.Unlock()
			continue
		}
		// log.Println("[client_PutAppend] ck.lastleader:", ck.lastleader, "args:", args, "reply:", reply, "ok:", ok)
		if len(reply.Err) != 0 {
			return
		}
		return
	}
	return
}

func (ck *Clerk) Put(key string, value string) {
	// log.Println("[Put] key:", key, "value:", value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	// log.Println("[Append] key:", key, "value:", value)
	ck.PutAppend(key, value, "Append")
}
