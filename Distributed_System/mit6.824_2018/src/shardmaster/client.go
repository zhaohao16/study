package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"
import "sync"
import "fmt"
import _ "net/http/pprof"
import "net/http"
import "log"

func init1() {
	go func() {
		for {
			port := nrand()%10000 + 10000
			log.Println(http.ListenAndServe(fmt.Sprintf(":%v", port), nil))
		}
	}()
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id         int64
	seq        int64
	mu         sync.Mutex
	lastLeader int
}

const RetryInterval = time.Duration(10 * time.Millisecond)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.id = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	for {
		// try each known server.
		var reply QueryReply
		DPrintln(2, "[client_Query] begin ck.lastLeader:", ck.lastLeader, "args:", *args)
		ok := ck.servers[ck.lastLeader].Call("ShardMaster.Query", args, &reply)
		DPrintln(2, "[client_Query] end ck.lastLeader:", ck.lastLeader, "args:", *args, "reply:", reply)
		if ok && reply.WrongLeader == false {
			return reply.Config
		}
		ck.mu.Lock()
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		ck.mu.Unlock()
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	ck.mu.Lock()
	ck.seq++
	seq := ck.seq
	ck.mu.Unlock()

	args.Seq = seq
	args.Servers = servers
	args.ID = ck.id
	for {

		var reply JoinReply
		DPrintln(2, "[client_Join] begin ck.lastLeader:", ck.lastLeader, "args:", *args)
		ok := ck.servers[ck.lastLeader].Call("ShardMaster.Join", args, &reply)
		DPrintln(2, "[client_Join] end ck.lastLeader:", ck.lastLeader, "args:", *args, "reply:", reply)
		if ok && reply.WrongLeader == false {
			return
		}

		ck.mu.Lock()
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		ck.mu.Unlock()
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	ck.mu.Lock()
	ck.seq++
	seq := ck.seq
	ck.mu.Unlock()
	args.GIDs = gids
	args.ID = ck.id
	args.Seq = seq
	for {

		var reply LeaveReply
		DPrintln(2, "[client_Leave] begin ck.lastLeader:", ck.lastLeader, "args:", *args)
		ok := ck.servers[ck.lastLeader].Call("ShardMaster.Leave", args, &reply)
		DPrintln(2, "[client_Leave] end ck.lastLeader:", ck.lastLeader, "args:", *args, "reply:", reply)
		if ok && reply.WrongLeader == false {
			return
		}
		ck.mu.Lock()
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		ck.mu.Unlock()
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	ck.mu.Lock()
	ck.seq++
	seq := ck.seq
	ck.mu.Unlock()
	args.ID = ck.id
	args.Seq = seq

	args.Shard = shard
	args.GID = gid

	for {

		var reply MoveReply
		DPrintln(2, "[client_Move] begin ck.lastLeader:", ck.lastLeader, "args:", *args)
		ok := ck.servers[ck.lastLeader].Call("ShardMaster.Move", args, &reply)
		DPrintln(2, "[client_Move] end ck.lastLeader:", ck.lastLeader, "args:", *args, "reply:", reply)
		if ok && reply.WrongLeader == false {
			return
		}
		ck.mu.Lock()
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		ck.mu.Unlock()
		time.Sleep(RetryInterval)
	}
}
