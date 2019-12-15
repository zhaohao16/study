package shardmaster

import "raft"
import "labrpc"
import "sync"
import "labgob"
import "time"
import "log"
import "fmt"

const Debug = 100

func DPrintln(level int, a ...interface{}) (n int, err error) {
	if level >= Debug {
		log.Println(a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	shutdown chan struct{}
	configs  []Config // indexed by config num

	notice  map[int]chan Op
	lastSeq map[int64]int64
}

type Op struct {
	// Your data here.
	Op    string
	Args  interface{}
	Seq   int64
	ID    int64
	Begin int64
}

func (op *Op) String() string {
	return fmt.Sprintf("Op:%v, Args:%v, ID:%v, Seq:%v, Begin:%v, BeginTime:%v", op.Op, op.Args, op.ID, op.Seq, op.Begin, time.Unix(0, op.Begin))
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Op:    "Join",
		Args:  *args,
		Seq:   args.Seq,
		ID:    args.ID,
		Begin: time.Now().UnixNano(),
	}
	reply.WrongLeader, _ = sm.sendOp(op)
	DPrintln(1, "[Join] me:", sm.me, "args:", *args, "reply:", reply)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Op:    "Leave",
		Args:  *args,
		Seq:   args.Seq,
		ID:    args.ID,
		Begin: time.Now().UnixNano(),
	}
	reply.WrongLeader, _ = sm.sendOp(op)
	DPrintln(1, "[Leave] me:", sm.me, "args:", *args, "reply:", reply)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Op:    "Move",
		Args:  *args,
		Seq:   args.Seq,
		ID:    args.ID,
		Begin: time.Now().UnixNano(),
	}
	reply.WrongLeader, _ = sm.sendOp(op)
	DPrintln(1, "[Move] me:", sm.me, "args:", *args, "reply:", reply)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Op:    "Query",
		Args:  *args,
		ID:    nrand(),
		Begin: time.Now().UnixNano(),
	}
	reply.WrongLeader, reply.Config = sm.sendOp(op)
	if reply.WrongLeader {
		return
	}
	// sm.mu.Lock()
	// defer sm.mu.Unlock()
	// last := len(sm.configs) - 1
	// if args.Num == -1 || args.Num > last {
	// 	args.Num = last
	// }
	// reply.Config = sm.configs[args.Num].copy()

	DPrintln(1, "[Query] me:", sm.me, "args:", *args, "reply:", reply)
}

func (sm *ShardMaster) sendOp(op Op) (fail bool, conf Config) {
	// Your code here.
	index, _, isleader := sm.rf.Start(op)
	if !isleader {
		return true, conf
	}
	ch := make(chan Op, 1)
	sm.putNotice(index, ch)
	DPrintln(1, "[sendOp] putNotice me:", sm.me, "op", op.String(), "index:", index)
	select {
	case cmd := <-ch:
		DPrintln(1, "[sendOp] me:", sm.me, "isCover(op, cmd)", isCover(op, cmd), "op:", op.String(), "cmd:", cmd.String())
		if isCover(op, cmd) {
			return true, conf
		}
		if op.Op == "Query" {
			var ok bool
			conf, ok = cmd.Args.(Config)
			if !ok {
				return true, conf
			}
		}
	case <-time.After(time.Millisecond * 1000):
		DPrintln(1, "[sendOp] me:", sm.me, "time out")
		return true, conf
	}

	return false, conf
}

func isCover(old Op, new Op) bool {
	if old.Op != new.Op || old.Seq != new.Seq || old.ID != new.ID {
		return true
	}
	return false
}

func (sm *ShardMaster) putNotice(index int, ch chan Op) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	oldCh, ok := sm.notice[index]
	if ok { //如果存在旧的删除发送一个错误的
		DPrintln(1, "[putNotice] Repeated me:", sm.me, "index", index)
		oldCh <- Op{}
	}
	sm.notice[index] = ch
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
	close(sm.shutdown)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.shutdown = make(chan struct{})
	sm.notice = make(map[int]chan Op)
	sm.lastSeq = make(map[int64]int64)
	go sm.loop()
	return sm
}

func (sm *ShardMaster) loop() {
	for {
		select {
		case m := <-sm.applyCh:
			if m.CommandValid == false {
			} else if v, ok := (m.Command).(Op); ok {
				id := nrand()
				DPrintln(2, "[loop] begin me:", sm.me, "m.CommandIndex:", m.CommandIndex, "id", id, "v:", v.String())
				sm.mu.Lock()
				if v.Op == "Query" {
					sm.handleOp(&v, m.CommandIndex)
				} else if v.Seq > sm.lastSeq[v.ID] {
					sm.handleOp(&v, m.CommandIndex)
					sm.lastSeq[v.ID] = v.Seq
				}
				ch, ok := sm.notice[m.CommandIndex]
				if ok {
					DPrintln(2, "[handleOp] send msg begin me:", sm.me, "CommandIndex:", m.CommandIndex, "op:", v.String(), "waste:", time.Since(time.Unix(0, v.Begin)))
					ch <- v
					delete(sm.notice, m.CommandIndex)
					DPrintln(2, "[handleOp] send msg end me:", sm.me, "CommandIndex:", m.CommandIndex, "op:", v.String(), "waste:", time.Since(time.Unix(0, v.Begin)))
				} else {
					DPrintln(2, "[handleOp] not found me:", sm.me, "CommandIndex:", m.CommandIndex, "op:", v.String(), "waste:", time.Since(time.Unix(0, v.Begin)))
				}
				sm.mu.Unlock()
				DPrintln(2, "[loop] end me:", sm.me, "m.CommandIndex:", m.CommandIndex, "id", id, "v:", v.String(), "waste:", time.Since(time.Unix(0, v.Begin)))
			}

		case <-sm.shutdown:
			return
		}
	}
}

func (sm *ShardMaster) handleOp(op *Op, index int) {
	last := len(sm.configs) - 1
	switch op.Op {
	case "Query":
		args, ok := op.Args.(QueryArgs)
		if ok {
			last := len(sm.configs) - 1
			if args.Num == -1 || args.Num > last {
				args.Num = last
			}
			op.Args = sm.configs[args.Num].copy()
		}

	case "Move":
		args, ok := op.Args.(MoveArgs)
		if ok {
			//sm.configs[last].Shards[args.Shard] = args.GID
			newConfig := sm.configs[last].copy()
			newConfig.Shards[args.Shard] = args.GID
			newConfig.rebalance(op.Op, args.GID)
			newConfig.Num++
			sm.configs = append(sm.configs, newConfig)
		}
	case "Leave":
		args, ok := op.Args.(LeaveArgs)
		if ok {
			newConfig := sm.configs[last].copy()
			for _, gid := range args.GIDs {
				delete(newConfig.Groups, gid)
				newConfig.rebalance(op.Op, gid)
			}
			newConfig.Num++
			sm.configs = append(sm.configs, newConfig)
		}
	case "Join":
		args, ok := op.Args.(JoinArgs)
		if ok {
			newConfig := sm.configs[last].copy()
			for k, v := range args.Servers {
				newConfig.Groups[k] = v
				newConfig.rebalance(op.Op, k)
			}
			newConfig.Num++

			sm.configs = append(sm.configs, newConfig)
		}
	}

}
