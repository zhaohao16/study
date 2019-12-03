package shardmaster

import "raft"
import "labrpc"
import "sync"
import "labgob"
import "time"
import "log"

const Debug = 0

func DPrintln(a ...interface{}) (n int, err error) {
	if Debug > 0 {
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
	Op   string
	Args interface{}
	Seq  int64
	ID   int64
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Op:   "Join",
		Args: *args,
		Seq:  args.Seq,
		ID:   args.ID,
	}
	reply.WrongLeader, _ = sm.sendOp(op)
	DPrintln("[Join] me:", sm.me, "args:", *args, "reply:", reply)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Op:   "Leave",
		Args: *args,
		Seq:  args.Seq,
		ID:   args.ID,
	}
	reply.WrongLeader, _ = sm.sendOp(op)
	DPrintln("[Leave] me:", sm.me, "args:", *args, "reply:", reply)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Op:   "Move",
		Args: *args,
		Seq:  args.Seq,
		ID:   args.ID,
	}
	reply.WrongLeader, _ = sm.sendOp(op)
	DPrintln("[Move] me:", sm.me, "args:", *args, "reply:", reply)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Op:   "Query",
		Args: *args,
		ID:   nrand(),
	}
	reply.WrongLeader, reply.Config = sm.sendOp(op)
	DPrintln("[Query] me:", sm.me, "args:", *args, "reply:", reply)
}

func (sm *ShardMaster) sendOp(op Op) (fail bool, config Config) {
	// Your code here.
	index, _, isleader := sm.rf.Start(op)
	if !isleader {
		return true, config
	}
	ch := make(chan Op)
	sm.putNotice(index, ch)
	select {
	case cmd := <-ch:
		if isCover(op, cmd) {
			return true, config
		}
		if op.Op == "Query" {
			var ok bool
			config, ok = cmd.Args.(Config)
			if !ok {
				return true, config
			}
		}
	case <-time.After(time.Millisecond * 300):
		return true, config
	}

	return false, config
}

func isCover(old Op, new Op) bool {
	if old.Op != new.Op || old.Seq != new.Seq || old.ID != new.ID {
		return true
	}
	return false
}

func (sm *ShardMaster) putNotice(index int, ch chan Op) {
	sm.mu.Lock()
	oldCh, ok := sm.notice[index]
	if ok { //如果存在旧的删除发送一个错误的
		oldCh <- Op{}
	}
	sm.notice[index] = ch
	sm.mu.Unlock()
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
				sm.mu.Lock()
				if v.Op == "Query" {
					sm.handleOp(v, m.CommandIndex)
				} else if v.Seq > sm.lastSeq[v.ID] {
					sm.handleOp(v, m.CommandIndex)
					sm.lastSeq[v.ID] = v.Seq
				}
				sm.mu.Unlock()
				DPrintln("[loop] me:", sm.me, "m.CommandIndex:", m.CommandIndex, "v", v, "sm.configs", sm.configs)
			}

		case <-sm.shutdown:
			return
		}
	}
}

func (sm *ShardMaster) handleOp(op Op, index int) {
	last := len(sm.configs) - 1
	switch op.Op {
	case "Query":
		args, ok := op.Args.(QueryArgs)
		if ok {
			if args.Num == -1 || args.Num > last {
				args.Num = last
			}
			op.Args = sm.configs[args.Num].copy()
			DPrintln("[handleOp] Query, args.Num", args.Num, "op.Args", op.Args)
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
	ch, ok := sm.notice[index]
	if ok {
		ch <- op
		delete(sm.notice, index)
	}
}
