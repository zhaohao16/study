package raftkv

import (
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	// "sort"
	"bytes"
	"strconv"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

var (
	frequencyTime = time.Millisecond * 100
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Op    string
	ID    int64
	Seq   int64
	Begin int64
}

func (op *Op) String() string {
	return fmt.Sprintf("key:%v, Value:%v, Op:%v, ID:%v, Seq:%v, Begin:%v, beginTme:%v", op.Key, op.Value, op.Op, op.ID, op.Seq, op.Begin, time.Unix(0, op.Begin))
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	// Your definitions here.
	shutdown  chan struct{}
	data      map[string]string
	notice    map[int]chan Op
	lastSeq   map[int64]int64
	frequency map[int]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// _, isleader := kv.rf.GetState()
	// if isleader {
	// 	defer DPrintf("[Get] kv.me:%v isleader:%v args:%v reply;%v\n", kv.me, isleader, args, reply)
	// }
	// if !isleader {
	// 	reply.WrongLeader = true
	// 	return
	// }
	op := Op{
		Key:   args.Key,
		Value: strconv.FormatInt(nrand(), 10), //防止重复，get请求在raft日志可能被覆盖
		Begin: time.Now().UnixNano(),
	}
	index, _, isleader := kv.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		return
	}
	// DPrintf("[Get] kv.me:%v isleader:%v args:%v index:%v\n", kv.me, isleader, args, index)
	ch := make(chan Op, 1)
	kv.putNotice(index, ch)
	select {
	case cmd := <-ch:
		if isCover(op, cmd) {
			reply.WrongLeader = true
			reply.Err = "data is cover"
			return
		}
	case <-time.After(time.Millisecond * 300):
		reply.WrongLeader = true
		reply.Err = "time out"
		return
	}

	kv.mu.Lock()
	val, ok := kv.data[args.Key]
	kv.mu.Unlock()
	if !ok {
		reply.Err = "not found"
	}
	reply.Value = val
}

func isCover(old Op, new Op) bool {
	if old.Key != new.Key || old.Value != new.Value || old.ID != new.ID || old.Seq != new.Seq || old.Op != new.Op || old.Begin != new.Begin {
		return true
	}
	return false
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// _, isleader := kv.rf.GetState()
	// if isleader {
	// 	defer DPrintf("[PutAppend] kv.me:%v isleader:%v args:%v reply;%v\n", kv.me, isleader, args, reply)
	// }
	// if !isleader {
	// 	reply.WrongLeader = true
	// 	return
	// }

	op := Op{
		Key:   args.Key,
		Value: args.Value,
		Op:    args.Op,
		ID:    args.ID,
		Seq:   args.Seq,
		Begin: time.Now().UnixNano(),
	}
	index, _, isleader := kv.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		return
	}
	// DPrintf("[PutAppend] kv.me:%v isleader:%v args:%v index:%v\n", kv.me, isleader, args, index)
	ch := make(chan Op, 1)
	kv.putNotice(index, ch)
	select {
	case cmd := <-ch:
		if isCover(op, cmd) {
			reply.WrongLeader = true
			reply.Err = "data is cover"
			return
		}
	case <-time.After(time.Millisecond * 300):
		reply.WrongLeader = true
		reply.Err = "time out"
		return
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.shutdown)
}

func (kv *KVServer) showDB() string {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return fmt.Sprintf("kv.me:%v data:%v", kv.me, kv.data)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	//maxraftstate = 100
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	// You may need initialization code here.
	kv.shutdown = make(chan struct{})
	kv.notice = make(map[int]chan Op)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.data = make(map[string]string)
	kv.lastSeq = make(map[int64]int64)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.frequency = make(map[int]int)

	kv.readSnapshot(persister.ReadSnapshot())
	go kv.loop()
	// You may need initialization code here.
	//go kv.frequencyShow()
	return kv
}
func (kv *KVServer) checkDoSnapshot() bool {
	if kv.maxraftstate <= 0 {
		return false
	}
	raftStateSize := kv.persister.RaftStateSize()
	if raftStateSize > kv.maxraftstate {
		return true
	}
	return false
}

func (kv *KVServer) writeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.data)
	e.Encode(kv.lastSeq)
	kv.mu.Unlock()
	go kv.rf.DoSnapshot(index, w.Bytes())
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.data)
	d.Decode(&kv.lastSeq)
}

func (kv *KVServer) putNotice(index int, ch chan Op) {
	kv.mu.Lock()
	oldCh, ok := kv.notice[index]
	if ok { //如果存在旧的删除发送一个错误的
		oldCh <- Op{}
	}

	kv.notice[index] = ch
	kv.mu.Unlock()
}

func (kv *KVServer) loop() {
	for {
		select {
		case m := <-kv.applyCh:
			if m.CommandValid == false {
				kv.mu.Lock()
				kv.readSnapshot(m.Snapshot)
				kv.mu.Unlock()
			} else if v, ok := (m.Command).(Op); ok {
				waste := time.Since(time.Unix(0, v.Begin))
				DPrintf("[loop] kv.me:%v v:%v Waste:%v m.CommandIndex:%v\n", kv.me, v.String(), waste, m.CommandIndex)
				kv.mu.Lock()
				if v.Seq > kv.lastSeq[v.ID] {
					kv.frequency[int(waste/frequencyTime)]++
					switch v.Op {
					case "Put":
						kv.data[v.Key] = v.Value
					case "Append":
						kv.data[v.Key] += v.Value
					}
					kv.lastSeq[v.ID] = v.Seq
				}
				kv.mu.Unlock()

				kv.mu.Lock()
				ch, ok := kv.notice[m.CommandIndex]
				if ok {
					ch <- v
					delete(kv.notice, m.CommandIndex)
				}
				kv.mu.Unlock()

				if kv.checkDoSnapshot() {
					//	DPrintf("[loop] star writeSnapshot m.CommandIndex:%v\n", m.CommandIndex)
					kv.writeSnapshot(m.CommandIndex)
					//	DPrintf("[loop] end writeSnapshot m.CommandIndex:%v\n", m.CommandIndex)
				}
			}
		case <-kv.shutdown:
			if len(kv.applyCh) == 0 {
				return
			}
		}
	}
}

func (kv *KVServer) frequencyShow() {

	for {
		time.Sleep(time.Millisecond * 1000)
		select {
		case <-kv.shutdown:
			return
		default:
		}
		_, isleader := kv.rf.GetState()
		if !isleader {
			continue
		}
		DPrintf("begin------------------------------------------------------------------------------------------------------")
		// var tmp []int
		kv.mu.Lock()
		// for waste := range kv.frequency {
		// 	tmp = append(tmp, waste)
		// }
		// sort.Ints(tmp)
		// for waste := range tmp {
		// 	DPrintf("[frequencyShow] kv.me:%v Waste:%v count:%v\n", kv.me, time.Duration(waste)*frequencyTime, kv.frequency[waste])
		// }

		for waste, count := range kv.frequency {
			DPrintf("[frequencyShow] kv.me:%v Waste:%v count:%v\n", kv.me, time.Duration(waste)*frequencyTime, count)
		}
		kv.mu.Unlock()
		DPrintf("end=========================================================================================================")
	}
}
