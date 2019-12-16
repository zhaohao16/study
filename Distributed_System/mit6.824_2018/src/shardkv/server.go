package shardkv

import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "labgob"
import "time"
import "log"
import "bytes"
import "fmt"

const Debug = 300

func DPrintln(level int, a ...interface{}) (n int, err error) {
	if level >= Debug {
		log.Println(a...)
	}
	return
}

func DPrintf(level int, format string, v ...interface{}) (n int, err error) {
	if level >= Debug {
		log.Printf(format, v...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key     string
	Value   string
	Op      string
	ID      int64
	Seq     int64
	ShardID []int
	Version int
	Result  string
	Err     Err //string
}

func (op *Op) String() string {
	return fmt.Sprintf("key:%v, Value:%v, Op:%v, ID:%v, Seq:%v, ShardID:%v, Err:%v", op.Key, op.Value, op.Op, op.ID, op.Seq, op.ShardID, op.Err)
}

type Shards struct {
	Data    map[string]string
	Version int
}

type Migration struct {
	Version int
	GID     int
	ShardID []int
	Data    map[string]string
	LastSeq map[int64]int64
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
	persister *raft.Persister
	mck       *shardmaster.Clerk
	shutdown  chan struct{}
	notice    map[int]chan Op
	data      map[string]string
	lastSeq   map[int64]int64
	shareData map[int]Shards //待共享的数据 ShardsID --> ShardsInfo
	syncData  map[int]int    //待接受的数据 ShardsID --> version
	config    shardmaster.Config
	Shards    [shardmaster.NShards]bool //那个分片属于这个组

	configMap   map[int]shardmaster.Config
	configMapmu sync.Mutex
}

func (kv *ShardKV) query(version int) shardmaster.Config {
	kv.configMapmu.Lock()
	defer kv.configMapmu.Unlock()
	if kv.configMap == nil {
		kv.configMap = make(map[int]shardmaster.Config)
	}
	conf, ok := kv.configMap[version]
	if ok {
		return conf
	}
	conf = kv.mck.Query(version)
	kv.configMap[conf.Num] = conf
	return conf
}

func (kv *ShardKV) Data(args *DataArgs, reply *DataReply) {
	DPrintln(2, "[loop] Data begin me:", kv.me, "gid:", kv.gid, "args", *args)
	kv.mu.Lock()
	if args.Version >= kv.config.Num {
		kv.mu.Unlock()
		reply.Err = "error version"
		return
	}
	kv.mu.Unlock()

	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.WrongLeader = true
		return
	}
	// op := Op{
	// 	Op: "Data",
	// 	ID: nrand(), //防止重复，get请求在raft日志可能被覆盖
	// }
	// index, _, isleader := kv.rf.Start(op)
	// if !isleader {
	// 	reply.WrongLeader = true
	// 	return
	// }
	// // DPrintf("[Get] kv.me:%v isleader:%v args:%v index:%v\n", kv.me, isleader, args, index)
	// ch := make(chan Op, 1)
	// kv.putNotice(index, ch)
	// select {
	// case cmd := <-ch:
	// 	if isCover(op, cmd) {
	// 		reply.WrongLeader = true
	// 		reply.Err = "data is cover"
	// 		return
	// 	}
	// case <-time.After(time.Millisecond * 300):
	// 	reply.WrongLeader = true
	// 	reply.Err = "time out"
	// 	return
	// }
	reply.Err = OK
	reply.Data = make(map[string]string)
	reply.LastSeq = make(map[int64]int64)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Version = args.Version
	for _, id := range args.ShardID {
		shardsInfo, ok := kv.shareData[id]
		if ok && shardsInfo.Version == args.Version {
			for k, v := range shardsInfo.Data {
				reply.Data[k] = v
			}
			for k, v := range kv.lastSeq {
				reply.LastSeq[k] = v
			}
		} else {
			reply.Err = "not found"
			DPrintln(2, "[loop] Data error me:", kv.me, "gid:", kv.gid, "args", *args, "reply:", *reply)
			reply.Version = 0
		}
	}
	DPrintln(2, "[loop] Data end me:", kv.me, "gid:", kv.gid, "args", *args, "reply:", *reply)
}

func (kv *ShardKV) CheckClean(args *CheckCleanArgs, reply *CheckCleanReply) {
	_, isleader := kv.rf.GetState()
	if !isleader {
		reply.WrongLeader = true
		return
	}
	reply.Err = OK
	reply.Version = args.Version
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if (args.Version+1 < kv.config.Num) || (args.Version+1 == kv.config.Num && kv.Shards[args.ShardID]) {
		reply.Result = true
	} else {
		reply.Result = false
	}

}

func (kv *ShardKV) checkKey(key string) bool {
	id := key2shard(key)
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	return kv.Shards[id]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// if !kv.checkKey(args.Key) {
	// 	reply.Err = ErrWrongGroup
	// 	return
	// }
	// Your code here.
	op := Op{
		Op:  "Get",
		Key: args.Key,
		ID:  nrand(), //防止重复，get请求在raft日志可能被覆盖
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
		if len(cmd.Err) != 0 {
			reply.Err = cmd.Err
		} else {
			reply.Value = cmd.Result
			reply.Err = OK
		}

	case <-time.After(time.Millisecond * 300):
		reply.WrongLeader = true
		reply.Err = "time out"
		return
	}

	DPrintln(2, "[Get] me:", kv.me, "gid:", kv.gid, "op", op.String(), "args", *args, "reply:", reply)

}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// if !kv.checkKey(args.Key) {
	// 	reply.Err = ErrWrongGroup
	// 	return
	// }
	op := Op{
		Key:   args.Key,
		Value: args.Value,
		Op:    args.Op,
		ID:    args.ID,
		Seq:   args.Seq,
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
		if len(cmd.Err) != 0 {
			reply.Err = cmd.Err
		} else {
			reply.Err = OK
		}
	case <-time.After(time.Millisecond * 300):
		reply.WrongLeader = true
		reply.Err = "time out"
		return
	}
	DPrintln(2, "[PutAppend] me:", kv.me, "gid:", kv.gid, "op", op.String(), "args", *args, "reply:", reply)
}

func isCover(old Op, new Op) bool {
	if old.Key != new.Key || old.Value != new.Value || old.ID != new.ID || old.Seq != new.Seq || old.Op != new.Op {
		return true
	}
	return false
}

func (kv *ShardKV) putNotice(index int, ch chan Op) {
	kv.mu.Lock()
	oldCh, ok := kv.notice[index]
	if ok { //如果存在旧的删除发送一个错误的
		oldCh <- Op{}
	}

	kv.notice[index] = ch
	kv.mu.Unlock()
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
	close(kv.shutdown)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(Migration{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.mck = shardmaster.MakeClerk(masters)
	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.shutdown = make(chan struct{})
	kv.notice = make(map[int]chan Op)
	kv.data = make(map[string]string)
	kv.lastSeq = make(map[int64]int64)
	kv.shareData = make(map[int]Shards)
	kv.syncData = make(map[int]int)
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readSnapshot(persister.ReadSnapshot())
	DPrintln(2, "[shardkv] ---------------------------------------------------------------------------------------------------me:", me, "gid:", gid, "maxraftstate", maxraftstate)
	go kv.loop()
	go kv.updateConfig()
	go kv.startMigration()
	go kv.cleanShareData()
	return kv
}

func (kv *ShardKV) checkDoSnapshot() bool {
	// if kv.maxraftstate <= 0 {
	// 	return false
	// }
	threshold := 10
	return kv.maxraftstate > 0 &&
		kv.maxraftstate-kv.persister.RaftStateSize() < kv.maxraftstate/threshold

	// raftStateSize := kv.persister.RaftStateSize()
	// if raftStateSize > kv.maxraftstate {
	// 	return true
	// }
	// return false
}

func (kv *ShardKV) writeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	id := nrand()
	kv.mu.Lock()
	e.Encode(kv.data)
	e.Encode(kv.lastSeq)
	e.Encode(kv.shareData)
	e.Encode(kv.syncData)
	e.Encode(kv.config)
	e.Encode(kv.Shards)
	e.Encode(kv.me)
	e.Encode(id)
	shareMap := make(map[int]int)
	for k, v := range kv.shareData {
		shareMap[k] = v.Version
	}
	DPrintln(222, "[loop] writeSnapshot begin me:", kv.me, "gid:", kv.gid, "id:", id, "Change:", kv.config, "kv.syncData", kv.syncData, "kv.Shards:", kv.Shards, "shareMap:", shareMap)
	kv.mu.Unlock()

	go kv.rf.DoSnapshot(index, w.Bytes())
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	shareMap := make(map[int]int)
	for k, v := range kv.shareData {
		shareMap[k] = v.Version
	}
	var remote, id int
	DPrintln(222, "[loop] readSnapshot begin me:", kv.me, "gid:", kv.gid, "Change:", kv.config, "kv.syncData", kv.syncData, "kv.Shards:", kv.Shards, "shareMap:", shareMap)
	// Your code here (2C).
	var newShards [shardmaster.NShards]bool
	kv.data = make(map[string]string)
	kv.lastSeq = make(map[int64]int64)
	kv.shareData = make(map[int]Shards)
	kv.syncData = make(map[int]int)
	kv.config = shardmaster.Config{}
	kv.Shards = newShards

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.data)
	d.Decode(&kv.lastSeq)
	d.Decode(&kv.shareData)
	d.Decode(&kv.syncData)
	d.Decode(&kv.config)
	d.Decode(&kv.Shards)
	d.Decode(&remote)
	d.Decode(&id)
	shareMap = make(map[int]int)
	for k, v := range kv.shareData {
		shareMap[k] = v.Version
	}
	DPrintln(222, "[loop] readSnapshot end me:", kv.me, "gid:", kv.gid, "<---- remote", remote, "id", id, "Change:", kv.config, "kv.syncData", kv.syncData, "kv.Shards:", kv.Shards, "shareMap:", shareMap)
}

func (kv *ShardKV) loop() {
	for {
		select {
		case m := <-kv.applyCh:
			id := nrand()
			begin := time.Now()
			DPrintf(23, "[loop] applyCh begin me: %v gid:%v id:%v m.Command.(type):%T\n", kv.me, kv.gid, id, m.Command)
			if m.CommandValid == false {
				kv.mu.Lock()
				kv.readSnapshot(m.Snapshot)
				kv.mu.Unlock()
				DPrintf(23, "[loop] applyCh end me: %v gid:%v id:%v m.Command.(type):%T waste:%v\n", kv.me, kv.gid, id, m.Command, time.Since(begin))
				//continue
			} else if v, ok := m.Command.(Migration); ok {
				kv.mu.Lock()
				// DPrintln(3, "[loop] Migration begin me:", kv.me, "gid:", kv.gid, "kv.syncData", kv.syncData, "Migration", v)
				kv.handleMigration(v)
				// DPrintln(3, "[loop] Migration end me:", kv.me, "gid:", kv.gid, "kv.syncData", kv.syncData, "Migration", v)
				kv.mu.Unlock()
				DPrintf(23, "[loop] applyCh end me: %v gid:%v id:%v m.Command.(type):%T waste:%v\n", kv.me, kv.gid, id, m.Command, time.Since(begin))
				//continue
			} else if v, ok := (m.Command).(shardmaster.Config); ok {
				kv.mu.Lock()
				// DPrintln(3, "[loop] Config begin me:", kv.me, "gid:", kv.gid, "old:", kv.config, "new", v, "syncData:", kv.syncData)
				kv.handleUpdateConfig(v)
				// DPrintln(3, "[loop] Config end me:", kv.me, "gid:", kv.gid, "old:", kv.config, "new", v, "syncData:", kv.syncData)
				kv.mu.Unlock()
				DPrintf(23, "[loop] applyCh end me: %v gid:%v id:%v m.Command.(type):%T waste:%v\n", kv.me, kv.gid, id, m.Command, time.Since(begin))
				//continue
			} else if v, ok := (m.Command).(Op); ok {
				kv.mu.Lock()
				// DPrintln(2, "[loop] Op begin me:", kv.me, "gid:", kv.gid, "op:", v.String(), "data:", kv.data, "config:", kv.config, "kv.checkKey(v.Key):", kv.checkKey(v.Key))
				if v.Op == "CheckClean" {
					//kv.handleClean(&v)
				} else {
					if !kv.checkKey(v.Key) {
						v.Err = ErrWrongGroup
					} else if v.Op == "Get" {
						val, ok := kv.data[v.Key]
						if ok {
							v.Result = val
						} else {
							v.Err = ErrNoKey
						}
					} else if v.Seq > kv.lastSeq[v.ID] {
						switch v.Op {
						case "Put":
							kv.data[v.Key] = v.Value
						case "Append":
							kv.data[v.Key] += v.Value
						}
						kv.lastSeq[v.ID] = v.Seq
					}
				}
				// kv.mu.Unlock()

				// kv.mu.Lock()
				ch, ok := kv.notice[m.CommandIndex]
				if ok {
					ch <- v
					delete(kv.notice, m.CommandIndex)
				}
				// DPrintln(2, "[loop] Op end me:", kv.me, "gid:", kv.gid, "op:", v.String(), "data:", kv.data, "config:", kv.config, "kv.checkKey(v.Key):", kv.checkKey(v.Key))
				kv.mu.Unlock()
			}
			DPrintf(2, "[loop] applyCh end me: %v gid:%v id:%v m.Command.(type):%T waste:%v\n", kv.me, kv.gid, id, m.Command, time.Since(begin))
			if kv.checkDoSnapshot() {
				id := nrand()
				DPrintf(3, "[loop] star writeSnapshot id:%v m.CommandIndex:%v kv.persister.RaftStateSize():%v\n", id, m.CommandIndex, kv.persister.RaftStateSize())
				kv.writeSnapshot(m.CommandIndex)
				DPrintf(3, "[loop] end writeSnapshot id:%v m.CommandIndex:%v kv.persister.RaftStateSize():%v\n", id, m.CommandIndex, kv.persister.RaftStateSize())
			}
		case <-kv.shutdown:
			return
		}
	}
}

func (kv *ShardKV) updateConfig() {
	update := func(queene chan interface{}) {
		defer func() {
			queene <- new(interface{})
		}()
		_, isleader := kv.rf.GetState()
		if !isleader {
			return
		}
		nextVerion := kv.config.Num + 1
		config := kv.query(nextVerion)
		if config.Num != nextVerion { //不是下一个
			return
		}

		kv.mu.Lock()
		DPrintln(200, "[updateConfig] @@@@@@@@@@@@@@@@@@ change config, me:", kv.me, "gid:", kv.gid, "old:", kv.config, "new:", config, "kv.syncData", kv.syncData)
		if len(kv.syncData) != 0 { //还有未同步的数据
			kv.mu.Unlock()
			return
		}
		DPrintln(200, "[updateConfig] =================== change config, me:", kv.me, "gid:", kv.gid, "old:", kv.config, "new:", config)
		kv.mu.Unlock()

		kv.rf.Start(config)
	}
	maxCount := 3
	queene := make(chan interface{}, maxCount)
	for i := 0; i < maxCount; i++ {
		queene <- new(interface{})
	}
	for {

		select {
		case <-kv.shutdown:
			return
		case <-time.After(time.Millisecond * 50):
			<-queene
			go update(queene)
		}
	}
}

func (kv *ShardKV) cleanShareData() {
	gc := func(queene chan interface{}) {
		defer func() {
			queene <- new(interface{})
		}()
		shareData := make(map[int]int) // ShardID-->version
		kv.mu.Lock()
		for shardID, shardsInfo := range kv.shareData {
			shareData[shardID] = shardsInfo.Version
		}
		kv.mu.Unlock()
		if len(shareData) == 0 {
			return
		}
		DPrintln(2, "[cleanShareData] 11111111111111, me:", kv.me, "gid:", kv.gid, "shareData:", shareData)
		handleReply := func(shardID int, version int) {
			kv.mu.Lock()
			defer kv.mu.Unlock()
			shardsInfo, ok := kv.shareData[shardID]
			if ok && shardsInfo.Version == version {
				delete(kv.shareData, shardID)
				DPrintln(2, "[cleanShareData] 2222222222222222, me:", kv.me, "gid:", kv.gid, "shardID:", shardID, "version:", version)
			}
			if len(kv.shareData) == 0 {
				DPrintln(222, "[cleanShareData] 3333333333, me:", kv.me, "gid:", kv.gid, "version:", kv.config.Num)

			}
		}
		var wg sync.WaitGroup
		for shardID, version := range shareData {
			wg.Add(1)
			go func(shardID int, version int) {
				defer wg.Done()
				config := kv.query(version + 1)
				gid := config.Shards[shardID]
				args := &CheckCleanArgs{
					Version: version,
					ShardID: shardID,
				}
				if servers, ok := config.Groups[gid]; ok {
					// try each server for the shard.
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						var reply CheckCleanReply
						ok := srv.Call("ShardKV.CheckClean", args, &reply)
						DPrintln(2, "[cleanShareData] 33333333333333, me:", kv.me, "gid:", kv.gid, "--->", gid, "args:", *args, "reply:", reply)
						if ok && reply.WrongLeader == false && reply.Err == OK && reply.Result {
							// if  {
							handleReply(shardID, version)
							break
							// }
						}

					}
				}
			}(shardID, version)
		}
		wg.Wait()
	}

	maxCount := 3
	queene := make(chan interface{}, maxCount)
	for i := 0; i < maxCount; i++ {
		queene <- new(interface{})
	}
	for {
		select {
		case <-kv.shutdown:
			return
		case <-time.After(time.Millisecond * 100):
			<-queene
			go gc(queene)
		}
	}
}

func (kv *ShardKV) startMigration() {
	migration := func(queene chan interface{}) {
		defer func() {
			queene <- new(interface{})
		}()
		_, isleader := kv.rf.GetState()
		if !isleader {
			return
		}
		kv.mu.Lock()
		if len(kv.syncData) == 0 {
			kv.mu.Unlock()
			return
		}
		version := kv.config.Num - 1
		kv.mu.Unlock()

		config := kv.query(version)
		argsMap := make(map[int]*DataArgs) //gid-->DataArgs

		kv.mu.Lock()
		for ShardsID, version := range kv.syncData {
			if version != kv.config.Num-1 {
				continue
			}
			gid := config.Shards[ShardsID]
			args, ok := argsMap[gid]
			if !ok {
				args = &DataArgs{
					Version: kv.config.Num - 1,
				}
			}
			args.ShardID = append(args.ShardID, ShardsID)
			argsMap[gid] = args
		}
		kv.mu.Unlock()

		DPrintln(222, "[loop] startMigration me:", kv.me, "gid:", kv.gid, "syncData", kv.syncData, "args", argsMap, "config:", config)
		handleReply := func(gid int, reply DataReply) {
			// kv.mu.Lock()
			// if reply.Version != kv.config.Num-1 || len(kv.syncData) == 0 {
			// 	kv.mu.Unlock()
			// 	return
			// }
			// kv.mu.Unlock()

			m := Migration{
				Version: reply.Version,
				GID:     gid,
				ShardID: argsMap[gid].ShardID,
				Data:    make(map[string]string),
				LastSeq: make(map[int64]int64),
			}

			for k, v := range reply.Data {
				m.Data[k] = v
			}
			for k, v := range reply.LastSeq {
				m.LastSeq[k] = v
			}
			// for _, shardID := range argsMap[gid].ShardID {
			// 	delete(kv.syncData, shardID)
			// 	kv.Shards[shardID] = true
			// }
			kv.rf.Start(m)
		}
		var wg sync.WaitGroup
		for gid, args := range argsMap {
			DPrintln(200, "[loop] ********************************startMigration me:", kv.me, "gid:", kv.gid, "version:", kv.config.Num, "gid", gid, "args:", *args)
			wg.Add(1)
			go func(gid int, args *DataArgs) {
				defer wg.Done()
				if servers, ok := config.Groups[gid]; ok {
					// try each server for the shard.
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						var reply DataReply
						ok := srv.Call("ShardKV.Data", args, &reply)
						if ok && reply.WrongLeader == false && reply.Err == OK {
							//return reply, OK
							handleReply(gid, reply)
						}
						if ok && (reply.Err == ErrWrongGroup) {
							//return reply, ErrWrongGroup
						}
					}
				}
			}(gid, args)
		}
		wg.Wait()
	}
	maxCount := 3
	queene := make(chan interface{}, maxCount)
	for i := 0; i < maxCount; i++ {
		queene <- new(interface{})
	}

	for {
		select {
		case <-kv.shutdown:
			return
		case <-time.After(time.Millisecond * 35):
			<-queene
			go migration(queene)

		}
	}
}

// func (kv *ShardKV) handleClean(op *Op) {

// }

func (kv *ShardKV) handleMigration(m Migration) {
	if m.Version != kv.config.Num-1 || len(kv.syncData) == 0 {
		DPrintln(200, "[loop] 000 Migration me:", kv.me, "gid:", kv.gid, "version:", kv.config.Num, "kv.syncData", kv.syncData, "m.Version", m.Version, "m.GID", m.GID, "m.ShardID", m.ShardID)
		return
	}
	// for _, shardID := range m.ShardID {
	// 	if kv.syncData[shardID] != m.Version {
	// 		return
	// 	}
	// }

	for k, v := range m.Data {
		kv.data[k] = v
	}
	for k, v := range m.LastSeq {
		if kv.lastSeq[k] < v {
			kv.lastSeq[k] = v
		}
	}
	for _, shardID := range m.ShardID {
		delete(kv.syncData, shardID)
		kv.Shards[shardID] = true
	}
	DPrintln(200, "[loop] Migration me:", kv.me, "gid:", kv.gid, "version:", kv.config.Num, "kv.syncData", kv.syncData, "m.Version", m.Version, "m.GID", m.GID, "m.ShardID", m.ShardID)
	// if len(kv.syncData) == 0 {
	// 	DPrintln(222, "[loop] Migration me:", kv.me, "gid:", kv.gid, "version:", kv.config.Num, "kv.syncData", kv.syncData, "kv.Shards:", kv.Shards)
	// }
}

func (kv *ShardKV) handleUpdateConfig(config shardmaster.Config) {
	if kv.config.Num != config.Num-1 || len(kv.syncData) != 0 {
		return
	}
	var shareShardID []int
	for i := 0; i < shardmaster.NShards; i++ {
		kv.Shards[i] = false
		if kv.config.Shards[i] == kv.gid {
			if config.Shards[i] == kv.gid {
				kv.Shards[i] = true
			} else { //带共享
				var shareData Shards
				shareData.Data = make(map[string]string)
				shareData.Version = kv.config.Num
				for k, v := range kv.data {
					if key2shard(k) == i {
						shareData.Data[k] = v
						delete(kv.data, k)
						shareShardID = append(shareShardID, i)
					}
				}
				kv.shareData[i] = shareData
				DPrintln(3, "[loop] handleUpdateConfig me:", kv.me, "gid:", kv.gid, "ShardsID:", i, "verion:", shareData.Version, "data:", shareData.Data)
			}
		} else if config.Shards[i] == kv.gid { //待接受
			if config.Num > 1 {
				kv.syncData[i] = config.Num - 1
			} else {
				kv.Shards[i] = true
			}
		}
	}
	shareMap := make(map[int]int)
	for k, v := range kv.shareData {
		shareMap[k] = v.Version
	}
	DPrintln(200, "[loop] UpdateConfig me:", kv.me, "gid:", kv.gid, "confChange:", kv.showChangeConf(config), "kv.syncData", kv.syncData, "shareShardID:", shareShardID, "kv.Shards:", kv.Shards, "shareMap:", shareMap)
	kv.config = config
}

func (kv *ShardKV) showChangeConf(config shardmaster.Config) string {
	var oldShards, newShards []int
	for i := 0; i < shardmaster.NShards; i++ {
		if kv.config.Shards[i] == kv.gid {
			oldShards = append(oldShards, i)
		}
		if config.Shards[i] == kv.gid {
			newShards = append(newShards, i)
		}
	}
	return fmt.Sprintf("oldVersion:%v, newVersion:%v, oldShards:%v, newShards:%v", kv.config.Num, config.Num, oldShards, newShards)
}
