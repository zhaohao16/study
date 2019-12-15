package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID  int64
	Seq int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type DataArgs struct {
	Version int
	ShardID []int
	// You'll have to add definitions here.
}

type DataReply struct {
	WrongLeader bool
	Err         Err
	Version     int
	Data        map[string]string
	LastSeq     map[int64]int64
}

type CheckCleanArgs struct {
	Version int
	ShardID int
}

type CheckCleanReply struct {
	WrongLeader bool
	Result      bool
	Err         Err
	Version     int
}
