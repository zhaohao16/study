package raftkv

import "fmt"

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID  int64
	Seq int64
}

func (args *PutAppendArgs) String() string {
	return fmt.Sprintf("key:%v, Value:%v, Op:%v, ID:%v, Seq:%v", args.Key, args.Value, args.Op, args.ID, args.Seq)
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
