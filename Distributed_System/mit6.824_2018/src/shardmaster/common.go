package shardmaster

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid		lab4将数据分成固定10个分片（10分片平均分到Groups个组中）
	Groups map[int][]string // gid -> servers[] 一个组多个副本
}

func (c *Config) copy() Config {
	config := Config{
		Num:    c.Num,
		Shards: c.Shards,
		Groups: make(map[int][]string),
	}
	for k, v := range c.Groups {
		config.Groups[k] = v
	}
	return config
}

func (c *Config) countShards() map[int][]int {
	shardsCount := map[int][]int{}
	for k := range c.Groups {
		shardsCount[k] = []int{}
	}
	for k, v := range c.Shards {
		if v != 0 {
			shardsCount[v] = append(shardsCount[v], k)
		}
	}
	return shardsCount
}

func (c *Config) getMaxIndex(shardsCount map[int][]int) int {
	minCount := 0
	minGid := 0
	for gid, shards := range shardsCount {
		if minCount < len(shards) {
			minCount = len(shards)
			minGid = gid
		}
	}
	return shardsCount[minGid][0]
}

func (c *Config) getMinGid(shardsCount map[int][]int) int {
	maxCount := 20
	maxGid := -1
	for gid, shards := range shardsCount {
		if maxCount > len(shards) {
			maxCount = len(shards)
			maxGid = gid
		}
	}
	return maxGid
}

func (c *Config) rebalance(op string, gid int) {
	num := len(c.Groups)
	if num == 0 {
		for i := 0; i < NShards; i++ {
			c.Shards[i] = 0
		}
		return
	}
	switch op {
	case "Move":

	case "Leave":
		var list []int
		for i, g := range c.Shards {
			if gid == g {
				c.Shards[i] = 0
				list = append(list, i)
			}
		}
		// fmt.Println("[rebalance] list", list)
		for i := 0; i < len(list); i++ {
			c.Shards[list[i]] = c.getMinGid(c.countShards())
			// DPrintln("[rebalance] 1111 i", i, "list[i]", list[i], c.getMinGid(c.countShards()), "c.countShards()", c.countShards(), "Config", c)
		}
	case "Join":
		if num == 1 {
			for i := 0; i < NShards; i++ {
				c.Shards[i] = gid
			}
			return
		}
		oneShardNum := NShards / num
		for i := 0; i < oneShardNum; i++ {
			c.Shards[c.getMaxIndex(c.countShards())] = gid
		}

	}
	DPrintln(1, "[rebalance] Config", c)
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings

	ID  int64
	Seq int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int

	ID  int64
	Seq int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int

	ID  int64
	Seq int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
