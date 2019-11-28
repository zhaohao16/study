package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//用户状态机执行的指令，和收到时的任期号
type logInfo struct {
	Command interface{} //指令
	Term    int         //任期
	Index   int
}

const (
	Follower  int32 = 0
	Candidate int32 = 1
	Leader    int32 = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state int32

	//全部服务器上面的可持久化状态:
	currentTerm int       //服务器看到的最近Term(第一次启动的时候为0,后面单调递增)
	votedFor    int       //当前Term收到的投票候选 (如果没有就为null)
	logs        []logInfo //日志项; 每个日志项包含机器状态和被leader接收的Term(first index is 1)

	//所有服务器上经常变的
	commitIndex int //已经被提交的最新的日志索引(第一次为0,后面单调递增)
	lastApplied int //已经应用到服务器状态的最新的日志索引(第一次为0,后面单调递增)

	//在领导人里经常改变的 （选举后重新初始化）
	nextIndex  []int //对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
	matchIndex []int //对于每一个服务器，已经复制给他的日志的最高索引值

	// timer
	electionTimer   *time.Timer //投票超时时间
	electiontimeout time.Duration
	heartbeatTimer  *time.Timer
	rpcTimeout      time.Duration
	applyCh         chan ApplyMsg
	shutdown        chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)

	return term, isleader
}

func (rf *Raft) GetStateV2() (int, int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logs)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //任期
	CandidateID  int //候选人id
	LastLogIndex int //候选人的最后日志条目的索引值
	LastLogTerm  int //候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int  //当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool //候选人赢得了此张选票时为真
}

func (rf *Raft) beFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.resetElectionTime()
	//rf.persist()
}

func (rf *Raft) beLeader() {
	rf.state = Leader
	lastLog := rf.getLastLog()
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLog.Index + 1
		rf.matchIndex[i] = 0
	}
	DPrintln(2, "[beLeader]------", rf.me, "rf.nextIndex:", rf.nextIndex)
}

func (rf *Raft) beCandidate() {
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	//rf.persist()
}

func (rf *Raft) getLastLog() logInfo {
	length := len(rf.logs)
	return rf.logs[length-1]
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintln(3, "[RequestVote]------", rf.me, "->", args.CandidateID, "reply:", reply, "state:", rf.state)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		DPrintln(3, "[RequestVote] state change beFollower: me", rf.me, "receive:", args.CandidateID, "oldstate:", rf.state, "oldTerm:", rf.currentTerm, "newTerm:", args.Term, "args:", args)
		rf.beFollower(args.Term)
	}
	//args.Term = rf.currentTerm
	lastlog := rf.getLastLog()
	if (rf.votedFor == -1 || rf.state == Candidate) &&
		(args.LastLogTerm > lastlog.Term || (args.LastLogTerm == lastlog.Term && args.LastLogIndex >= lastlog.Index)) {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		rf.state = Follower
		rf.resetElectionTime()
		return
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) (ok bool) {
	ch := make(chan bool, 1)
	go func() {
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
		ch <- true
	}()
	select {
	case <-ch:
	case <-time.After(rf.rpcTimeout):
		//DPrintln(0, "[election] rpc failed,", rf.me, "->", index, "currentTerm:", rf.currentTerm)
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendLogEntriesReq, reply *AppendLogEntriesRsp) (ok bool) {
	ch := make(chan bool, 1)
	go func() {
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
		ch <- true
	}()
	select {
	case <-ch:
	case <-time.After(rf.rpcTimeout):
		//DPrintln(0, "[election] rpc failed,", rf.me, "->", index, "currentTerm:", rf.currentTerm)
	}
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	term := -1
	index := -1
	// DPrintln(4, "[Start]------", rf.me, "rf.logs:", rf.logs)
	// defer DPrintln(4, "[Start]------", rf.me, "rf.logs:", rf.logs)
	rf.mu.Lock()

	isLeader := rf.state == Leader
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	term = rf.currentTerm
	lastLog := rf.getLastLog()
	index = lastLog.Index + 1
	newLog := logInfo{
		Command: command,
		Term:    term,
		Index:   index,
	}
	rf.logs = append(rf.logs, newLog)
	rf.mu.Unlock()
	rf.startAppendLog()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.persist()
	close(rf.shutdown)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
// 建一个Raft端点。
// peers参数是通往其他Raft端点处于连接状态下的RPC连接。
// me参数是自己在端点数组中的索引。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = Follower
	rf.votedFor = -1
	electionTime := rand.Intn(150) + 150
	rf.electiontimeout = time.Duration(electionTime) * time.Millisecond
	rf.electionTimer = time.NewTimer(rf.electiontimeout)
	rf.heartbeatTimer = time.NewTimer(100 * time.Millisecond)

	rf.logs = make([]logInfo, 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.shutdown = make(chan struct{})
	rf.rpcTimeout = 50 * time.Millisecond
	rf.applyCh = applyCh
	DPrintln(3, "[Make] initialize me:", me, ",electiontimeout: ", me, rf.electiontimeout)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.raftLoop()
	return rf
}

func (rf *Raft) raftLoop() {
	for {
		DPrintln(0, "[Make] initialize me:", rf.me, "state:", rf.state, ",electiontimeout: ", rf.electiontimeout)
		switch atomic.LoadInt32(&rf.state) {
		case Follower:
			select {
			case <-rf.electionTimer.C:
				rf.startElection()
			case <-rf.shutdown:
				return
			}
		case Candidate:
			select {
			case <-rf.electionTimer.C:
				rf.startElection()
			case <-rf.shutdown:
				return
			}
		case Leader:
			select {
			case <-rf.heartbeatTimer.C:
				rf.heartbeatTimer.Reset(100 * time.Millisecond)
				rf.startAppendLog()
			case <-rf.shutdown:
				return
			}

		}

	}
}

//AppendLogEntriesReq 附加日志 由领导人负责调用来复制日志指令；也会用作heartbeat
type AppendLogEntriesReq struct {
	Term         int //任期
	LeaderID     int
	PrevLogIndex int       //新的日志条目紧随之前的索引值
	PrevLogTerm  int       //prevLogIndex 条目的任期号
	Entries      []logInfo //准备存储的日志条目
	LeaderCommit int       //领导人已经提交的日志的索引值
}

//AppendLogEntriesRsp ..
type AppendLogEntriesRsp struct {
	Term    int  //当前的任期号，用于领导人去更新自己
	Success bool //跟随者包含了匹配上 prevLogIndex 和 prevLogTerm 的日志时为真
}

func min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

//AppendEntries ...
func (rf *Raft) AppendEntries(args AppendLogEntriesReq, reply *AppendLogEntriesRsp) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintln(3, "[AppendEntries] ######## currentTerm:", rf.currentTerm, "me:", rf.me, "args:", args, "rf.logs:", rf.logs)
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		DPrintln(3, "[AppendEntries] state change beFollower: me", rf.me, "receive:", args.LeaderID, "oldstate:", rf.state, "oldTerm:", rf.currentTerm, "newTerm:", args.Term, "args:", args)
		rf.beFollower(args.Term)
	}
	rf.resetElectionTime()
	if len(rf.logs) > args.PrevLogIndex {
		if len(args.Entries) > 0 {
			DPrintln(3, "[AppendEntries] ===== currentTerm:", rf.currentTerm, "me:", rf.me, "args:", args, "rf.logs:", rf.logs)
		}
		if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			return
		}
		for i := 0; i < len(args.Entries); i++ {
			index := i + args.PrevLogIndex + 1
			if index >= len(rf.logs) {
				rf.logs = append(rf.logs, args.Entries[i:]...)
				break
			} else {
				if rf.logs[index].Term != args.Entries[i].Term {
					rf.logs = append(rf.logs[:index], args.Entries[i:]...)
					break
				}
			}
		}
		if len(args.Entries) > 0 {
			DPrintln(3, "[AppendEntries] ---- currentTerm:", rf.currentTerm, "me:", rf.me, "args:", args, "rf.logs:", rf.logs)
		}
	} else { //pre太大
		return
	}
	commitIndex := min(len(rf.logs)-1, args.LeaderCommit)
	commitIndex = min(commitIndex, args.PrevLogIndex+len(args.Entries))
	if commitIndex > rf.commitIndex {
		rf.commitIndex = commitIndex
		rf.sendApplyCh()
	}
	reply.Success = true
}

func (rf *Raft) sendApplyCh() {
	if rf.lastApplied < rf.commitIndex {
		DPrintln(3, "[sendApplyCh] begin me:", rf.me, "rf.lastApplied:", rf.lastApplied, "rf.commitIndex:", rf.commitIndex)
		defer DPrintln(3, "[sendApplyCh] end me:", rf.me, "rf.lastApplied:", rf.lastApplied, "rf.commitIndex:", rf.commitIndex)
	}
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[rf.lastApplied].Command,
			CommandIndex: rf.logs[rf.lastApplied].Index,
		}
	}
}

func (rf *Raft) resetElectionTime() {
	//rf.electionTimer.Stop()
	electionTime := rand.Intn(150) + 150
	rf.electiontimeout = time.Duration(electionTime) * time.Millisecond
	rf.electionTimer.Reset(time.Duration(electionTime) * time.Millisecond)
	//DPrintln(0, "[resetElectionTime] currentTerm:", rf.currentTerm, "me:", rf.me, "timeout:", rf.electiontimeout)
}

func (rf *Raft) startAppendLog() {
	var wg sync.WaitGroup
	DPrintln(0, "[startAppendLog] star currentTerm:", rf.currentTerm, "me:", rf.me)
	for i := 0; i < len(rf.peers) && rf.state == Leader; i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			oldNextIndex := rf.nextIndex[index]
			//DPrintln(2, "[startAppendLog] 111111 star currentTerm:", rf.currentTerm, "me:", rf.me, "index:", index, "oldNextIndex:", oldNextIndex, "len:", len(rf.logs))
			preLog := rf.logs[rf.nextIndex[index]-1]
			var appendlog []logInfo
			if len(rf.logs) > preLog.Index+1 {
				appendlog = rf.logs[preLog.Index+1:]
			}
			DPrintln(0, "[startAppendLog]", "netindex:", rf.nextIndex, "preLog.Index+1:", preLog.Index+1, "len(rf.logs):", len(rf.logs))
			DPrintln(0, "[startAppendLog] 111111 star currentTerm:", rf.currentTerm, "me:", rf.me, "index:", index, "oldNextIndex:", oldNextIndex, "len:", len(rf.logs), "appendlog:", appendlog)
			args := AppendLogEntriesReq{
				Term:         rf.currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: preLog.Index,
				PrevLogTerm:  preLog.Term,
				Entries:      appendlog,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			var reply AppendLogEntriesRsp
			ok := rf.sendAppendEntries(index, args, &reply)
			if len(appendlog) > 0 {
				DPrintln(3, "[startAppendLog] 22222 star currentTerm:", rf.currentTerm, "me:", rf.me, "index:", index, "oldNextIndex:", oldNextIndex, "len:", len(rf.logs),
					"args:", args, "reply:", reply)
			}
			if ok {
				if reply.Success == false {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm { //出现新的任期
						DPrintln(3, "[startAppendLog] state change beFollower: me", rf.me, "receive:", index, "oldstate:", rf.state, "oldTerm:", rf.currentTerm, "newTerm:", reply.Term, "reply:", reply)

						rf.beFollower(reply.Term)
						return
					} else { //不正确后退
						if rf.nextIndex[index] == oldNextIndex && rf.nextIndex[index] > 1 && rf.state == Leader {
							rf.nextIndex[index]--
						}
					}
				} else { //追加日志成功，更新nextIndex
					if rf.nextIndex[index] == oldNextIndex && rf.state == Leader {
						rf.nextIndex[index] += len(appendlog)
						rf.matchIndex[index] = rf.nextIndex[index] - 1
					}
				}
			}
		}(i)
	}
	wg.Wait()
	//DPrintln(4, "[startElection] startAppendLog star me:", rf.me, "currentTerm:", rf.currentTerm)
	commitIndex := make([]int, len(rf.peers))
	rf.mu.Lock()
	copy(commitIndex, rf.matchIndex)
	sort.Ints(commitIndex)
	rf.commitIndex = commitIndex[len(rf.peers)/2+1]
	DPrintln(0, "[startAppendLog] startAppendLog star me:", rf.me, "currentTerm:", rf.currentTerm, "commitIndex:", commitIndex, "rf.commitIndex:", rf.commitIndex)
	rf.sendApplyCh()
	rf.mu.Unlock()
}

//变成候选者
func (rf *Raft) startElection() {
	DPrintln(3, "[startElection] 11111 star me:", rf.me, "currentTerm:", rf.currentTerm)
	rf.resetElectionTime()
	rf.mu.Lock()
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	lastLog := rf.getLastLog()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	rf.mu.Unlock()

	var wg sync.WaitGroup
	var voteNum int32 = 1
	DPrintln(3, "[startElection] star me:", rf.me, "currentTerm:", rf.currentTerm)
	for i := 0; i < len(rf.peers) && rf.state == Candidate; i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			var reply RequestVoteReply
			ok := rf.sendRequestVote(index, args, &reply)
			DPrintln(3, "[startElection] rpc state:", ok, rf.me, "->", index, "currentTerm:", rf.currentTerm, "reply:", reply)
			if ok {
				if reply.VoteGranted {
					atomic.AddInt32(&voteNum, 1)
				} else {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						DPrintln(3, "[startElection] state change beFollower: me", rf.me, "receive:", index, "oldstate:", rf.state, "oldTerm:", rf.currentTerm, "newTerm:", reply.Term, "reply:", reply)
						rf.beFollower(args.Term)
					}
					rf.mu.Unlock()
				}
			}
		}(i)
	}
	wg.Wait()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if int(voteNum) >= (len(rf.peers)+1)/2 && rf.state == Candidate {
		//rf.state = Leader
		rf.beLeader()
		DPrintln(3, "[startElection] success me:", rf.me, "currentTerm:", rf.currentTerm, "voteNum:", voteNum)
		return
	}
	DPrintln(3, "[startElection] failure me:", rf.me, "currentTerm:", rf.currentTerm, "voteNum:", voteNum)
}
