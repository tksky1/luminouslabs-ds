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
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"luminouslabs-ds/labgob"
	"luminouslabs-ds/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// 拓展：log压缩
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int        //所有server持久化的数据（回应rpc前在本地磁盘上更新）
	votedFor     int        //所有server持久化的数据
	logs         []LogEntry //所有server持久化的数据
	Status       status     //server状态是leader,candidate,follower
	commitIndex  int        //server易变的数据
	lastApplied  int        //server易变的数据
	nextIndex    []int      //leader易变的数据（选举后注意更新）
	matchIndex   []int      //leader易变的数据（选举后注意更新）
	votesGet     int
	electionTime time.Time
	ApplyMsgCh   chan ApplyMsg
}
type LogEntry struct {
	Term    int
	Command interface{}
}
type status int

const (
	follower status = iota
	candidate
	leader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.Status == leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (拓展：持久化).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (拓展：持久化).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (拓展：日志压缩).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here.
	Term         int
	VotedGranted bool
}
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(Args *RequestVoteArgs, Reply *RequestVoteReply) { //判断条件按论文里写
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if Args.Term < rf.currentTerm {
		Reply.VotedGranted = false
		Reply.Term = rf.currentTerm
		return
	} else if rf.votedFor == -1 || rf.votedFor == Args.CandidateId {
		if Args.LastLogIndex >= len(rf.logs)-1 {
			Reply.VotedGranted = true
			Reply.Term = Args.Term
			rf.currentTerm = Args.Term
			rf.votedFor = Args.CandidateId
			rf.electionTime = time.Now()
			rf.persist()
		}
	}
	return
}

// func candidateAskVote()
func (rf *Raft) AppendEntries(Args *AppendEntriesArgs, Reply *AppendEntriesReply) { //多线程发送心跳
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if Args.Term < rf.currentTerm { //任期过时时添加失败
		Reply.Success = false
		Reply.Term = rf.currentTerm
		return
	}
	Reply.Success = true //添加成功，重置raft状态
	rf.currentTerm = Args.Term
	Reply.Term = rf.currentTerm
	rf.votedFor = -1
	rf.logs = append(rf.logs, Args.Entries...)
	rf.Status = follower
	rf.votesGet = 0
	rf.electionTime = time.Now() //每次接受心跳后重置选举计时
	rf.logs = append(rf.logs, Args.Entries...)
	return
}

// func ServerResponse() bool {
// 	args := AppenEntriesArgs{}
// 	reply := AppendEntriesReply{}
// 	ok := call("Raft.AppendEntries", &args, &reply)
// 	if ok {
// 		fmt.Println("server is alive!")
// 		return true
// 	} else {
// 		fmt.Println("call failed!")
// 		return false
// 	}
// }

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		fmt.Println("server选票发送成功！")
	} else {
		fmt.Println("选票发送失败！")
	}
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		fmt.Println("向server发送添加日志请求成功！")
	} else {
		fmt.Println("添加日志请求发送失败！")
	}
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (拓展：日志复制).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		// Your code here
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		startTime := time.Now() //记录每次循环的睡眠前时间，必须要在给定选举时间里重置选举计时，当超时时仅有睡眠前时间点之前的选举计时
		ms := 50 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		// if rf.Status == leader {
		// 	rf.leaderStartHeartbeat()
		// } else
		if rf.electionTime.Before(startTime) && rf.Status != leader { //选举超时,转变状态
			fmt.Print("选举超时了,", rf.me, "开始选举！")
			rf.Status = candidate //自己变成candiadate,先投自己一票，任期+1
			rf.votedFor = rf.me
			rf.currentTerm += 1
			rf.electionTime = time.Now() //重置选举计时
			rf.persist()                 //持久化存储
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) heartbeatTicker() { //心跳计时器,因为心跳的计时要低于选举计时
	for rf.killed() == false {
		time.Sleep(30 * time.Millisecond)
		rf.mu.Lock()
		if rf.Status == leader {
			rf.leaderStartHeartbeat()
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
	}
}
func (rf *Raft) leaderStartHeartbeat() {
	for severId, _ := range rf.peers {
		if severId == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: len(rf.logs) - 1,
				PrevLogTerm:  rf.logs[len(rf.logs)-1].Term,
				Entries:      make([]LogEntry, 0),
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			rf.mu.Unlock()
			rf.sendAppendEntries(server, &args, &reply)
			return
		}(severId)
	}
}
func (rf *Raft) startElection() {
	for serverId, _ := range rf.peers {
		if serverId == rf.me {
			continue //当为自己时不执行操作
		}
		go func(server int) { //并发发起选举
			rf.mu.Lock()
			fmt.Println("拉取投票！")
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.logs) - 1,
				LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
			}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			// reply := rf.RequestVote(&args, &response)
			flag := rf.sendRequestVote(server, &args, &reply) //每个其他的server投票
			if flag == true {
				//选票通信成功，出现论文中三种情形，当win the election（变成leader）/other candidate win（发送了一个信息发现任期比自己大，变回follower）/nobody wins（继续下一轮，要求要快）选举结束
				//同时要注意candidate的任期（args.Term）是否比rf.currenTerm大,因为是在循环后面可能碰到状态转换所以得判断自己还是不是candidate
				rf.mu.Lock()
				if args.Term < rf.currentTerm { //任期冲突，返回
					rf.mu.Unlock()
					return
				}
				if reply.Term > args.Term { //rpc返回了一个比自己大的任期，小丑竟是我自己，变回follower
					rf.Status = follower
					rf.votedFor = -1
					rf.votesGet = 0
					rf.currentTerm = reply.Term
					rf.electionTime = time.Now()
					rf.persist()
					rf.mu.Unlock()
					return
				}
				if reply.VotedGranted == true { //统计投票咯
					rf.votesGet++
					if rf.votesGet >= len(rf.peers)/2+1 { //当投票超过一半时
						rf.Status = leader
						rf.votedFor = -1
						rf.votesGet = 0
						rf.nextIndex = make([]int, len(rf.peers))
						for i, _ := range rf.nextIndex {
							rf.nextIndex[i] = len(rf.logs)
						}
						rf.matchIndex = make([]int, len(rf.peers))
						rf.matchIndex[rf.me] = len(rf.logs) - 1 //当AppendEntries时继续更新
						rf.electionTime = time.Now()
						rf.persist()
						rf.mu.Unlock()
						return
					}
				}
				rf.mu.Unlock()
				return
			}
		}(serverId)

	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			ApplyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.commitIndex],
				CommandIndex: rf.lastApplied,
			}
			rf.ApplyMsgCh <- ApplyMsg
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []LogEntry{}
	rf.logs = append(rf.logs, LogEntry{})
	rf.Status = follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votesGet = 0
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = 0
	}
	for i, _ := range rf.matchIndex {
		rf.matchIndex[i] = -1
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.electionTicker()
	go rf.heartbeatTicker()

	return rf
}
