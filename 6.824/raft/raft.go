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
	"context"
	"fmt"
	"github.com/WinChua/course/6.824/labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "github.com/WinChua/course/6.824/labgob"

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

func IDSTRING(i int) string {
	switch i {
	case E_IDEN_LEADER:
		return "L"
	case E_IDEN_FOLLOWER:
		return "F"
	case E_IDEN_CANDIDATE:
		return "C"
	}
	return "U"
}

const (
	E_IDEN_FOLLOWER = iota
	E_IDEN_LEADER
	E_IDEN_CANDIDATE
)

type LogEntry struct {
	Cmd  interface{}
	Term int
}

//
// A Go object implementing a single Raft peer.
//

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	identity    int

	lastHeartbeatTime time.Time
	termHaveVote      map[int]bool

	mIdxLogEntry       map[int]LogEntry
	lastSaveLogIdx     int
	followerNextLodIdx map[int]int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.identity == E_IDEN_LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	Who  int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Ok   bool
	Term int
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) String() string {
	return fmt.Sprintf("r[%d]t[%d]i[%s]", rf.me, rf.currentTerm, IDSTRING(rf.identity))
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	currentTerm := rf.currentTerm
	defer func() {
		DPrintf("%s receive vote from r[%d]t[%d]: args[%+v],reply[%+v]\n", rf.String(), args.Who, args.Term, args, reply)
		reply.Term = rf.currentTerm
	}()
	rf.mu.Lock()
	if v, ok := rf.termHaveVote[args.Term]; ok && v == true { // 如果当前term已经投过票了,不要投票
		rf.mu.Unlock()
		reply.Ok = false
		reply.Term = currentTerm
		return
	}
	rf.mu.Unlock()
	if currentTerm > args.Term { // 候选者的term比较小
		reply.Ok = false
		reply.Term = currentTerm
		return
	}
	if currentTerm == args.Term { // 目前没有log index比较,暂时返回true
		reply.Ok = true
		rf.mu.Lock()
		rf.identity = E_IDEN_FOLLOWER
		rf.mu.Unlock()
		return
	}
	if currentTerm < args.Term { // 如果candidate的term比我们的大,给他投票
		rf.mu.Lock()
		rf.identity = E_IDEN_FOLLOWER
		rf.currentTerm = args.Term
		rf.mu.Unlock()
		reply.Ok = true
		return
	}
}

type AppendEntryArgs struct {
	Who  int
	Term int
}

type AppendEntryReply struct {
	Ok   bool
	Term int
}

// empty msg if for heartbeat
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	defer func() {
		reply.Term = rf.currentTerm
	}()
	currentTerm := rf.currentTerm
	if currentTerm > args.Term { // 请求非法
		return
	} else if currentTerm <= args.Term {
		rf.mu.Lock()
		rf.currentTerm = currentTerm
		rf.identity = E_IDEN_FOLLOWER
		rf.lastHeartbeatTime = time.Now()
		rf.mu.Unlock()
	}
	return
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) requestForVoteToI(server int, currentTerm int) <-chan *RequestVoteReply {
	r := make(chan *RequestVoteReply, 1)
	go func() {
		args := &RequestVoteArgs{
			Who:  rf.me,
			Term: currentTerm,
		}
		reply := &RequestVoteReply{}
		defer func() {
			DPrintf("%s request vote to r[%d] with term[%d], args[%+v],reply[%+v]\n", rf.String(), server, currentTerm, args, reply)
		}()
		rf.sendRequestVote(server, args, reply)
		r <- reply
	}()
	return r
}

func (rf *Raft) requestForVote() {
	count := 1
	rf.mu.Lock()
	rf.currentTerm += 1
	currentTerm := rf.currentTerm
	rf.termHaveVote[rf.currentTerm] = true
	rf.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	results := make(chan *RequestVoteReply, 1)
	var wg sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			select {
			case reply := <-rf.requestForVoteToI(i, currentTerm):
				results <- reply
			case <-ctx.Done():
				results <- &RequestVoteReply{
					Ok: false,
				}
			}
		}(i)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	for result := range results {
		if result.Ok {
			count += 1
			if count > len(rf.peers)/2 {
				rf.mu.Lock()
				rf.identity = E_IDEN_LEADER
				rf.mu.Unlock()
				DPrintf("%s become leader\n", rf.String())
				cancel() // 取消其他投票请求的执行
				rf.sendHeartbeat()
				return
			}
		} else {
			if result.Term >= currentTerm {
				rf.mu.Lock()
				rf.identity = E_IDEN_FOLLOWER
				rf.currentTerm = result.Term
				rf.mu.Unlock()
				cancel()
				return
			}
		}
	}
	// no winner
	rf.mu.Lock()
	rf.identity = E_IDEN_FOLLOWER
	rf.mu.Unlock()
	DPrintf("raft[%d][%d] no winner\n", rf.me, currentTerm)
}

func (rf *Raft) sendHeartbeat() {
	currentTerm := rf.currentTerm
	for i := range rf.peers {
		if i == rf.me {
			rf.mu.Lock()
			rf.lastHeartbeatTime = time.Now()
			rf.mu.Unlock()
		} else {
			go func(i int) {
				args := &AppendEntryArgs{
					Who:  rf.me,
					Term: currentTerm,
				}
				reply := &AppendEntryReply{}
				rf.sendAppendEntry(i, args, reply)
			}(i)
		}
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	isLeader = rf.identity == E_IDEN_LEADER
	term = rf.currentTerm

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.termHaveVote = make(map[int]bool)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for range time.Tick(100 * time.Millisecond) {
			if rf.dead != 1 && rf.identity == E_IDEN_LEADER {
				rf.sendHeartbeat()
			}
			if rf.dead == 1 {
				return
			}
		}
	}()
	go func() {
		for {
			if rf.dead != 1 {
				rSTime := rand.Int63() % 50
				time.Sleep(time.Duration(100+rSTime) * time.Millisecond)
				if rf.identity != E_IDEN_LEADER {
					if time.Since(rf.lastHeartbeatTime) > 150*time.Millisecond {
						DPrintf("raft[%d][%d] loss heartbeat.\n", rf.me, rf.currentTerm)
						rf.requestForVote()
					}
				}
			} else {
				DPrintf("raft[%d] dead\n", rf.me)
				return
			}
		}
	}()
	return rf
}
