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
	"strings"
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
	//termHaveVote      map[int]bool
	termHaveVote sync.Map

	mIdxLogEntry   sync.Map
	lastLogIdx     int
	lastSaveLogIdx int // 最后一个已经被commit的log的index

	followerNextLogIdx sync.Map

	saveLogCh chan int

	applyCh chan ApplyMsg
}

func (rf *Raft) showMap(m *sync.Map) string {
	text := make([]string, 0)
	f := func(key interface{}, value interface{}) bool {
		text = append(text, fmt.Sprintf("%v:%v", key, value))
		return true
	}
	m.Range(f)
	return strings.Join(text, ",")
}

func (rf *Raft) DebugString() string {
	return fmt.Sprintf("rf[%d]term[%d]identity[%s]lastLogIdx[%d]lastSaveLogIdx[%d]logs[%v]",
		rf.me, rf.currentTerm, IDSTRING(rf.identity), rf.lastLogIdx, rf.lastSaveLogIdx, rf.showMap(&rf.mIdxLogEntry))
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

func (rf *Raft) getNextLog(server int) (int, int) {
	//idx, _ := rf.followerNextLogIdx[server]
	var idx int
	v, ok := rf.followerNextLogIdx.Load(server)
	if ok {
		idx = v.(int)
	}
	//if v, ok := rf.mIdxLogEntry[idx]; ok {
	if v, ok := rf.mIdxLogEntry.Load(idx); ok {
		return idx, v.(LogEntry).Term
	}
	return idx, rf.currentTerm
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
	Term        int
	Who         int
	LastLogIdx  int
	LastLogTerm int
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

func (rf *Raft) replicateLog(command interface{}) (int, bool) {
	rf.mu.Lock()
	lastSaveLogIdx := rf.lastSaveLogIdx
	currentTerm := rf.currentTerm
	//rf.lastLogIdx++
	//rf.mIdxLogEntry[rf.lastLogIdx] = LogEntry{
	//	Cmd:  command,
	//	Term: currentTerm,
	//}
	currentCmd := LogEntry{Cmd: command, Term: currentTerm}
	lastLogIdx := rf.lastLogIdx
	var count = 1
	resultCh := make(chan *AppendEntryReply, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var wg sync.WaitGroup
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			f := func() chan *AppendEntryReply {
				rs := make(chan *AppendEntryReply, 1)
				go func() {
					iPrevLogIdx, iPrevLogTerm := rf.getNextLog(i)
					cmd := make([]interface{}, 0)
					terms := make([]int, 0)
					DPrintf("i:%d, iPrevLogIdx:%d, lastLogIdx:%d\n", i, iPrevLogIdx, lastLogIdx)
					for i := iPrevLogIdx + 1; i <= lastLogIdx; i++ {
						v, ok := rf.mIdxLogEntry.Load(i)
						if !ok {
							continue
						}
						l := v.(LogEntry)
						cmd = append(cmd, l.Cmd)
						terms = append(terms, l.Term)
					}
					cmd = append(cmd, currentCmd.Cmd)
					terms = append(terms, currentTerm)
					DPrintf("i[%d],len(cmd)[%d]\n", i, len(cmd))
					args := &AppendEntryArgs{
						PrevLogIdx:     iPrevLogIdx,
						PrevLogTerm:    iPrevLogTerm,
						Cmds:           cmd,
						CmdsTerm:       terms,
						Who:            rf.me,
						Term:           currentTerm,
						LastSaveLogIdx: lastSaveLogIdx,
					}
					reply := &AppendEntryReply{}
					rf.sendAppendEntry(i, args, reply)
					reply.Who = i
					rs <- reply
				}()
				return rs
				//resultCh <- reply
			}
			select {
			case reply := <-f():
				resultCh <- reply
			case <-ctx.Done():
				resultCh <- &AppendEntryReply{Who: i}
			}
		}(i)
	}
	go func() {
		wg.Wait()
		close(resultCh)
	}()
	idx := -1
	success := false
	for r := range resultCh {
		if r.Ok {
			count += 1
			rf.followerNextLogIdx.Store(r.Who, lastLogIdx+1)
			DPrintf("set nextlogid[%d] for r[%d]\n", lastLogIdx+1, r.Who)
			if count > len(rf.peers)/2 { // replicate success
				if !success {
					//rf.mu.Lock()
					rf.lastLogIdx++
					//	rf.mu.Unlock()
					rf.mIdxLogEntry.Store(rf.lastLogIdx, currentCmd)
					success = true
					idx = rf.lastLogIdx
				}
			}
		} else {
			if r.Term > rf.currentTerm {
				rf.currentTerm = r.Term
			}
			if r.Term != 0 {
				if v, ok := rf.followerNextLogIdx.Load(r.Who); ok {
					t := v.(int)
					if t != 0 {
						rf.followerNextLogIdx.Store(r.Who, t-1)
						DPrintf("minus %d's next log to %d\n", r.Who, t-1)
					}
				}
			}
		}
	}
	DPrintf("%s's lastLogIdx[%d] rf.lastLogIdx[%d] followerNext is %v", rf, lastLogIdx, rf.lastLogIdx, rf.showMap(&rf.followerNextLogIdx))
	rf.mu.Unlock()
	return idx, success
}

func (rf *Raft) commitLog(index int) {
	rf.saveLogCh <- index
}

func (rf *Raft) GetStatus() string {
	return fmt.Sprintf("%s,[%v]", rf, rf.showMap(&rf.mIdxLogEntry))
}
func (rf *Raft) String() string {
	return fmt.Sprintf("r[%d]t[%d]i[%s]", rf.me, rf.currentTerm, IDSTRING(rf.identity))
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	initStatus := rf.DebugString()
	currentTerm := rf.currentTerm
	defer func() {
		//DPrintf("%s receive vote from r[%d]t[%d]: args[%+v],reply[%+v]\n", rf.String(), args.Who, args.Term, args, reply)
		DPrintf("%s -> RequestVote(%v, %v) -> %s\n", initStatus, args, reply, rf.DebugString())
		reply.Term = rf.currentTerm
	}()
	if _, ok := rf.termHaveVote.Load(args.Term); ok {
		reply.Ok = false
		reply.Term = currentTerm
		return
	}
	if currentTerm > args.Term { // 候选者的term比较小
		reply.Ok = false
		reply.Term = currentTerm
		return
	}
	if currentTerm == args.Term { // 目前没有log index比较,暂时返回true
		//reply.Ok = true
		if rf.newerThanOur(args.LastLogIdx, args.LastLogTerm) {
			reply.Ok = true
			//rf.mu.Lock()
			rf.identity = E_IDEN_FOLLOWER
			//rf.mu.Unlock()
		} else {
			reply.Ok = false
		}
		return
	}
	if currentTerm < args.Term { // 如果candidate的term比我们的大,给他投票
		//reply.Ok = true
		//rf.mu.Lock()
		rf.currentTerm = args.Term
		if rf.newerThanOur(args.LastLogIdx, args.LastLogTerm) {
			rf.identity = E_IDEN_FOLLOWER
			reply.Ok = true
		} else {
			reply.Ok = false
		}
		//rf.mu.Unlock()
		return
	}
}

func (rf *Raft) newerThanOur(lastLogIdx, lastLogTerm int) bool {
	var oLastLogTerm int
	oLastLogIdx := rf.lastLogIdx
	if oLastLogIdx != 0 {
		if v, ok := rf.mIdxLogEntry.Load(rf.lastLogIdx); ok {
			oLastLogIdx = v.(LogEntry).Term
		}
		//oLastLogTerm = rf.mIdxLogEntry[rf.lastLogIdx].Term
	}
	defer func() {
		DPrintf("%s theirs(%d,%d), ours(%d,%d)\n", rf, lastLogIdx, lastLogTerm, oLastLogIdx, oLastLogTerm)
	}()
	if lastLogTerm > oLastLogTerm {
		return true
	} else if lastLogTerm == oLastLogTerm {
		if lastLogIdx >= oLastLogIdx {
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

type AppendEntryArgs struct {
	Who            int
	Term           int
	Cmds           []interface{}
	CmdsTerm       []int
	LastSaveLogIdx int
	PrevLogIdx     int
	PrevLogTerm    int
}

type AppendEntryReply struct {
	Ok   bool
	Term int
	Who  int
}

func (rf *Raft) checkPrevLogTerm(prevLogIdx, prevLogTerm int) bool {
	if prevLogIdx == 0 { // 初始没有任何提交
		return true
	}
	v, ok := rf.mIdxLogEntry.Load(prevLogIdx)
	if !ok {
		return false
	}
	prevLog, ok := v.(LogEntry)
	if !ok {
		return false
	}
	if prevLog.Term != prevLogTerm {
		return false
	}
	return true
}

// empty msg if for heartbeat
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.lastHeartbeatTime = time.Now()
	initStatus := rf.DebugString()
	defer func() {
		if len(args.Cmds) > 0 {
			DPrintf("%s AppendEntry(%+v, %+v) -> %s\n", initStatus, args, reply, rf.DebugString())
		}
		if len(args.Cmds) == 0 {
			//DPrintf("%s receive heartbeat from %d at [%v]\n", rf.DebugString(), args.Who, rf.lastHeartbeatTime)
		}
	}()
	defer func() {
		reply.Term = rf.currentTerm
		reply.Who = rf.me
		rf.saveLogCh <- args.LastSaveLogIdx
	}()
	currentTerm := rf.currentTerm
	if currentTerm > args.Term { // 请求非法
		reply.Ok = false
		return
	} else if currentTerm <= args.Term {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.identity = E_IDEN_FOLLOWER
		rf.mu.Unlock()
		prevCheck := rf.checkPrevLogTerm(args.PrevLogIdx, args.PrevLogTerm)
		if prevCheck {
			if len(args.Cmds) > 0 {
				//DPrintf("prevCheck: %v, lastLogIdx[%d], PrevLogIdx[%d]\n", prevCheck, rf.lastLogIdx, args.PrevLogIdx)
			}
			rf.mu.Lock()
			if rf.lastLogIdx > args.PrevLogIdx && len(args.Cmds) > 0 {
				//DPrintf("lastLogIdx[%d], prevLogIdx[%d], len(args.Cmds)[%d]\n", rf.lastLogIdx, args.PrevLogIdx, len(args.Cmds))
				DPrintf("%s lastLogIdx[%d], args.PrevLogIdx[%d]\n", rf, rf.lastLogIdx, args.PrevLogIdx)
				for ; rf.lastLogIdx > args.PrevLogIdx; rf.lastLogIdx-- {
					DPrintf("%s deleting %d, log[%v]\n", rf, rf.lastLogIdx, rf.showMap(&rf.mIdxLogEntry))
					rf.mIdxLogEntry.Delete(rf.lastLogIdx)
				}
			}
			for i, cmd := range args.Cmds {
				rf.lastLogIdx++
				rf.mIdxLogEntry.Store(rf.lastLogIdx, LogEntry{Cmd: cmd, Term: args.CmdsTerm[i]})
			}
			rf.mu.Unlock()
			reply.Ok = true
		} else {
			reply.Ok = false
		}
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

func (rf *Raft) requestForVoteToI(server int, currentTerm int, lastLogIdx, lastLogTerm int) <-chan *RequestVoteReply {
	r := make(chan *RequestVoteReply, 1)
	go func() {
		args := &RequestVoteArgs{
			Who:  rf.me,
			Term: currentTerm,
		}
		reply := &RequestVoteReply{}
		defer func() {
			//DPrintf("%s request vote to r[%d] with term[%d], args[%+v],reply[%+v]\n", rf.String(), server, currentTerm, args, reply)
		}()
		rf.sendRequestVote(server, args, reply)
		r <- reply
	}()
	return r
}

func (rf *Raft) requestForVote() {
	initStatus := rf.DebugString()
	defer func() {
		DPrintf("%s requestForVote get %s\n", initStatus, rf.DebugString())
	}()
	count := 1
	rf.mu.Lock()
	lastLogIdx := rf.lastLogIdx
	var lastLogTerm int
	if lastLogIdx != 0 {
		v, ok := rf.mIdxLogEntry.Load(lastLogIdx)
		if ok {
			l := v.(LogEntry)
			lastLogTerm = l.Term
		}
		//lastLogTerm = rf.mIdxLogEntry[lastLogIdx].Term
	}
	rf.currentTerm += 1
	currentTerm := rf.currentTerm
	rf.termHaveVote.Store(rf.currentTerm, true)
	//rf.termHaveVote[rf.currentTerm] = true
	rf.identity = E_IDEN_CANDIDATE
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
			case reply := <-rf.requestForVoteToI(i, currentTerm, lastLogIdx, lastLogTerm):
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
				//go func() {
				//	rf.sendHeartbeat()
				//}()
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
	////DPrintf("raft[%d][%d] no winner\n", rf.me, currentTerm)
}

func (rf *Raft) sendHeartbeat() {
	//DPrintf("%s sendheartbeat at %v\n", rf, time.Now())
	currentTerm := rf.currentTerm
	for i := range rf.peers {
		if i == rf.me {
			rf.mu.Lock()
			rf.lastHeartbeatTime = time.Now()
			rf.mu.Unlock()
		} else {
			go func(i int) {
				iPrevLogIdx, iPrevLogTerm := rf.getNextLog(i)
				args := &AppendEntryArgs{
					PrevLogIdx:     iPrevLogIdx,
					PrevLogTerm:    iPrevLogTerm,
					Who:            rf.me,
					Term:           currentTerm,
					LastSaveLogIdx: rf.lastSaveLogIdx,
				}
				reply := &AppendEntryReply{}
				rf.sendAppendEntry(i, args, reply)
				if reply.Term > currentTerm {
					rf.mu.Lock()
					//rf.identity = E_IDEN_FOLLOWER
					rf.currentTerm = reply.Term
					rf.mu.Unlock()
				}
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
	DPrintf("---------------- Start [%v]--------------\n", command)
	defer func() {
		DPrintf("---------------------END [%v]--------------------\n", command)
	}()
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	isLeader = rf.identity == E_IDEN_LEADER
	if !isLeader {
		return 0, 0, false // I'm not leader, couldn't replicate log
	}
	term = rf.currentTerm
	index, ok := rf.replicateLog(command)
	//DPrintf("%s replicate result [%d],[%v]\n", rf, index, ok)
	if ok {
		rf.commitLog(index)
	} else {
		index = rf.lastLogIdx + 1
		//index = -1
	}

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

	//rf.termHaveVote = make(map[int]bool)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//rf.mIdxLogEntry = make(map[int]LogEntry)
	//rf.followerNextLogIdx = make(map[int]int)
	for i := range rf.peers {
		rf.followerNextLogIdx.Store(i, 0)
		//rf.followerNextLogIdx[i] = 0
	}
	rf.saveLogCh = make(chan int, 10)
	go func() {
		for range time.Tick(100 * time.Millisecond) {
			if !rf.killed() && rf.identity == E_IDEN_LEADER {
				rf.sendHeartbeat()
			}
			if rf.killed() {
				return
			}
		}
	}()
	go func() {
		for {
			if !rf.killed() {
				rSTime := rand.Int63() % 50
				time.Sleep(time.Duration(100+rSTime) * time.Millisecond)
				//if rf.identity != E_IDEN_LEADER {
				if rf.identity == E_IDEN_FOLLOWER {
					if time.Since(rf.lastHeartbeatTime) > 110*time.Millisecond {
						DPrintf("raft[%d][%d] loss heartbeat at %v lastHeartbeatTime[%v].\n", rf.me, rf.currentTerm, time.Now(), rf.lastHeartbeatTime)
						rf.requestForVote()
					}
				}
			} else {
				//DPrintf("raft[%d] dead\n", rf.me)
				return
			}
		}
	}()
	rf.applyCh = applyCh
	go func() { // 只用一个协程提交log
		for {
			if !rf.killed() {
				saveLogIdx := <-rf.saveLogCh
				////DPrintf("%s receive savelogidx[%d], lastSaveLogidx[%d]\n", rf, saveLogIdx, rf.lastSaveLogIdx)
				for rf.lastSaveLogIdx < saveLogIdx {
					v, ok := rf.mIdxLogEntry.Load(rf.lastSaveLogIdx + 1)
					if !ok {
						//continue
						break
					}
					logEntry, ok := v.(LogEntry)
					if !ok {
						break
					}
					rf.lastSaveLogIdx++
					rf.applyCh <- ApplyMsg{
						CommandValid: true,
						Command:      logEntry.Cmd,
						CommandIndex: rf.lastSaveLogIdx,
					}
				}

			} else {
				return
			}
		}
	}()
	return rf
}
