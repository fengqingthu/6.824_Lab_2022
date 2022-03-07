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
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// global const, timeout range
const LOW = 500
const HIGH = 1000

// global const, heartbeat interval
const HEARTBEAT = 50

// global const, atomic unit
const INTERVAL = 50
const TIMERUNIT = 20

// DPrint configs
const PRINTLOG = true
const PRINTCOMMAND = true

// persist configs
const PERSIST = true

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Command interface{}
	Term    int
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
	votedFor    int
	log         []*Entry // 1-indexed
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	leader      int // leader's index into peers[]

	timeout   *Counter      // randomized timeout
	timer     *Counter      // timer
	applyCond *sync.Cond    // applyCOnd
	applyCh   chan ApplyMsg // apply channel
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.leader == rf.me
	rf.mu.Unlock()
	return term, isleader
}

// re-randomized timeout
func (rf *Raft) ResetTimeout() {
	rf.timeout.reset()
	rf.timeout.increment(rand.Intn(HIGH-LOW) + LOW)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	if !PERSIST {
		return
	}
	// DPrintf("Server %d (T: %d) persists its state!\n", rf.me, rf.currentTerm)

	// encode and send to persister
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if !PERSIST {
		return
	}
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Log []*Entry

	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil ||
		d.Decode(&Log) != nil {
		panic("Decoding Error!\n")
	} else {
		rf.currentTerm = CurrentTerm
		rf.votedFor = VotedFor
		rf.log = Log
	}

	DPrintf("Server %d (T: %d) reads from persisted state, now log: "+rf.printLog(), rf.me, rf.currentTerm)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		// update term and convert to follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leader = -1

		rf.persist()
	}

	// if from previous term, reject vote
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	// larger or equal terms, get state to compare log
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term

	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		if lastLogTerm > args.LastLogTerm {
			reply.VoteGranted = false
			return
		}
		if lastLogTerm < args.LastLogTerm || lastLogIndex <= args.LastLogIndex {
			// grant vote
			if rf.votedFor != args.CandidateId {
				rf.votedFor = args.CandidateId

				rf.persist()
			}
			reply.VoteGranted = true
			rf.timer.reset()
			return
		}
	}
	reply.VoteGranted = false
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

// a thread-safe Counter class
type Counter struct {
	sync.RWMutex
	num int
}

// get method
func (ct *Counter) get() int {
	ct.RLock()
	res := ct.num
	ct.RUnlock()
	return res
}

// increment counter
func (ct *Counter) increment(h int) {
	ct.Lock()
	ct.num += h
	ct.Unlock()
}

// reset counter
func (ct *Counter) reset() {
	ct.Lock()
	ct.num = 0
	ct.Unlock()
}

//
// method to start election
//
func (rf *Raft) startElection() {
	// reset timer
	rf.timer.reset()

	// get state and increment term
	rf.mu.Lock()
	rf.leader = -1
	rf.currentTerm++
	rf.votedFor = rf.me
	currentTerm := rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.persist()
	rf.mu.Unlock()

	// instead of wait group, can use counters with lock, or a channel
	total := len(rf.peers)
	majority := total/2 + 1
	winVotes := Counter{num: 1} // vote for itself
	loseVotes := Counter{}
	hasEnded := Counter{} // if larger than 0 then the election should have ended

	for i := 0; i < total; i++ {
		if i == rf.me {
			continue
		}
		// send RequestVote RPC in parallel
		go func(rf *Raft, server int) {
			// check if election already ends
			if hasEnded.get() > 0 {
				return
			}
			// check candidate state and term
			rf.mu.Lock()
			if rf.leader != -1 || rf.votedFor != rf.me || rf.currentTerm != currentTerm {
				rf.mu.Unlock()
				return
			}
			// initialize args and reply
			args := RequestVoteArgs{currentTerm, rf.me, lastLogIndex, lastLogTerm}
			reply := RequestVoteReply{}
			rf.mu.Unlock()
			// RPC call
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					// found higher term, convert to follower
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.leader = -1
					rf.persist()
				}
				// check candidate state and term
				if rf.leader != -1 || rf.votedFor != rf.me || rf.currentTerm != currentTerm {
					return
				}
				// check for election has not ended
				if hasEnded.get() > 0 {
					return
				}
				if reply.VoteGranted {
					winVotes.increment(1)
				} else {
					loseVotes.increment(1)
				}

				// wins the election
				if winVotes.get() >= majority {
					// becomes leader
					hasEnded.increment(1)
					DPrintf("Server %d (T: %d) becomes the leader!\n", rf.me, rf.currentTerm)
					rf.leader = rf.me
					// initialize leader's state and send heartbeat
					for i := 0; i < total; i++ {
						rf.nextIndex[i] = len(rf.log)
						rf.matchIndex[i] = 0
					}
					// start heartbeat goroutine
					go rf.heartbeater()
					return
				}
				// lose the election
				if loseVotes.get() >= majority {
					// convert to follower
					hasEnded.increment(1)
					rf.votedFor = -1

					rf.persist()

					return
				}
			}
		}(rf, i)
	}
}

//
// AppendEntries RPC Args structure
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Entry
	LeaderCommit int
}

//
// AppendEntries RPC Reply structure
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// AppendEntries RPC Handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// from higher term: update term and convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leader = -1
		rf.persist()
	}

	// if from previous term, decline
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	// match prevLog: if log length does not match
	lastLogIndex := len(rf.log) - 1
	if lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		return
	}
	// if log term does not match
	prevLogTerm := rf.log[args.PrevLogIndex].Term
	if prevLogTerm != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// compare leader' entries with existing entries
	i := args.PrevLogIndex + 1
	j := 0
	for i <= lastLogIndex && j < len(args.Entries) {
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
		i++
		j++
	}
	// if an exisiting entry conflicts with a new one, delete the existing entry and all that follow it
	if i <= lastLogIndex && j < len(args.Entries) {
		rf.log = rf.log[:i]
		rf.persist()
	}
	// append new entries not already in the log
	if j < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[j:]...)
		DPrintf("Follower %d (T: %d) replicated by leader %d (T: %d), now log:"+rf.printLog(), rf.me, rf.currentTerm, args.LeaderId, args.Term)
		rf.persist()
	}

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit <= len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
		rf.applyCond.Broadcast()
	}

	// for heartbeat: reset timer and build authority
	rf.leader = args.LeaderId
	rf.timer.reset()

	// return true
	reply.Success = true
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
	// Your code here (2B).
	currentTerm, isLeader := rf.GetState()
	if rf.killed() || !isLeader {
		return -1, currentTerm, isLeader
	}
	// append command entry to its own log
	rf.mu.Lock()
	index := len(rf.log) // the expected index
	rf.log = append(rf.log, &Entry{command, currentTerm})
	DPrintf("Leader %d (T: %d) receives command, now log: "+rf.printLog(), rf.me, rf.currentTerm)
	rf.persist()
	rf.mu.Unlock()

	if _, isLeader := rf.GetState(); isLeader {
		go rf.sendHeartbeat()
	}

	return index, currentTerm, isLeader
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
// the long-running goroutine of heartbeat for the leader
//
func (rf *Raft) heartbeater() {
	for !rf.killed() {
		_, isleader := rf.GetState()
		if isleader {
			// send heartbeat
			go rf.sendHeartbeat()
			// check for commitable entries
			go rf.updateCommit()
		} else {
			// ends if no longer leader
			return
		}
		time.Sleep(HEARTBEAT * time.Millisecond)
	}
}

//
// the method to send heartbeat to all followers
//
func (rf *Raft) sendHeartbeat() {

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			// send heatbeat in parallel
			go rf.sendAppendEntries(i)
		}
	}
}

//
// the method to send appendEntries RPC call to given follower
//
func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	// check leadership
	if rf.leader != rf.me {
		rf.mu.Unlock()
		return
	}
	currentTerm := rf.currentTerm
	nextIndex := rf.nextIndex[server]
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.log[prevLogIndex].Term

	entries := []*Entry{} // initialized as empty
	if len(rf.log)-1 >= nextIndex {
		entries = append(entries, rf.log[nextIndex:]...)
	}

	// initialize RPC args and reply
	args := AppendEntriesArgs{currentTerm, rf.leader, prevLogIndex, prevLogTerm, entries, rf.commitIndex}
	reply := AppendEntriesReply{}

	rf.mu.Unlock()
	// call AppendEntriesArgs RPC
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			// convert back to follower
			rf.currentTerm = reply.Term
			rf.leader = -1
			rf.votedFor = -1
			rf.persist()
		}

		// check leadership, term and up-to-date reply
		if rf.leader != rf.me || rf.currentTerm != currentTerm || rf.nextIndex[server] != nextIndex {
			return
		}

		// update nextIndex and matchIndex
		if reply.Success {
			if len(entries) != 0 {
				DPrintf("Leader %d (T: %d) replicated with follower %d\n", rf.me, rf.currentTerm, server)
			}
			rf.matchIndex[server] = prevLogIndex + len(entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		} else {
			// kinda binary search, retry
			rf.nextIndex[server] = nextIndex / 2
			go rf.sendAppendEntries(server)
		}
	}
}

//
// the method to check for commitable entry and update commitIndex
//
func (rf *Raft) updateCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	total := len(rf.peers)
	majority := total/2 + 1
	mx := rf.commitIndex
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		if rf.log[n].Term == rf.currentTerm {
			num := 1 // leader itself
			for i := 0; i < total; i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					num++
				}
			}
			if num >= majority {
				DPrintf("Leader %d (T: %d) views entries till %d commitable\n", rf.me, rf.currentTerm, n)
				mx = n
				break
			}
		}
	}
	if mx > rf.commitIndex {
		rf.commitIndex = mx
		rf.applyCond.Broadcast()
	}
}

//
// the method to fetch log content string for debugging
//
func (rf *Raft) printLog() string {
	content := ""
	if PRINTLOG {
		for _, entry := range rf.log {
			if PRINTCOMMAND {
				content += fmt.Sprintf("%d,%v ", entry.Term, entry.Command)
			} else {
				content += fmt.Sprintf("%d ", entry.Term)
			}
		}
	}
	content += "\n"
	return content
}

//
// the applier goroutine
//
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for !(rf.commitIndex > rf.lastApplied) {
			rf.applyCond.Wait()
		}

		rf.lastApplied++
		// apply
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied,
		}
		rf.mu.Unlock()
		rf.applyCh <- applyMsg
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// Your code here to check if a leader election should
	// be started and to randomize sleeping time using
	// time.Sleep().

	// initialize timeout
	rf.ResetTimeout()

	// timer goroutine
	go func(timer *Counter) {
		lastTime := time.Now()
		for !rf.killed() {
			time.Sleep(TIMERUNIT * time.Millisecond)
			timer.increment(int(time.Since(lastTime).Milliseconds()))
			lastTime = time.Now()
		}
	}(rf.timer)

	// election timeout checker
	for !rf.killed() {
		_, isleader := rf.GetState()
		if !isleader {
			if rf.timer.get() > rf.timeout.get() {
				rf.ResetTimeout()
				rf.mu.Lock()
				DPrintf("Server %d (T: %d) starts election, now log: "+rf.printLog(), rf.me, rf.currentTerm)
				rf.mu.Unlock()
				rf.startElection()
			}
		}
		interval := rand.Intn(INTERVAL) + INTERVAL/2
		time.Sleep(time.Duration(interval) * time.Millisecond)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should(n't?) start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	// initialize global timestamp
	if gStart.IsZero() {
		gStart = time.Now()
	}

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	DPrintf("Server %d launched!\n", me)
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh
	rf.leader = -1
	rf.votedFor = -1
	rf.log = []*Entry{}
	rf.log = append(rf.log, &Entry{nil, 0})
	rf.timer = &Counter{}
	rf.timeout = &Counter{}
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start background goroutines to start elections
	go rf.ticker()
	go rf.applier()
	return rf
}
