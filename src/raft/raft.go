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

	//	"6.824/labgob"
	"6.824/labrpc"
)

const DEBUGPRINTS = true

// global const, timeout range
const LOW = 500
const HIGH = 1000

// global const, heartbeat interval
const HEARTBEAT = 100

// global const, atomic unit
const INTERVAL = 50

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

	timeout *Counter      // randomized timeout
	timer   *Counter      // timer
	applyCh chan ApplyMsg // apply channel
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

// re-randomized timeout and return the new timeout
func (rf *Raft) ResetTimeout() int {
	ran := rand.Intn(HIGH-LOW) + LOW
	rf.timeout.reset()
	rf.timeout.increment(ran)
	return ran
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
	currentTerm, _ := rf.GetState()
	reply.Term = currentTerm
	// if from previous term, reject vote
	if args.Term < currentTerm {
		reply.VoteGranted = false
		return
	}

	if args.Term > currentTerm {
		// update term and convert to follower
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leader = -1
		rf.mu.Unlock()
		rf.timer.reset()
	}

	// larger or equal terms, get state to compare log
	rf.mu.Lock()
	votedFor := rf.votedFor
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()
	if votedFor < 0 || votedFor == args.CandidateId {
		if lastLogTerm > args.LastLogTerm {
			return
		}
		if lastLogTerm < args.LastLogTerm || lastLogIndex <= args.LastLogIndex {
			// grant vote
			rf.mu.Lock()
			rf.votedFor = args.CandidateId
			rf.mu.Unlock()
			reply.VoteGranted = true
			rf.timer.reset()
			return
		}
	}
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
	total := len(rf.peers)
	term := rf.currentTerm
	id := rf.me
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()

	// instead of wait group, can use counters with lock, or a channel
	majority := total/2 + 1
	winVotes := Counter{}
	winVotes.num = 1 // vote for itself
	loseVotes := Counter{}
	newTerm := Counter{}

	for i := 0; i < total; i++ {
		if i == id {
			continue
		}
		// send RequestVote RPC
		go func(rf *Raft, server int, win *Counter, lose *Counter, new *Counter) {
			// initialize args and reply
			args := RequestVoteArgs{term, id, lastLogIndex, lastLogTerm}
			reply := RequestVoteReply{}
			// RPC call
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				if reply.Term > term { // found higher term
					// update itself, convert to follower
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.mu.Unlock()
					new.increment(1)
				}
				if reply.VoteGranted {
					win.increment(1)
				} else {
					lose.increment(1)
				}
			}
		}(rf, i, &winVotes, &loseVotes, &newTerm)
	}
	// check if election should end
	for {
		// lose the election
		if newTerm.get() > 0 || loseVotes.get() >= majority {
			// convert to follower
			return
		}
		// hear other leader
		rf.mu.Lock()
		leader := rf.leader
		rf.mu.Unlock()
		if leader > 0 && leader != rf.me {
			return
		}
		// wins election
		if winVotes.get() >= majority {
			// becomes leader
			if DEBUGPRINTS {
				fmt.Printf("Server %d becomes the leader!\n", rf.me)
			}
			rf.mu.Lock()
			rf.leader = rf.me
			// initialize leader's state and send heartbeat
			rf.nextIndex = []int{}
			rf.matchIndex = []int{}
			for i := 0; i < total; i++ {
				rf.nextIndex = append(rf.nextIndex, len(rf.log))
				rf.matchIndex = append(rf.matchIndex, 0)
			}
			rf.mu.Unlock()
			return
		}
		// split votes: all votes collected or timeout but no result
		if loseVotes.get()+winVotes.get() == total || rf.timer.get() > rf.timeout.get() {
			return
		}
	}
}

//
// AppendEntries RPC Args structure
//
type AppendEntriesArgs struct {
	Term         int
	LearId       int
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
	// get term and assign to reply
	currentTerm, _ := rf.GetState()
	reply.Term = currentTerm

	// if from previous term, decline
	if args.Term < currentTerm {
		reply.Success = false
		return
	}
	// from higher term: update term and convert to follower
	if args.Term > currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leader = -1
		rf.timer.reset()
		rf.mu.Unlock()
	}

	// get state and match prevLog
	rf.mu.Lock()
	lastLogIndex := len(rf.log) - 1
	rf.mu.Unlock()
	if lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		return
	}
	rf.mu.Lock()
	lastLogTerm := rf.log[args.PrevLogIndex].Term
	rf.mu.Unlock()
	if lastLogTerm != args.PrevLogTerm {
		reply.Success = false
		return
	}
	// clip trailing entries
	if lastLogIndex > args.PrevLogIndex {
		rf.mu.Lock()
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.mu.Unlock()
	}
	// append leader's entries
	rf.mu.Lock()
	rf.log = append(rf.log, args.Entries...)
	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit <= len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
	}
	// for heartbeat: reset timer and build authority
	rf.timer.reset()
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.leader = args.LearId
	rf.mu.Unlock()
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
	if DEBUGPRINTS {
		fmt.Printf("Leader %d receives command, now log:", rf.me)
		rf.printLog()
	}
	rf.mu.Unlock()
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
// the long-running goroutine for heartbeat
//
func (rf *Raft) heartbeat() {
	me := rf.me
	total := len(rf.peers)
	for !rf.killed() {
		_, isleader := rf.GetState()
		// if leader
		if isleader {
			rf.timer.reset()
			for i := 0; i < total; i++ {
				if i == me {
					continue
				}
				// send heatbeat
				go rf.sendHeartbeat(i)
			}
			// check for commitable entries
			go rf.updateCommit()
		}
		// all servers need to apply entries
		go rf.applyEntries()
		time.Sleep(HEARTBEAT * time.Millisecond)
	}
}

//
// the method to send heartbeat to given follower
//
func (rf *Raft) sendHeartbeat(server int) {
	// get state
	rf.mu.Lock()
	term := rf.currentTerm
	me := rf.me
	lastLogIndex := len(rf.log) - 1
	nextIndex := rf.nextIndex[server]
	prevLogIndex := nextIndex - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()
	entries := []*Entry{} // initialized as empty
	if lastLogIndex >= nextIndex {
		rf.mu.Lock()
		for i := nextIndex; i <= lastLogIndex; i++ {
			entries = append(entries, rf.log[i])
		}
		rf.mu.Unlock()
	}

	// initialize RPC args and reply
	args := AppendEntriesArgs{term, me, prevLogIndex, prevLogTerm, entries, leaderCommit}
	reply := AppendEntriesReply{}
	// call AppendEntriesArgs RPC
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	if ok {
		if reply.Term > term {
			// convert back to follower
			rf.mu.Lock()
			rf.leader = -1
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.timer.reset()
			rf.mu.Unlock()
			return
		}
		// if empty heartbeat
		if len(entries) == 0 {
			return
		}
		// update nextIndex and matchIndex
		if reply.Success {
			rf.mu.Lock()
			if DEBUGPRINTS {
				fmt.Printf("Replicated with follower %d\n", server)
			}
			rf.nextIndex[server] = len(rf.log)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			rf.mu.Unlock()
		} else { // kinda binary search
			rf.mu.Lock()
			rf.nextIndex[server] = nextIndex / 2
			rf.mu.Unlock()
		}
	}
}

//
// the method to check for commitable entry and update commitIndex
//
func (rf *Raft) updateCommit() {
	rf.mu.Lock()
	total := len(rf.peers)
	majority := total/2 + 1
	mx := rf.commitIndex
	for n := rf.commitIndex + 1; n < len(rf.log); n++ {
		if rf.log[n].Term == rf.currentTerm {
			num := 1 // leader itself
			for i := 0; i < total; i++ {
				if i != rf.me && rf.matchIndex[i] >= n {
					num++
				}
			}
			if num >= majority {
				if DEBUGPRINTS {
					fmt.Printf("Leader %d views entry %d commitable\n", rf.me, n)
				}
				mx = n
			} else {
				break
			}
		}
	}
	rf.commitIndex = mx
	rf.mu.Unlock()
}

//
// the method to apply commited entries
//
func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		// apply
		applyMsg := ApplyMsg{}
		applyMsg.CommandValid = true
		applyMsg.Command = rf.log[rf.lastApplied].Command
		applyMsg.CommandIndex = rf.lastApplied
		if DEBUGPRINTS {
			fmt.Printf("Server %d applied entry %d, now log:", rf.me, rf.lastApplied)
			rf.printLog()
		}
		rf.applyCh <- applyMsg
	}
	rf.mu.Unlock()
}

// method to print log for debugging
func (rf *Raft) printLog() {
	if DEBUGPRINTS {
		for i, entry := range rf.log {
			fmt.Printf(" %d,%d ", i, entry.Term)
		}
		fmt.Printf("\n")
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
	go func(rf *Raft) {
		for !rf.killed() {
			time.Sleep(INTERVAL * time.Millisecond)
			rf.timer.increment(INTERVAL)
		}
	}(rf)
	// election timeout checker
	for !rf.killed() {
		_, isleader := rf.GetState()
		if !isleader {
			if rf.timer.get() > rf.timeout.get() {
				rf.ResetTimeout()
				if DEBUGPRINTS {
					rf.mu.Lock()
					fmt.Printf("Server %d (term %d) starts election, now log:", rf.me, rf.currentTerm)
					rf.printLog()
					rf.mu.Unlock()
				}
				rf.startElection()
				// after an election, always reset the timer
				if DEBUGPRINTS {
					fmt.Printf("Server %d's election ends!\n", rf.me)
				}
				rf.timer.reset()
			}
		}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	if DEBUGPRINTS {
		fmt.Printf("Server %d launched!\n", me)
	}
	rf.applyCh = applyCh
	rf.leader = -1
	rf.votedFor = -1
	rf.log = []*Entry{}
	rf.log = append(rf.log, &Entry{"", 0})
	rf.timer = &Counter{}
	rf.timeout = &Counter{}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start background goroutines to start elections
	go rf.ticker()
	go rf.heartbeat()
	return rf
}
