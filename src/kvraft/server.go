package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const TIMEOUT = 500 * time.Millisecond

const INTERVAL = 10 * time.Millisecond

const Debug = false

var gStart time.Time

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		prefix := fmt.Sprintf("%06d ", time.Since(gStart).Milliseconds())
		fmt.Printf(prefix+format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Type      string
	ClientID  int64
	RequestID int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	state            map[string]string          // state machine for k-v pairs as string
	sessions         map[int64]ClientRecord     // memoization for each client's last response
	waitChs          map[int]chan *CommandReply // a map of waitChs to retrieve corresponding command after agreement
	lastAppliedIndex int                        // last applied index of log entry
	persister        *raft.Persister            // the persister of the kv server
}

// merge read/write RPCs to one
// RPC handler for client's command
func (kv *KVServer) CommandRequest(args *CommandArgs, reply *CommandReply) {
	kv.mu.Lock()
	defer DPrintf("Server %d responded client %d's request %d: %+v\n", kv.me, args.ClientID, args.RequestID, reply)

	DPrintf("Server %d received request: %+v\n", kv.me, args)
	// check for duplicates
	if args.Op != "Get" && kv.checkDuplicate(args.ClientID, args.RequestID) {
		reply.Err = kv.sessions[args.ClientID].LastResponse.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	// send command to raft for agreement
	index, _, isLeader := kv.rf.Start(Op{args.Key, args.Value, args.Op, args.ClientID, args.RequestID})
	if index == -1 || !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	DPrintf("Server %d sent client %d's request %d to raft at index %d\n", kv.me, args.ClientID, args.RequestID, index)
	kv.mu.Lock()
	waitCh := kv.getWaitCh(index)
	kv.mu.Unlock()

	select {
	case agreement := <-waitCh:
		reply.Err = agreement.Err
		reply.Value = agreement.Value

	case <-time.NewTimer(TIMEOUT).C:
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.killWaitCh(index)
		kv.mu.Unlock()
	}()
}

// method to check duplicated CommandRequest
func (kv *KVServer) checkDuplicate(clientID int64, requestID int) bool {
	clientRecord, ok := kv.sessions[clientID]
	return ok && requestID <= clientRecord.RequestID
}

// method to get a waitCh for an expected commit index
func (kv *KVServer) getWaitCh(index int) chan *CommandReply {
	ch, ok := kv.waitChs[index]
	if !ok {
		ch := make(chan *CommandReply, 1)
		kv.waitChs[index] = ch
		return ch
	}
	return ch
}

// method to kill waitCh, have to call with lock
func (kv *KVServer) killWaitCh(index int) {
	ch, ok := kv.waitChs[index]
	if ok {
		close(ch)
		delete(kv.waitChs, index)
	}
}

// method to apply command to state machine
func (kv *KVServer) applyCommand(op Op) *CommandReply {
	reply := &CommandReply{Err: OK}
	if op.Type == "Get" {
		val, ok := kv.state[op.Key]
		if ok {
			reply.Value = val
		} // else can reply empty string for no-key
	} else if op.Type == "Put" {
		kv.state[op.Key] = op.Value
	} else {
		kv.state[op.Key] += op.Value
	}
	return reply
}

//
// For lab 3b
//

// the mothod to determine if raft state is oversized
func (kv *KVServer) needSnapshot() bool {
	return kv.maxraftstate >= 0 && kv.persister.RaftStateSize() >= kv.maxraftstate
}

// the method to take snapshot, call with lock held
func (kv *KVServer) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.state)
	e.Encode(kv.sessions)
	e.Encode(kv.lastAppliedIndex)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

// method to apply snapshot to state machine, call with lock held
func (kv *KVServer) applySnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var State map[string]string
	var Sessions map[int64]ClientRecord
	var LastAppliedIndex int

	// decode, print error but do not panic
	err1 := d.Decode(&State)
	err2 := d.Decode(&Sessions)
	err3 := d.Decode(&LastAppliedIndex)
	if err1 != nil || err2 != nil || err3 != nil {
		DPrintf("Decoding error:%v, %v\n", err1, err2)
	} else {
		// apply
		kv.state = State
		kv.sessions = Sessions
		kv.lastAppliedIndex = LastAppliedIndex
	}
}

//
// long-running applier goroutine
//
func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			// commited command
			if applyMsg.CommandValid {
				op := applyMsg.Command.(Op)

				kv.mu.Lock()
				// if outdated, ignore
				if applyMsg.CommandIndex <= kv.lastAppliedIndex {
					kv.mu.Unlock()
					continue
				}
				kv.lastAppliedIndex = applyMsg.CommandIndex

				var reply *CommandReply
				// check for duplicates before apply to state machine
				if op.Type != "Get" && kv.checkDuplicate(op.ClientID, op.RequestID) {
					reply = kv.sessions[op.ClientID].LastResponse
				} else {
					reply = kv.applyCommand(op)
					// DPrintf("Server %d applied command %+v\n", kv.me, command)
					if op.Type != "Get" {
						kv.sessions[op.ClientID] = ClientRecord{op.RequestID, reply}
					}
				}

				// after applying command, compare if raft is oversized
				if kv.needSnapshot() {
					DPrintf("Server %d takes a snapshot till index %d\n", kv.me, applyMsg.CommandIndex)
					kv.takeSnapshot(applyMsg.CommandIndex)
				}

				// check the same term and leadership before reply
				if currentTerm, isLeader := kv.rf.GetState(); currentTerm == applyMsg.CommandTerm && isLeader {
					ch := kv.getWaitCh(applyMsg.CommandIndex)
					ch <- reply
				}
				kv.mu.Unlock()
			} else { // committed snapshot
				kv.mu.Lock()
				if kv.lastAppliedIndex < applyMsg.SnapshotIndex {
					DPrintf("Server %d receives a snapshot till index %d\n", kv.me, applyMsg.SnapshotIndex)
					kv.applySnapshot(applyMsg.Snapshot)
					// server receiving snapshot must be a follower/crashed leader so no need to reply
				}
				kv.mu.Unlock()
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// initialize global timestamp
	if gStart.IsZero() {
		gStart = time.Now()
	}
	DPrintf("Server %d launched!\n", me)
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	// You may need initialization code here.
	kv.state = make(map[string]string)
	kv.waitChs = make(map[int]chan *CommandReply)
	kv.sessions = make(map[int64]ClientRecord)

	// restore snapshot
	kv.applySnapshot(kv.persister.ReadSnapshot())

	go kv.applier()
	return kv
}
