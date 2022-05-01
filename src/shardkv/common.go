package shardkv

import "6.824/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type ClientRecord struct {
	RequestID    int
	LastResponse *CommandReply
}

type Shard struct {
	Num int
	// mu   sync.RWMutex
	Data map[string]string
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Command string // "Internal" or "Request"

	// for request command - handle client requests
	Key       string
	Value     string
	Type      string
	ClientID  int64
	RequestID int

	// for internal command - change config state
	InternalID int // the serial ID for internal commands
	Config     shardctrler.Config
	Serving    bool
}

// use an integrated RPC args & replys instead
type CommandArgs struct {
	Key       string
	Value     string
	Op        string
	ClientID  int64
	RequestID int
}

type CommandReply struct {
	Err   Err
	Value string
}
