package shardkv

import (
	"6.824/shardctrler"
)

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
	ErrWrongGroup  = "ErrWrongGroup"
	ErrNotServing  = "ErrNotServing"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type ClientRecord struct {
	RequestID    int
	LastResponse *CommandReply
}

type Shard struct {
	Num      int
	Sessions map[int64]ClientRecord // each shard maintains its own sessions for handling duplicates
	Data     map[string]string
}

const (
	Serving = "Serving"
	Prepare = "Prepare"
	Ready   = "Ready"
)

type State string

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
	InternalID int64 // uuid for internal commands
	Config     shardctrler.Config
	State      State

	// for internal command - change db
	DB map[int]Shard
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

type PullShardsArgs struct {
	ConfigNum int
	Shards    []int
}

type PullShardsReply struct {
	Shards []Shard
	Err    Err
}
