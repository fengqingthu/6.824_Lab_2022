package shardctrler

//
// Shard controler: assigns shards to replication groups.
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
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK         = "OK"
	ErrTimeout = "ErrTimeout"
)

type Err string

const (
	Join  = "Join"
	Leave = "Leave"
	Move  = "Move"
	Query = "Query"
)

type Command string

// type JoinArgs struct {
// 	Servers map[int][]string // new GID -> servers mappings
// }

// type JoinReply struct {
// 	WrongLeader bool
// 	Err         Err
// }

// type LeaveArgs struct {
// 	GIDs []int
// }

// type LeaveReply struct {
// 	WrongLeader bool
// 	Err         Err
// }

// type MoveArgs struct {
// 	Shard int
// 	GID   int
// }

// type MoveReply struct {
// 	WrongLeader bool
// 	Err         Err
// }

// type QueryArgs struct {
// 	Num int // desired config number
// }

// type QueryReply struct {
// 	WrongLeader bool
// 	Err         Err
// 	Config      Config
// }

type ClientRecord struct {
	RequestID    int
	LastResponse *CommandReply
}

type CommandArgs struct {
	Type      Command
	ClientID  int64
	RequestID int
	Servers   map[int][]string // for join: new GID -> servers mappings
	GIDs      []int            // for leave: GIDs to remove
	Shard     int              // for move: the shard to be assigned
	GID       int              // for move: the specified GID
	Num       int              // for query: desired config number
}

type CommandReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
