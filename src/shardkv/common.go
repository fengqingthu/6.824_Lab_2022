package shardkv

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
