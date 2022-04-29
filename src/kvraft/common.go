package kvraft

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type ClientRecord struct {
	RequestID    int
	LastResponse *CommandReply
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
