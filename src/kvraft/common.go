package kvraft

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type ClientRecord struct {
	requestID    int
	lastResponse *CommandReply
}

// Put or Append
// type PutAppendArgs struct {
// 	Key   string
// 	Value string
// 	Op    string // "Put" or "Append"
// 	// You'll have to add definitions here.
// 	// Field names must start with capital letters,
// 	// otherwise RPC will break.
// 	ID string
// }

// type PutAppendReply struct {
// 	Err Err
// }

// type GetArgs struct {
// 	Key string
// 	// You'll have to add definitions here.
// 	ID string
// }

// type GetReply struct {
// 	Err   Err
// 	Value string
// }

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
