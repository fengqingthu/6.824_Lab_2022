package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

// method to generate uuid
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	clientID   int64
	requestID  *raft.Counter // serial id of requests
	gid2leader map[int]int
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientID = nrand()
	ck.gid2leader = make(map[int]int)
	ck.requestID = &raft.Counter{}
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := &CommandArgs{
		Key:       key,
		Op:        "Get",
		ClientID:  ck.clientID,
		RequestID: ck.requestID.Increment(1),
	}
	return ck.sendCommandRequest(args).Value
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &CommandArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientID:  ck.clientID,
		RequestID: ck.requestID.Increment(1),
	}
	ck.sendCommandRequest(args)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// merge RPCs to one
func (ck *Clerk) sendCommandRequest(args *CommandArgs) *CommandReply {
	// send request persistently
	for {
		shard := key2shard(args.Key)
		gid := ck.config.Shards[shard]
		if gid != 0 {
			leader := ck.gid2leader[gid]
			oldLeader := leader

			if servers, ok := ck.config.Groups[gid]; ok {
				for {
					srv := ck.make_end(servers[leader])
					reply := &CommandReply{}
					ok := srv.Call("ShardKV.CommandRequest", args, reply)

					if ok && reply.Err == OK {
						// handled successfully, update leader map
						ck.gid2leader[gid] = leader
						return reply
					}
					if ok && reply.Err == ErrWrongGroup {
						break
					}
					// if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout, try next server
					leader = (leader + 1) % len(servers)
					// if already tried all servers
					if leader == oldLeader {
						break
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}
