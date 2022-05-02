package shardkv

import (
	"sync"

	"6.824/shardctrler"
)

// RPC handler for shard migration
func (kv *ShardKV) PullShards(args *PullShardsArgs, reply *PullShardsReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// if the group does not have the shards for the required config
	var configNum int
	if kv.state == Ready {
		configNum = kv.config.Num - 1
	} else {
		configNum = kv.config.Num
	}
	if args.ConfigNum > configNum+1 {
		return
	}

	// if not leader or still serving, return
	if _, isLeader := kv.rf.GetState(); !isLeader || kv.state == Serving {
		return
	}

	// send shards to reply
	reply.Shards = []Shard{}
	for _, shardNum := range args.Shards {
		reply.Shards = append(reply.Shards, copyShard(kv.db[shardNum]))
	}
	reply.Err = OK
}

// method to pull shards from other groups, return a new db
func (kv *ShardKV) pullShards(newConfig shardctrler.Config) map[int]Shard {
	kv.mu.Lock()
	oldConfig := kv.config

	needShards := make(map[int][]int) // map gid->shardNum
	for shardNum, gid := range newConfig.Shards {
		source := kv.config.Shards[shardNum]
		if gid == kv.gid && source != kv.gid {
			needShards[source] = append(needShards[source], shardNum)
		}
	}

	newDB := copyDB(kv.db)
	lock := sync.Mutex{}
	kv.mu.Unlock()

	// send pullShards RPC in parallel, use wait group to sync
	var wg sync.WaitGroup
	for gid, shards := range needShards {
		if gid != 0 {
			wg.Add(1)

			go func(gid int, shards []int, newDB *map[int]Shard, lock *sync.Mutex) {
				defer wg.Done()

				args := PullShardsArgs{
					ConfigNum: newConfig.Num,
					Shards:    shards,
				}
				servers := oldConfig.Groups[gid]

				for {
					// try each server in the group
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						var reply PullShardsReply
						ok := srv.Call("ShardKV.PullShards", &args, &reply)
						// persistently try until OK
						if ok && reply.Err == OK {
							// update the shards in the newDB
							lock.Lock()
							for _, shard := range reply.Shards {
								db := *newDB
								db[shard.Num] = shard
							}
							lock.Unlock()
							return
						}
					}
				}

			}(gid, shards, &newDB, &lock)
		}
	}
	wg.Wait()
	return newDB
}

// helper function to make a deep copy of db
func copyDB(db map[int]Shard) map[int]Shard {
	cp := make(map[int]Shard)
	for i := 0; i < shardctrler.NShards; i++ {
		cp[i] = copyShard(db[i])
	}
	return cp
}

// helper function to make a deep copy of shard
func copyShard(shard Shard) Shard {
	cp := Shard{
		Num:      shard.Num,
		Sessions: make(map[int64]ClientRecord),
		Data:     make(map[string]string),
	}
	for key, value := range shard.Data {
		cp.Data[key] = value
	}
	for key, value := range shard.Sessions {
		cp.Sessions[key] = value
	}
	return cp
}

// helper function to make a deep copy of config
func copyConfig(config shardctrler.Config) shardctrler.Config {
	var shards [shardctrler.NShards]int
	for shard, gid := range config.Shards {
		shards[shard] = gid
	}
	cp := shardctrler.Config{
		Num:    config.Num,
		Shards: shards,
		Groups: make(map[int][]string),
	}
	for key, value := range config.Groups {
		cp.Groups[key] = value
	}
	return cp
}
