package shardkv

import (
	"time"

	"6.824/shardctrler"
)

// method to apply command to internal state
func (kv *ShardKV) applyCommandInternal(op Op) {
	switch op.Type {
	// case "Config":
	// 	kv.config = copyConfig(op.Config)
	// 	if _, isLeader := kv.rf.GetState(); isLeader {
	// 		DPrintf("Group %d server %d changed config to %+v\n", kv.gid, kv.me, kv.config)
	// 	}
	case "Prepare":
		kv.config = copyConfig(op.Config)
		kv.db = copyDB(op.DB)
		kv.state = op.State
		if _, isLeader := kv.rf.GetState(); isLeader {
			DPrintf("Group %d server %d changed config to %+v\n", kv.gid, kv.me, kv.config)
			DPrintf("Group %d server %d changed db to %+v\n", kv.gid, kv.me, kv.db)
		}
	case "State":
		kv.state = op.State
		if _, isLeader := kv.rf.GetState(); isLeader {
			DPrintf("Group %d server %d changed state to %+v during config %d\n", kv.gid, kv.me, kv.state, kv.config.Num)
		}
	// case "DB":
	// 	kv.db = copyDB(op.DB)
	// 	if _, isLeader := kv.rf.GetState(); isLeader {
	// 		DPrintf("Group %d server %d changed db to %+v during config %d\n", kv.gid, kv.me, kv.db, kv.config.Num)
	// 	}
	case "Empty":
		if _, isLeader := kv.rf.GetState(); isLeader {
			DPrintf("Group %d applied empty log entry during config %d\n", kv.gid, kv.config.Num)
		}
	}
}

// method to change the state of this replica group
// func (kv *ShardKV) changeConfig(config shardctrler.Config) bool {
// 	kv.mu.Lock()
// 	internalID := nrand()
// 	for {
// 		if _, ok := kv.appliedInternal[internalID]; ok {
// 			internalID = nrand()
// 		} else {
// 			break
// 		}
// 	}
// 	kv.appliedInternal[internalID] = false

// 	op := Op{
// 		Command:    "Internal",
// 		Type:       "Config",
// 		InternalID: internalID,
// 		Config:     config,
// 	}
// 	kv.mu.Unlock()
// 	return kv.sendCommandInternal(op)
// }

// method to prepare for the newconfig with the pulled newDB
func (kv *ShardKV) prepare(newConfig shardctrler.Config, newDB map[int]Shard) bool {
	kv.mu.Lock()
	internalID := nrand()
	for {
		if _, ok := kv.appliedInternal[internalID]; ok {
			internalID = nrand()
		} else {
			break
		}
	}
	kv.appliedInternal[internalID] = false

	op := Op{
		Command:    "Internal",
		Type:       "Prepare",
		InternalID: internalID,
		Config:     newConfig,
		DB:         newDB,
		State:      Ready,
	}
	kv.mu.Unlock()
	return kv.sendCommandInternal(op)
}

// method to change the state of this replica group
func (kv *ShardKV) changeState(state State) bool {
	kv.mu.Lock()
	internalID := nrand()
	for {
		if _, ok := kv.appliedInternal[internalID]; ok {
			internalID = nrand()
		} else {
			break
		}
	}
	kv.appliedInternal[internalID] = false

	op := Op{
		Command:    "Internal",
		Type:       "State",
		InternalID: internalID,
		State:      state,
	}
	kv.mu.Unlock()
	return kv.sendCommandInternal(op)
}

// method to change the db of this replica group
// func (kv *ShardKV) changeDB(db map[int]Shard) bool {
// 	kv.mu.Lock()
// 	internalID := nrand()
// 	for {
// 		if _, ok := kv.appliedInternal[internalID]; ok {
// 			internalID = nrand()
// 		} else {
// 			break
// 		}
// 	}
// 	kv.appliedInternal[internalID] = false

// 	op := Op{
// 		Command:    "Internal",
// 		Type:       "DB",
// 		InternalID: internalID,
// 		DB:         db,
// 	}
// 	kv.mu.Unlock()
// 	return kv.sendCommandInternal(op)
// }

func (kv *ShardKV) sendEmpty() bool {
	kv.mu.Lock()
	internalID := nrand()
	for {
		if _, ok := kv.appliedInternal[internalID]; ok {
			internalID = nrand()
		} else {
			break
		}
	}
	kv.appliedInternal[internalID] = false

	op := Op{
		Command:    "Internal",
		Type:       "Empty",
		InternalID: internalID,
	}
	kv.mu.Unlock()
	return kv.sendCommandInternal(op)
}

// method to persistently send internal command
func (kv *ShardKV) sendCommandInternal(op Op) bool {
	// persistently send internal command
	for !kv.killed() {
		// if no longer leader, return false
		if _, isLeader := kv.rf.GetState(); !isLeader {
			return false
		}
		// if succeessfully applied return true
		if kv.commandInternal(op) {
			return true
		}
		time.Sleep(INTERVAL)
	}
	return false
}

// method to send internal command to raft layer, return true if agreed otherwise false
func (kv *ShardKV) commandInternal(op Op) bool {
	// send command to raft layer for agreement
	index, _, isLeader := kv.rf.Start(op)
	if index == -1 || !isLeader {
		return false
	}

	// checker loop
	for !kv.killed() {
		kv.mu.Lock()
		// if successfully applied
		if applied, ok := kv.appliedInternal[op.InternalID]; applied && ok {
			kv.mu.Unlock()
			return true
		} else {
			// if not applied
			if kv.lastAppliedIndex >= index {
				kv.mu.Unlock()
				return false
			}
		}
		kv.mu.Unlock()
		time.Sleep(INTERVAL)
	}
	return false
}
