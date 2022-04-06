package raft

import (
	"sync"
)

// a thread-safe Counter class
type Counter struct {
	sync.RWMutex
	num int
}

// get method
func (ct *Counter) get() int {
	ct.RLock()
	res := ct.num
	ct.RUnlock()
	return res
}

// increment counter
func (ct *Counter) increment(h int) int {
	ct.Lock()
	num := ct.num + h
	ct.num = num
	ct.Unlock()
	return num
}

// reset counter
func (ct *Counter) reset() {
	ct.Lock()
	ct.num = 0
	ct.Unlock()
}
