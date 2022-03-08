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
func (ct *Counter) increment(h int) {
	ct.Lock()
	ct.num += h
	ct.Unlock()
}

// reset counter
func (ct *Counter) reset() {
	ct.Lock()
	ct.num = 0
	ct.Unlock()
}
