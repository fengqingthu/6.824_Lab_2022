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
func (ct *Counter) Get() int {
	ct.RLock()
	res := ct.num
	ct.RUnlock()
	return res
}

// increment counter
func (ct *Counter) Increment(h int) int {
	ct.Lock()
	num := ct.num + h
	ct.num = num
	ct.Unlock()
	return num
}

// reset counter
func (ct *Counter) Reset() {
	ct.Lock()
	ct.num = 0
	ct.Unlock()
}
