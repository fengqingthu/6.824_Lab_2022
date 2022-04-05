package raft

import (
	"fmt"
	"time"
)

// Debugging
const Debug = true

// DPrint configs
const PRINTLOG = false
const PRINTCOMMAND = false

var gStart time.Time

func DPrintf(format string, a ...interface{}) {
	if Debug {
		prefix := fmt.Sprintf("%06d ", time.Since(gStart).Milliseconds())
		fmt.Printf(prefix+format, a...)
	}
	return
}

//
// the method to fetch log content string for debugging
//
func (rf *Raft) printLog() string {
	content := ""
	if PRINTLOG {
		for _, entry := range rf.log {
			if PRINTCOMMAND {
				content += fmt.Sprintf("%d,%v ", entry.Term, entry.Command)
			} else {
				content += fmt.Sprintf("%d ", entry.Term)
			}
		}
	}
	content += "\n"
	return content
}
