package raft

import (
	"flag"
	"log"
)

func init() {
	flag.IntVar(&Debug, "debug", 0, "debug log")
}

// Debugging
var Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
