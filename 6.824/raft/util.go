package raft

import (
	"flag"
	"fmt"
	"log"
)

func init() {
	flag.IntVar(&Debug, "debug", 0, "debug log")
	log.SetFlags(log.Lshortfile)
}

// Debugging
var Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Output(2, fmt.Sprintf(format, a...))
		//log.Printf(format, a...)
	}
	return
}
