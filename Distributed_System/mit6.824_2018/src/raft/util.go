package raft

import "log"

// Debugging
const Debug = 30

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DPrintln(level int, a ...interface{}) (n int, err error) {
	if level >= Debug {
		log.Println(a...)
	}
	return
}

func init() {
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
}
