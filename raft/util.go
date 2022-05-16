package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func genRandomElectionTimeout() time.Duration {
	nums := rand.Int63n(int64(ElectionDeltaTime))
	return ElectionMinTime + time.Duration(nums)*time.Millisecond
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func logNewer(a1, a2, b1, b2 int64) bool {
	if a1 > b1 {
		return true
	}
	if a1 == b1 {
		return a2 >= b2
	}
	return false
}

func copyLog(log []LogEntry) []LogEntry {
	newLog := make([]LogEntry, 0, len(log))
	copy(newLog, log)
	return newLog
}

func copySlice(src []LogEntry, from int, data []LogEntry) []LogEntry {
	if from > len(src)-1 {
		return append(src, data...)
	}

	for i := from; i < minInt(len(src), from+len(data)); i++ {
		src[i] = data[i-from]
	}

	if len(data) > len(src)-from {
		src = append(src, data[len(src)-from:]...)
	}
	return src
}
