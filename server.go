package main

import (
	"flag"

	"github.com/2naive2/raftkv/kvraft"
	"github.com/2naive2/raftkv/raft"
	lg "github.com/sirupsen/logrus"
)

func main() {
	num := flag.Int("num", 0, "number of running raft peer")
	debug := flag.Bool("debug", false, "whether to output debug info")
	flag.Parse()
	persister := raft.MakePersister()
	kvraft.StartKVServer(*num, persister, 0, *debug)
	lg.Info("start server done!")
	select {}
}
