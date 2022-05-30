package main

import (
	"github.com/2naive2/raftkv/kvraft"
	"github.com/2naive2/raftkv/raft"
	lg "github.com/sirupsen/logrus"
)

func main() {
	persister := raft.MakePersister()
	server := kvraft.StartKVServer(nil, 0, persister, 0)
	lg.Info("start server done!,server:%+v", server)
	select {}
}
