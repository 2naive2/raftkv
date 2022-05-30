package main

import (
	"6.824/kvraft"
	"6.824/raft"
	lg "github.com/sirupsen/logrus"
)

func main() {
	persister := raft.MakePersister()
	server := kvraft.StartKVServer(nil, 0, persister, 0)
	lg.Info("start server done!,server:%+v", server)
	select {}
}
