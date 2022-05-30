package main

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/2naive2/raftkv/kvraft"
	"github.com/2naive2/raftkv/model"
	"github.com/2naive2/raftkv/raft"
	lg "github.com/sirupsen/logrus"
)

func main() {
	conf := model.Conf{}
	reader, err := os.Open("conf.json")
	if err != nil {

	}
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {

	}
	err = json.Unmarshal(bytes, &conf)
	if err != nil {

	}
	persister := raft.MakePersister()
	server := kvraft.StartKVServer(nil, 0, persister, 0, conf.ServerAddress["0"])
	lg.Info("start server done!,server:%+v", server)
	select {}
}
