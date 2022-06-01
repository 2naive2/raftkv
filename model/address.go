package model

import (
	"encoding/json"
	"io/ioutil"
	"os"

	lg "github.com/sirupsen/logrus"
)

type Conf struct {
	ServerAddress map[string]string `json:"server_address"`
}

func GetConf() *Conf {
	conf := Conf{}
	reader, err := os.Open("conf.json")
	if err != nil {
		lg.Fatalf("open config file failed,err:%v", err)
	}
	bytes, err := ioutil.ReadAll(reader)
	if err != nil {
		lg.Fatalf("read config file failed,err:%v", err)
	}
	err = json.Unmarshal(bytes, &conf)
	if err != nil {
		lg.Fatalf("json unmarshal failed,err:%v", err)
	}
	return &conf
}
