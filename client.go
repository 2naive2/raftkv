package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/2naive2/raftkv/kvraft"
	lg "github.com/sirupsen/logrus"
)

func main() {
	ck := kvraft.MakeClerk()
	time.Sleep(time.Second * 3)
	lg.Info("starting client...")
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print(">:")
		cmd, err := reader.ReadString('\n')
		if err != nil {
			lg.Fatal("read command failed")
		}
		ops := strings.Fields(cmd)
		switch ops[0] {
		case "get":
			key := ops[1]
			res := ck.Get(key)
			fmt.Printf("get [%v] => [%v],ok\n", ops[1], res)
		case "put":
			key := ops[1]
			val := ops[2]
			ck.Put(key, val)
			fmt.Printf("put [%v] [%v] ok\n", ops[1], ops[2])
		case "append":
			key := ops[1]
			val := ops[2]
			ck.Append(key, val)
			fmt.Printf("append [%v] [%v] ok\n", ops[1], ops[2])
		default:
			fmt.Printf("unknown operation:%v", ops[0])
		}
	}
}
