package kvraft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {
	cfg := make_config(t, 3, false, 0)
	defer cfg.cleanup()

	cfg.begin("server test")
	ck := cfg.makeClient(cfg.All())
	time.Sleep(time.Second * 3)

	ck.Put("stuff", "lee")
	val := ck.Get("stuff")
	assert.Equal(t, val, "lee")

	ck.Put("name", "jack")
	val = ck.Get("name")
	assert.Equal(t, val, "jack")

	ck.Append("stuff", "_haha")
	val = ck.Get("stuff")
	assert.Equal(t, val, "lee_haha")
}
