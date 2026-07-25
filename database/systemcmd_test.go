package database

import (
	"math/rand"
	"testing"
	"time"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestPing(t *testing.T) {
	c := connection.NewFakeConn()
	actual := Ping(c, utils.ToCmdLine())
	asserts.AssertStatusReply(t, actual, "PONG")
	val := utils.RandString(5)
	actual = Ping(c, utils.ToCmdLine(val))
	asserts.AssertBulkReply(t, actual, val)
	actual = Ping(c, utils.ToCmdLine(val, val))
	asserts.AssertErrReply(t, actual, "ERR wrong number of arguments for 'ping' command")
}

func TestInfo(t *testing.T) {
	c := connection.NewFakeConn()
	ret := testServer.Exec(c, utils.ToCmdLine("INFO"))
	asserts.AssertNotError(t, ret)
	ret = testServer.Exec(c, utils.ToCmdLine("INFO", "server"))
	asserts.AssertNotError(t, ret)
	ret = testServer.Exec(c, utils.ToCmdLine("INFO", "client"))
	asserts.AssertNotError(t, ret)
	ret = testServer.Exec(c, utils.ToCmdLine("INFO", "cluster"))
	asserts.AssertNotError(t, ret)
	ret = testServer.Exec(c, utils.ToCmdLine("iNFO", "SeRvEr"))
	asserts.AssertNotError(t, ret)
	ret = testServer.Exec(c, utils.ToCmdLine("INFO", "Keyspace"))
	asserts.AssertNotError(t, ret)
	ret = testServer.Exec(c, utils.ToCmdLine("iNFO", "abc", "bde"))
	asserts.AssertErrReply(t, ret, "ERR wrong number of arguments for 'info' command")
	ret = testServer.Exec(c, utils.ToCmdLine("INFO", "abc"))
	asserts.AssertErrReply(t, ret, "ERR Invalid section for 'info' command")
}

func TestDbSize(t *testing.T) {
	c := connection.NewFakeConn()
	testServer.Exec(c, utils.ToCmdLine("FLUSHALL"))
	rand.NewSource(time.Now().UnixNano())
	randomNum := rand.Intn(10) + 1
	for i := 0; i < randomNum; i++ {
		key := utils.RandString(10)
		value := utils.RandString(10)
		testServer.Exec(c, utils.ToCmdLine("SET", key, value))
	}
	ret := testServer.Exec(c, utils.ToCmdLine("dbsize"))
	asserts.AssertIntReply(t, ret, randomNum)
}
