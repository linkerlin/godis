package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps9FailoverTimeout(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("FAILOVER", "TIMEOUT", "-1")),
		"ERR FAILOVER timeout must be greater than 0")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("FAILOVER", "TIMEOUT", "0")),
		"ERR FAILOVER timeout must be greater than 0")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("FAILOVER", "TIMEOUT", "abc")),
		"ERR value is not an integer or out of range")
}

func TestGaps9WaitTimeoutNegative(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("WAIT", "0", "-1")),
		"ERR timeout is negative")
}

func TestGaps9WaitAOFRanges(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("WAITAOF", "-1", "0", "0")),
		"ERR value is out of range, value must between 0 and 1")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("WAITAOF", "0", "-1", "0")),
		"ERR value is out of range, must be positive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("WAITAOF", "0", "0", "-1")),
		"ERR timeout is negative")
}

func TestGaps9ScanCountNonPositive(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SCAN", "0", "COUNT", "0")),
		"ERR syntax error")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SCAN", "0", "COUNT", "-1")),
		"ERR syntax error")
}

func TestGaps9ClientKillIDNonPositive(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "KILL", "ID", "-1")),
		"ERR client-id should be greater than 0")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "KILL", "ID", "0")),
		"ERR client-id should be greater than 0")
}

func TestGaps9MemoryUsageSamplesNegative(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("MEMORY", "USAGE", "k", "SAMPLES", "-1")),
		"ERR syntax error")
}

func TestGaps9ObjectFreqMissingKey(t *testing.T) {
	db := makeTestDB()
	asserts.AssertNullBulk(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "FREQ", "nosuch")))
}
