package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps28ConfigWording(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "dynamic-hz", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'dynamic-hz') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replica-serve-stale-data", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'replica-serve-stale-data') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "set-max-listpack-entries", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'set-max-listpack-entries') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "stream-node-max-entries", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'stream-node-max-entries') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replica-priority", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'replica-priority') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-announce-bus-port", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-announce-bus-port') - argument couldn't be parsed into an integer")
}

func TestGaps28ArityAndHelpers(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("MONITOR", "x")),
		"ERR wrong number of arguments for 'monitor' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("SAVE", "x")),
		"ERR wrong number of arguments for 'save' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("MEMORY", "DOCTOR", "x")),
		"ERR wrong number of arguments for 'memory|doctor' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("LATENCY", "LATEST", "x")),
		"ERR wrong number of arguments for 'latency|latest' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("MODULE", "LIST", "x")),
		"ERR wrong number of arguments for 'module|list' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("PUBSUB", "HELP", "x")),
		"ERR wrong number of arguments for 'pubsub|help' command")
}

func TestGaps28StreamTSSearch(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TS.CREATERULE", "a", "b", "AGGREGATION", "avg", "100")),
		"ERR TSDB: the key does not exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TS.DELETERULE", "a", "b")),
		"ERR TSDB: the key does not exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("FT.ALIASDEL", "a")),
		"Alias does not exist")

	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XADD", "x", "*", "a", "1")))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XCLAIM", "x", "nosuch", "c", "0", "0-1")),
		"NOGROUP No such key 'x' or consumer group 'nosuch'")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XAUTOCLAIM", "x", "nosuch", "c", "0", "0-0")),
		"NOGROUP No such key 'x' or consumer group 'nosuch'")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XINFO", "CONSUMERS", "x", "nosuch")),
		"NOGROUP No such consumer group 'nosuch' for key name 'x'")
}
