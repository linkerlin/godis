package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps27ConfigWording(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "hz", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'hz') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slowlog-log-slower-than", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'slowlog-log-slower-than') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slowlog-max-len", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'slowlog-max-len') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "hash-max-listpack-entries", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'hash-max-listpack-entries') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "list-max-listpack-size", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'list-max-listpack-size') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "zset-max-listpack-entries", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'zset-max-listpack-entries') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "stream-node-max-bytes", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'stream-node-max-bytes') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "notify-keyspace-events", "!!")),
		"ERR CONFIG SET failed (possibly related to argument 'notify-keyspace-events') - Invalid event class character. Use 'Ag$lshzxeKEtmdnocaSTIV'.")
}

func TestGaps27ServerArityAndLatency(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("TIME", "x")),
		"ERR wrong number of arguments for 'time' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("DBSIZE", "x")),
		"ERR wrong number of arguments for 'dbsize' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("LASTSAVE", "x")),
		"ERR wrong number of arguments for 'lastsave' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("COMMAND", "COUNT", "x")),
		"ERR wrong number of arguments for 'command|count' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("BGSAVE", "FOO")),
		"ERR syntax error")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("LATENCY", "GRAPH", "foo")),
		"ERR No samples available for event 'foo'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("REPLICAOF", "127.0.0.1", "abc")),
		"ERR Invalid master port")
}

func TestGaps27JSONProbStreamSearch(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("JSON.STRAPPEND", "nosuch", "$", `"x"`)),
		"ERR could not perform this operation on a key that doesn't exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TOPK.COUNT", "nosuch", "a")),
		"TopK: key does not exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("CMS.MERGE", "d", "1", "nosuch")),
		"CMS: key does not exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("FT.SUGADD", "k", "hello", "abc")),
		"ERR invalid score")

	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XADD", "x", "*", "a", "1")))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XSETID", "x", "0-0")),
		"ERR The ID specified in XSETID is smaller than the target stream top item")
}
