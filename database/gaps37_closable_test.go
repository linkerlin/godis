package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps37ConfigRangesAndArity(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-node-timeout", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-node-timeout') - argument must be between 0 and 9223372036854775807 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-backlog-ttl", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-backlog-ttl') - argument must be between 0 and 9223372036854775807 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "auto-aof-rewrite-min-size", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'auto-aof-rewrite-min-size') - argument must be a memory value")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "auto-aof-rewrite-min-size", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'auto-aof-rewrite-min-size') - argument must be a memory value")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-announce-port", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-announce-port') - argument must be between 0 and 65535 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-announce-port", "65536")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-announce-port') - argument must be between 0 and 65535 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-announce-bus-port", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-announce-bus-port') - argument must be between 0 and 65535 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slave-announce-port", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'slave-announce-port') - argument must be between 0 and 65535 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replica-announce-port", "65536")),
		"ERR CONFIG SET failed (possibly related to argument 'replica-announce-port') - argument must be between 0 and 65535 inclusive")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "RESETSTAT", "x")),
		"ERR wrong number of arguments for 'config|resetstat' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "REWRITE", "x")),
		"ERR wrong number of arguments for 'config|rewrite' command")
}

func TestGaps37StreamXDelXRangeCount(t *testing.T) {
	db := makeTestDB()
	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XADD", "xs", "*", "a", "1")))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "xs", "abc")),
		"ERR Invalid stream ID specified as stream command argument")
	asserts.AssertMultiBulkReplySize(t, db.Exec(nil, utils.ToCmdLine("XRANGE", "xs", "-", "+", "COUNT", "-1")), 0)
	asserts.AssertMultiBulkReplySize(t, db.Exec(nil, utils.ToCmdLine("XRANGE", "xs", "-", "+", "COUNT", "0")), 0)
	asserts.AssertMultiBulkReplySize(t, db.Exec(nil, utils.ToCmdLine("XREVRANGE", "xs", "+", "-", "COUNT", "-1")), 0)
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XRANGE", "xs", "-", "+", "COUNT", "abc")),
		"ERR value is not an integer or out of range")
}
