package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps40VectorAndFTArityCasing(t *testing.T) {
	db := makeTestDB()
	for _, cmd := range []string{"VCARD", "VDIM", "VINFO", "VEMB", "VREM", "VISMEMBER", "VGETATTR"} {
		asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine(cmd)),
			"ERR wrong number of arguments for '"+cmd+"' command")
	}
	for _, cmd := range []string{"FT.CREATE", "FT.SEARCH", "FT.ALIASADD", "FT.INFO", "FT.AGGREGATE"} {
		asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine(cmd)),
			"ERR wrong number of arguments for '"+cmd+"' command")
	}
}

func TestGaps40ConfigLatencyAclRdbReplPing(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "latency-monitor-threshold", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'latency-monitor-threshold') - argument must be between 0 and 9223372036854775807 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "latency-monitor-threshold", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'latency-monitor-threshold') - argument couldn't be parsed into an integer")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "latency-monitor-threshold", "0")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "latency-monitor-threshold")),
		[]string{"latency-monitor-threshold", "0"})

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "acl-pubsub-default", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'acl-pubsub-default') - argument(s) must be one of the following: allchannels, resetchannels")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "acl-pubsub-default", "allchannels")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "acl-pubsub-default")),
		[]string{"acl-pubsub-default", "allchannels"})
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "acl-pubsub-default", "ResetChannels")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "acl-pubsub-default")),
		[]string{"acl-pubsub-default", "resetchannels"})

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "rdb-save-incremental-fsync", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'rdb-save-incremental-fsync') - argument must be 'yes' or 'no'")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "rdb-save-incremental-fsync", "yes")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "rdb-save-incremental-fsync")),
		[]string{"rdb-save-incremental-fsync", "yes"})

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-ping-replica-period", "0")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-ping-replica-period') - argument must be between 1 and 2147483647 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-ping-replica-period", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-ping-replica-period') - argument couldn't be parsed into an integer")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-ping-replica-period", "10")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "repl-ping-replica-period")),
		[]string{"repl-ping-replica-period", "10"})
}
