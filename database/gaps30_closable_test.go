package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps30ConfigWording(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "aof-load-truncated", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'aof-load-truncated') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "activerehashing", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'activerehashing') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replica-announced", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'replica-announced') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replica-ignore-maxmemory", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'replica-ignore-maxmemory') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-allow-replica-migration", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-allow-replica-migration') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "sanitize-dump-payload", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'sanitize-dump-payload') - argument(s) must be one of the following: no, yes, clients")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-replica-validity-factor", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-replica-validity-factor') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-timeout", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-timeout') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "min-replicas-to-write", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'min-replicas-to-write') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "min-replicas-max-lag", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'min-replicas-max-lag') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-samples", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'maxmemory-samples') - argument couldn't be parsed into an integer")
}

func TestGaps30ArityClientTSStream(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("ACL", "HELP", "x")),
		"ERR wrong number of arguments for 'acl|help' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("PUBSUB", "NUMPAT", "x")),
		"ERR wrong number of arguments for 'pubsub|numpat' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("REPLICAOF", "127.0.0.1")),
		"ERR wrong number of arguments for 'replicaof' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "CACHING", "FOO")),
		"ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled")

	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TS.CREATERULE", "a", "b", "AGGREGATION", "foo", "100")),
		"ERR TSDB: Unknown aggregation type")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TS.CREATERULE", "a", "b", "AGGREGATION", "avg", "abc")),
		"ERR TSDB: Couldn't parse AGGREGATION")
	asserts.AssertNullBulk(t, db.Exec(nil, utils.ToCmdLine("JSON.OBJLEN", "nosuch")))

	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XADD", "x", "*", "a", "1")))
	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "x", "g", "0")))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "SETID", "x", "g", "abc")),
		"ERR Invalid stream ID specified as stream command argument")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XACK", "x", "g", "abc")),
		"ERR Invalid stream ID specified as stream command argument")
}
