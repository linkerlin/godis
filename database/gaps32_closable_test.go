package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Gaps-32: Redis 8.10 closable ERR wording + sibling coverage of gaps-27..31.

func TestGaps32ConfigWording(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replica-read-only", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'replica-read-only') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "rdbcompression", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'rdbcompression') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-allow-reads-when-down", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-allow-reads-when-down') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "stop-writes-on-bgsave-error", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'stop-writes-on-bgsave-error') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-migration-barrier", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-migration-barrier') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "client-query-buffer-limit", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'client-query-buffer-limit') - argument must be a memory value")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "proto-max-bulk-len", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - argument must be a memory value")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "hll-sparse-max-bytes", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'hll-sparse-max-bytes') - argument must be a memory value")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-backlog-size", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-backlog-size') - argument must be a memory value")
	for _, name := range []string{
		"lazyfree-lazy-expire",
		"lazyfree-lazy-server-del",
		"lazyfree-lazy-user-del",
		"lazyfree-lazy-user-flush",
	} {
		asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", name, "maybe")),
			"ERR CONFIG SET failed (possibly related to argument '"+name+"') - argument must be 'yes' or 'no'")
	}
}

func TestGaps32ArityAndStream(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("MEMORY", "STATS", "x")),
		"ERR wrong number of arguments for 'memory|stats' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("MEMORY", "PURGE", "x")),
		"ERR wrong number of arguments for 'memory|purge' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("MEMORY", "MALLOC-STATS", "x")),
		"ERR wrong number of arguments for 'memory|malloc-stats' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("MEMORY", "HELP", "x")),
		"ERR wrong number of arguments for 'memory|help' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("LATENCY", "DOCTOR", "x")),
		"ERR wrong number of arguments for 'latency|doctor' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("LATENCY", "HELP", "x")),
		"ERR wrong number of arguments for 'latency|help' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "TRACKING", "ON", "OPTOUT", "OPTIN")),
		"ERR You can't specify both OPTIN mode and OPTOUT mode")

	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "IDLETIME")),
		"ERR wrong number of arguments for 'object|idletime' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "REFCOUNT")),
		"ERR wrong number of arguments for 'object|refcount' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "DELCONSUMER", "missingkey", "g", "c")),
		"ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XAUTOCLAIM", "nx", "g", "c", "abc", "0")),
		"ERR Invalid min-idle-time argument for XAUTOCLAIM")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("FT.SUGADD", "dict", "member", "nan")),
		"ERR invalid score")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("FT.SUGADD", "dict", "member", "inf")), 1)

	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XADD", "xs", "*", "a", "1")))
	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "xs", "g", "0")))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XCLAIM", "xs", "g", "c", "0", "badid")),
		"ERR Unrecognized XCLAIM option 'badid'")
}
