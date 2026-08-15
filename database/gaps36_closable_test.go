package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps36ConfigRanges(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "stream-node-max-entries", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'stream-node-max-entries') - argument must be between 0 and 9223372036854775807 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-backlog-size", "0")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-backlog-size') - argument must be between 1 and 9223372036854775807 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-backlog-size", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-backlog-size') - argument must be a memory value")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-diskless-sync-delay", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-diskless-sync-delay') - argument must be between 0 and 2147483647 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-migration-barrier", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-migration-barrier') - argument must be between 0 and 2147483647 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "auto-aof-rewrite-percentage", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'auto-aof-rewrite-percentage') - argument must be between 0 and 2147483647 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "auto-aof-rewrite-percentage", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'auto-aof-rewrite-percentage') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "lua-time-limit", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'lua-time-limit') - argument must be between 0 and 9223372036854775807 inclusive")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "set-max-intset-entries", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'set-max-intset-entries') - argument must be between 0 and 9223372036854775807 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "zset-max-listpack-entries", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'zset-max-listpack-entries') - argument must be between 0 and 9223372036854775807 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tracking-table-max-keys", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'tracking-table-max-keys') - argument must be between 0 and 9223372036854775807 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slowlog-max-len", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'slowlog-max-len') - argument must be between 0 and 9223372036854775807 inclusive")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replica-priority", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'replica-priority') - argument must be between 0 and 2147483647 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "min-replicas-to-write", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'min-replicas-to-write') - argument must be between 0 and 2147483647 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "min-replicas-max-lag", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'min-replicas-max-lag') - argument must be between 0 and 2147483647 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-replica-validity-factor", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-replica-validity-factor') - argument must be between 0 and 2147483647 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-timeout", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-timeout') - argument must be between 1 and 2147483647 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-timeout", "0")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-timeout') - argument must be between 1 and 2147483647 inclusive")
}

func TestGaps36ClientHelpArity(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "HELP", "x")),
		"ERR wrong number of arguments for 'client|help' command")
	asserts.AssertNotError(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "HELP")))
}
