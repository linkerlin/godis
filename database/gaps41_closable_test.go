package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps41ScriptFunctionUnknownSubcommand(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("SCRIPT")),
		"ERR wrong number of arguments for 'script' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("SCRIPT", "SHOW")),
		"ERR unknown subcommand 'SHOW'. Try SCRIPT HELP.")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("SCRIPT", "show")),
		"ERR unknown subcommand 'show'. Try SCRIPT HELP.")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("FUNCTION")),
		"ERR wrong number of arguments for 'function' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("FUNCTION", "FOO")),
		"ERR unknown subcommand 'FOO'. Try FUNCTION HELP.")
}

func TestGaps41ConfigLFUTenacityListAofCrash(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "lfu-log-factor", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'lfu-log-factor') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "lfu-log-factor", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'lfu-log-factor') - argument must be between 0 and 2147483647 inclusive")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "lfu-log-factor", "10")), "OK")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "lfu-decay-time", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'lfu-decay-time') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "lfu-decay-time", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'lfu-decay-time') - argument must be between 0 and 2147483647 inclusive")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-eviction-tenacity", "101")),
		"ERR CONFIG SET failed (possibly related to argument 'maxmemory-eviction-tenacity') - argument must be between 0 and 100 inclusive")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-eviction-tenacity", "10")), "OK")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "list-compress-depth", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'list-compress-depth') - argument must be between 0 and 2147483647 inclusive")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "list-compress-depth", "0")), "OK")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "aof-timestamp-enabled", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'aof-timestamp-enabled') - argument must be 'yes' or 'no'")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "aof-timestamp-enabled", "no")), "OK")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-disable-tcp-nodelay", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-disable-tcp-nodelay') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "latency-tracking", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'latency-tracking') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "crash-log-enabled", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'crash-log-enabled') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "crash-memcheck-enabled", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'crash-memcheck-enabled') - argument must be 'yes' or 'no'")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-diskless-load", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-diskless-load') - argument(s) must be one of the following: disabled, on-empty-db, swapdb, flushdb")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-diskless-load", "disabled")), "OK")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-preferred-endpoint-type", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-preferred-endpoint-type') - argument(s) must be one of the following: ip, hostname, unknown-endpoint")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-preferred-endpoint-type", "ip")), "OK")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-link-sendbuf-limit", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-link-sendbuf-limit') - argument must be a memory value")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-link-sendbuf-limit", "0")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-link-sendbuf-limit")),
		[]string{"cluster-link-sendbuf-limit", "0"})

	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-announce-hostname", "node.example")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-announce-hostname")),
		[]string{"cluster-announce-hostname", "node.example"})
}
