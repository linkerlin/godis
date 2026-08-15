package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps31ConfigWording(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-require-full-coverage", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-require-full-coverage') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-diskless-sync", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-diskless-sync') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-node-timeout", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-node-timeout') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-diskless-sync-delay", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-diskless-sync-delay') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-backlog-ttl", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-backlog-ttl') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tracking-table-max-keys", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'tracking-table-max-keys') - argument couldn't be parsed into an integer")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "sanitize-dump-payload", "clients")), "OK")
}

func TestGaps31ClientStreamTSObject(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "SETINFO", "FOO", "bar")),
		"ERR Unrecognized option 'FOO'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "TRACKING", "ON", "OPTIN", "OPTOUT")),
		"ERR You can't specify both OPTIN mode and OPTOUT mode")

	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TS.CREATE", "t", "DUPLICATE_POLICY", "foo")),
		"ERR TSDB: Unknown DUPLICATE_POLICY")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XINFO", "HELP", "x")),
		"ERR wrong number of arguments for 'xinfo|help' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING")),
		"ERR wrong number of arguments for 'object|encoding' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "FREQ", "s", "x")),
		"ERR wrong number of arguments for 'object|freq' command")

	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XADD", "x", "*", "a", "1")))
	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "x", "g", "0")))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "SETID", "x", "nosuch", "0")),
		"NOGROUP No such consumer group 'nosuch' for key name 'x'")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XCLAIM", "x", "g", "c", "abc", "0-1")),
		"ERR Invalid min-idle-time argument for XCLAIM")

	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("SET", "s", "v")))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "s", "inf")),
		"ERR value is not a valid float")
}
