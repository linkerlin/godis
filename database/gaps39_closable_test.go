package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps39ConfigSlaveAliasesAndSetListpackValue(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slave-read-only", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'slave-read-only') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slave-serve-stale-data", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'slave-serve-stale-data') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slave-priority", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'slave-priority') - argument must be between 0 and 2147483647 inclusive")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "set-max-listpack-value", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'set-max-listpack-value') - argument must be between 0 and 9223372036854775807 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "set-max-listpack-value", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'set-max-listpack-value') - argument couldn't be parsed into an integer")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "set-max-listpack-value", "32")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "set-max-listpack-value")),
		[]string{"set-max-listpack-value", "32"})
}

func TestGaps39AskingXTrimVAdd(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	db := makeTestDB()
	msg := "ERR This instance has cluster support disabled"
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("ASKING")), msg)
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("READONLY")), msg)
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("READWRITE")), msg)

	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XADD", "xs", "*", "a", "1")))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "xs", "MAXLEN", "~")),
		"ERR value is not an integer or out of range")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "xs", "MINID", "~")),
		"ERR Invalid stream ID specified as stream command argument")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("VADD")),
		"ERR wrong number of arguments for 'VADD' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("VSIM")),
		"ERR wrong number of arguments for 'VSIM' command")
}
