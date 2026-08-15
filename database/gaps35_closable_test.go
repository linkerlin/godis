package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps35ConfigRanges(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-samples", "0")),
		"ERR CONFIG SET failed (possibly related to argument 'maxmemory-samples') - argument must be between 1 and 64 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-samples", "65")),
		"ERR CONFIG SET failed (possibly related to argument 'maxmemory-samples') - argument must be between 1 and 64 inclusive")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "timeout", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'timeout') - argument must be between 0 and 2147483647 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tcp-keepalive", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'tcp-keepalive') - argument must be between 0 and 2147483647 inclusive")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "busy-reply-threshold", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'busy-reply-threshold') - argument must be between 0 and 9223372036854775807 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "acllog-max-len", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'acllog-max-len') - argument must be between 0 and 9223372036854775807 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "hash-max-listpack-entries", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'hash-max-listpack-entries') - argument must be between 0 and 9223372036854775807 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "set-max-listpack-entries", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'set-max-listpack-entries') - argument must be between 0 and 9223372036854775807 inclusive")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "proto-max-bulk-len", "0")),
		"ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - argument must be between 1048576 and 9223372036854775807 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "client-query-buffer-limit", "0")),
		"ERR CONFIG SET failed (possibly related to argument 'client-query-buffer-limit') - argument must be between 1048576 and 9223372036854775807 inclusive")
}

func TestGaps35StreamClientLPos(t *testing.T) {
	db := makeTestDB()
	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XADD", "xs", "*", "a", "1")))
	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "xs", "g", "0")))

	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XCLAIM", "xs", "g", "c", "0", "0-0", "LASTID")),
		"ERR Unrecognized XCLAIM option 'LASTID'")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XCLAIM", "xs", "g", "c", "0", "0-0", "LASTID", "abc")),
		"ERR Invalid stream ID specified as stream command argument")

	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "xs", "g2", "$", "ENTRIESREAD", "-2")),
		"ERR value for ENTRIESREAD must be positive or -1")
	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "xs", "g3", "$", "ENTRIESREAD", "0")))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "SETID", "xs", "g", "$", "ENTRIESREAD", "-2")),
		"ERR value for ENTRIESREAD must be positive or -1")

	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "l", "a", "COUNT", "abc")),
		"ERR COUNT can't be negative")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "l", "a", "MAXLEN", "abc")),
		"ERR MAXLEN can't be negative")

	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "UNBLOCK", "1", "FOO")),
		"ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR")
}
