package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps25ConfigWording(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tcp-keepalive", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'tcp-keepalive') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "lua-time-limit", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'lua-time-limit') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "loglevel", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'loglevel') - argument(s) must be one of the following: debug, verbose, notice, warning, nothing")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "appendfsync", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'appendfsync') - argument(s) must be one of the following: everysec, always, no")
}

func TestGaps25ScriptHelloLolwut(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("SCRIPT", "FLUSH", "FOO")),
		"ERR SCRIPT FLUSH only support SYNC|ASYNC option")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("LOLWUT", "VERSION", "abc")),
		"ERR value is not an integer or out of range")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("HELLO", "2", "SETNAME")),
		"ERR Syntax error in HELLO option 'SETNAME'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("HELLO", "2", "AUTH")),
		"ERR Syntax error in HELLO option 'AUTH'")
}

func TestGaps25IntercardLimitAndProbMiss(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "1", "a", "LIMIT", "abc")),
		"ERR LIMIT can't be negative")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "1", "a", "LIMIT", "abc")),
		"ERR LIMIT can't be negative")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TOPK.QUERY", "nosuch", "x")),
		"TopK: key does not exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TOPK.LIST", "nosuch")),
		"TopK: key does not exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("CMS.QUERY", "nosuch", "x")),
		"CMS: key does not exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("CMS.INFO", "nosuch")),
		"CMS: key does not exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TDIGEST.RANK", "nosuch", "1")),
		"ERR T-Digest: key does not exist")
}

func TestGaps25JSONAndTSMiss(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("JSON.OBJKEYS", "nosuch", "$")),
		"ERR could not perform this operation on a key that doesn't exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("JSON.OBJLEN", "nosuch", "$")),
		"ERR Path does not exist or not an object")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("JSON.ARRLEN", "nosuch", "$")),
		"ERR could not perform this operation on a key that doesn't exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("JSON.STRLEN", "nosuch", "$")),
		"ERR could not perform this operation on a key that doesn't exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("JSON.CLEAR", "nosuch", "$")),
		"ERR could not perform this operation on a key that doesn't exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TS.RANGE", "nosuch", "-", "+")),
		"ERR TSDB: the key does not exist")
}
