package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps19LSetKeyBeforeIndex(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "nosuch", "abc", "x")),
		"ERR no such key")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "nosuch", "0", "x")),
		"ERR no such key")
	db.Exec(nil, utils.ToCmdLine("SET", "s", "v"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "s", "abc", "x")),
		"WRONGTYPE Operation against a key holding the wrong kind of value")
	db.Exec(nil, utils.ToCmdLine("LPUSH", "l", "a"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "l", "abc", "x")),
		"ERR value is not an integer or out of range")
}

func TestGaps19SetExpireParseNotSyntax(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SET", "k", "v", "EX", "abc")),
		"ERR value is not an integer or out of range")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SET", "k", "v", "PX", "abc")),
		"ERR value is not an integer or out of range")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SETEX", "k", "abc", "v")),
		"ERR value is not an integer or out of range")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("PSETEX", "k", "abc", "v")),
		"ERR value is not an integer or out of range")
	db.Exec(nil, utils.ToCmdLine("SET", "gk", "v"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "gk", "EX", "abc")),
		"ERR value is not an integer or out of range")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "gk", "EX", "0")),
		"ERR invalid expire time in 'getex' command")
}

func TestGaps19ExpireUnsupportedOptionCase(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SET", "k", "v"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "k", "10", "FOO")),
		"ERR Unsupported option FOO")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "k", "10", "foo")),
		"ERR Unsupported option foo")
}

func TestGaps19TypeScanCursorBeforeMiss(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HSCAN", "nosuch", "abc")),
		"ERR invalid cursor")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SSCAN", "nosuch", "abc")),
		"ERR invalid cursor")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZSCAN", "nosuch", "abc")),
		"ERR invalid cursor")
	db.Exec(nil, utils.ToCmdLine("SET", "ht", "v"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HSCAN", "ht", "abc")),
		"ERR invalid cursor")
	r := db.Exec(nil, utils.ToCmdLine("HSCAN", "nosuch2", "0"))
	asserts.AssertMultiBulkReplySize(t, r, 2)
}

func TestGaps19ClientKillIDNonInteger(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "KILL", "ID", "abc")),
		"ERR client-id should be greater than 0")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "KILL", "ID", "-1")),
		"ERR client-id should be greater than 0")
}
