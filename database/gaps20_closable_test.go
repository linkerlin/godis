package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps20EvalNumKeys(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("EVAL", "return 1", "-1")),
		"ERR Number of keys can't be negative")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("EVAL", "return 1", "abc")),
		"ERR value is not an integer or out of range")
}

func TestGaps20GeoDistUnitBeforeMiss(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("GEODIST", "nosuch", "a", "b", "FOO")),
		"ERR unsupported unit provided. please use M, KM, FT, MI")
	asserts.AssertNullBulk(t, db.Exec(nil, utils.ToCmdLine("GEODIST", "nosuch", "a", "b")))
}

func TestGaps20ScanCountNonInteger(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SCAN", "0", "COUNT", "abc")),
		"ERR value is not an integer or out of range")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SCAN", "0", "COUNT", "0")),
		"ERR syntax error")
}

func TestGaps20HelloProtocolVersion(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("HELLO", "abc")),
		"ERR Protocol version is not an integer or out of range")
}

func TestGaps20ClientTrackingRedirect(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "TRACKING", "ON", "REDIRECT", "abc")),
		"ERR value is not an integer or out of range")
}

func TestGaps20ClientPauseTimeout(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "PAUSE", "abc")),
		"ERR timeout is not an integer or out of range")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "PAUSE", "-1")),
		"ERR timeout is negative")
}

func TestGaps20SlowlogGetCount(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("SLOWLOG", "GET", "abc")),
		"ERR count should be greater than or equal to -1")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("SLOWLOG", "GET", "-2")),
		"ERR count should be greater than or equal to -1")
	r := srv.Exec(c, utils.ToCmdLine("SLOWLOG", "GET", "-1"))
	asserts.AssertMultiBulkReplySize(t, r, 0)
}

func TestGaps20CopyDBNonInteger(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("COPY", "a", "b", "DB", "abc")),
		"ERR value is not an integer or out of range")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("COPY", "a", "a")),
		"ERR source and destination objects are the same")
}
