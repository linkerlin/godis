package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps22ZCountOpenParenBorder(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("ZCOUNT", "nosuch", "(", "1"))
	asserts.AssertIntReply(t, r, 0)
	db.Exec(nil, utils.ToCmdLine("SET", "s", "v"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZCOUNT", "s", "(", "1")),
		"WRONGTYPE Operation against a key holding the wrong kind of value")
}

func TestGaps22FunctionFlushOptionWording(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("FUNCTION", "FLUSH", "FOO")),
		"ERR FUNCTION FLUSH only supports SYNC|ASYNC option")
}

func TestGaps22ClientTrackingRedirectMissing(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	srv.Exec(c, utils.ToCmdLine("HELLO", "3"))
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "TRACKING", "ON", "REDIRECT", "-1")),
		"ERR The client ID you want redirect to does not exist")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "TRACKING", "ON", "REDIRECT", "0")),
		"ERR The client ID you want redirect to does not exist")
}

func TestGaps22TSWording(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TS.CREATE", "t", "RETENTION", "abc")),
		"ERR TSDB: Couldn't parse RETENTION")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TS.ADD", "t", "*", "abc")),
		"ERR TSDB: invalid value")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TS.GET", "nosuch")),
		"ERR TSDB: the key does not exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TS.INFO", "nosuch")),
		"ERR TSDB: the key does not exist")
}

func TestGaps22ProbabilisticWording(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("CF.RESERVE", "c", "abc")),
		"Bad capacity")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("CMS.INITBYDIM", "c", "abc", "5")),
		"CMS: invalid width")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TOPK.RESERVE", "t", "abc")),
		"TopK: invalid k")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TDIGEST.CREATE", "t", "COMPRESSION", "abc")),
		"ERR T-Digest: error parsing compression parameter")
}

func TestGaps22FTSearchIndexNotFound(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "nosuch", "*")),
		"SEARCH_INDEX_NOT_FOUND Index not found: nosuch")
}

func TestGaps22VAddSpecAndFCallOrder(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("VADD", "v", "VALUES", "2", "1", "abc", "e1")),
		"ERR invalid vector specification")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("FCALL", "nosuch", "1.5")),
		"ERR Function not found")
}

func TestGaps22ConfigTimeoutWording(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "timeout", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'timeout') - argument couldn't be parsed into an integer")
}
