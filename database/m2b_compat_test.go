package database

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2ZAddOptions(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "z", "NX", "2", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "z", "XX", "CH", "3", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "z", "GT", "2", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "z", "CH", "GT", "6", "a")), 1)
	incr := db.Exec(nil, utils.ToCmdLine("ZADD", "z", "INCR", "1", "a"))
	asserts.AssertBulkReply(t, incr, "7")
}

func TestM2ZRangeUnified(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1", "a", "2", "b", "3", "c"))
	rev := db.Exec(nil, utils.ToCmdLine("ZRANGE", "z", "0", "-1", "REV"))
	mb := rev.(*protocol.MultiBulkReply)
	if len(mb.Args) != 3 || string(mb.Args[0]) != "c" {
		t.Fatalf("REV: %s", rev.ToBytes())
	}
	byScore := db.Exec(nil, utils.ToCmdLine("ZRANGE", "z", "1", "2", "BYSCORE", "WITHSCORES"))
	smb := byScore.(*protocol.MultiBulkReply)
	if len(smb.Args) != 4 {
		t.Fatalf("BYSCORE WITHSCORES: %s", byScore.ToBytes())
	}
}

func TestM2MSetClearsTTL(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SET", "k", "v", "EX", "100"))
	db.Exec(nil, utils.ToCmdLine("MSET", "k", "v2"))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "k")), -1)
}

func TestM2GetRangeEmpty(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SET", "k", "abc"))
	r := db.Exec(nil, utils.ToCmdLine("GETRANGE", "k", "5", "10"))
	asserts.AssertBulkReply(t, r, "")
}

func TestM2IncrOverflow(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SET", "k", strconv.FormatInt(math.MaxInt64, 10)))
	r := db.Exec(nil, utils.ToCmdLine("INCR", "k"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("expected overflow, got %s", r.ToBytes())
	}
}

func TestM2SyntaxErrPrefix(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("SET", "k", "v", "BADOPT"))
	if !strings.Contains(string(r.ToBytes()), "-ERR syntax error") {
		t.Fatalf("got %s", r.ToBytes())
	}
}
