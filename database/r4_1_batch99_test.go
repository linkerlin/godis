package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 99 R4-1 extras: LINSERT/LTRIM/BLMOVE, ZLEX*, ZUNIONSTORE WEIGHTS+SUM,
// ZINTERSTORE MIN, BITOP XOR/NOT, LCS, GETSET, SETEX/PSETEX, HEXPIRE, single
// HRANDFIELD/SPOP/ZRANDMEMBER, ZPOPMAX, ZREMRANGEBYRANK, SORT numeric, SMOVE, MSETNX.
func TestR41Batch99Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b99l", "w", "x", "y", "z")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b99l", "BEFORE", "x", "X")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b99l", "1", "-2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b99l", "0", "-1")), []string{"X", "x", "y"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("BLMOVE", "b99l", "b99l2", "LEFT", "RIGHT", "0")), "X")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b99zlex", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYLEX", "b99zlex", "[b", "[d")), []string{"b", "c", "d"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b99zlex", "(a", "[d")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b99zlex", "[a", "[b")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b99z1", "1", "a", "3", "b", "5", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b99z2", "2", "b", "4", "d")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b99zu", "2", "b99z1", "b99z2", "WEIGHTS", "1", "2", "AGGREGATE", "SUM")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b99zu", "0", "-1", "WITHSCORES")),
		[]string{"a", "1", "c", "5", "b", "7", "d", "8"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b99zi", "2", "b99z1", "b99z2", "AGGREGATE", "MIN")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b99zi", "0", "-1", "WITHSCORES")), []string{"b", "2"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b99b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b99b1", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b99b2", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b99b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b99bx", "b99b1", "b99b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b99bx")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "NOT", "b99bn", "b99b1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b99bn", "0")), 0)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b99lcs1", "abcxyz")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b99lcs2", "abcwxy")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b99lcs1", "b99lcs2", "LEN")), 5)

	gs := db.Exec(nil, utils.ToCmdLine("GETSET", "b99gs", "old"))
	if _, ok := gs.(*protocol.NullBulkReply); !ok {
		t.Fatalf("GETSET miss: %T %s", gs, gs.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b99gs", "old")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b99gs", "new")), "old")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SETEX", "b99sx", "90", "v")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b99sx", "b99sx2")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PSETEX", "b99ps", "6000", "w")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PERSIST", "b99ps")), 1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("HMSET", "b99h", "f1", "aa", "f3", "cc")), "OK")
	hexp := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b99h", "70", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hexp) {
		t.Fatalf("HEXPIRE: %s", hexp.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b99h1", "only", "one")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "b99h1")), "only")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b99s1", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b99s1")), "only")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b99zr", "1", "a")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b99zr")), []string{"a", "1"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b99zc", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYRANK", "b99zc", "1", "3")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b99zc", "0", "-1")), []string{"a", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECRBY", "b99dec", "2")), -2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b99dec", "20")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECRBY", "b99dec", "5")), 15)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b99x", "1-0", "a", "1")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b99x", "2-0", "a", "2")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b99x", "3-0", "a", "3")), "3-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b99x", "MAXLEN", "2")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b99sort", "9", "1", "5")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b99sort")), []string{"1", "5", "9"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b99lx", "z")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b99lx", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b99lx", "b")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SMOVE", "b99sm1", "b99sm2", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b99sm1", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b99sm2", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SMOVE", "b99sm1", "b99sm2", "a")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b99m1", "1", "b99m2", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b99m1", "x", "b99m3", "y")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PEXPIREAT", "b99sx2", "4102444800000")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "b99")), "b99")
}
