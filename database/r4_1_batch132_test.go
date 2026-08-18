package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 132 R4-1 extras: LPOP COUNT 3, LMOVE RIGHT LEFT, LINSERT BEFORE,
// LPOS COUNT 0 / RANK -1 / COUNT RANK, LMPOP LEFT COUNT 1, BLMOVE LEFT RIGHT,
// LSET/LREM/LTRIM/LPUSHX, ZRANGE BYSCORE LIMIT WS, ZRANGESTORE BYSCORE,
// ZREVRANGEBYSCORE, ZPOPMAX, ZUNIONSTORE MAX WEIGHTS, ZRANGE BYLEX,
// ZREVRANGEBYLEX, ZREMRANGEBYLEX, ZINTER MAX WS, ZINTERSTORE MAX, ZDIFF WS,
// ZMPOP MIN, ZADD GT/LT/INCR, BITOP OR/NOT/DIFF, BITFIELD i8 SAT,
// GETEX PX/PERSIST, HEXPIRE NX/LT, HPEXPIRE XX, HGETEX PX/PERSIST,
// SUNIONSTORE/SINTERSTORE, XTRIM MAXLEN, GEOSEARCH BYRADIUS/GEORADIUS,
// SORT ALPHA, LCS, PFMERGE.
func TestR41Batch132Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b132l", "3", "5", "7", "9", "11", "13")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b132l", "3")), []string{"3", "5", "7"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b132l", "0", "-1")), []string{"9", "11", "13"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b132l", "b132l2", "RIGHT", "LEFT")), "13")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b132l", "0", "-1")), []string{"9", "11"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b132l2", "0", "-1")), []string{"13"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b132ins", "n", "p")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b132ins", "BEFORE", "p", "o")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b132ins", "0", "-1")), []string{"n", "o", "p"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b132lp", "y", "w", "y", "w", "y")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b132lp", "y", "COUNT", "0")), []string{"0", "2", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b132lp", "y", "RANK", "-1")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b132lp", "y", "COUNT", "2", "RANK", "2")), []string{"2", "4"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b132lmp", "a", "b", "c", "d")), 4)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b132lmp", "LEFT", "COUNT", "1"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "a") {
		t.Fatalf("LMPOP LEFT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b132lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b132bl", "m", "n", "o")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b132bl", "b132bld", "LEFT", "RIGHT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "m") {
		t.Fatalf("BLMOVE LEFT RIGHT: %s", bl.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b132bl", "0", "-1")), []string{"n", "o"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b132bld", "0", "-1")), []string{"m"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b132lt", "9", "8", "7", "6", "5")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b132lt", "0", "A")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b132lt", "1", "6")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b132lt", "0", "2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b132lt", "0", "-1")), []string{"A", "8", "7"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b132lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b132lx", "q")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b132lx", "z")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b132lx", "y")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b132zs", "1", "a", "5", "b", "9", "c", "13", "d", "17", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b132zs", "5", "13", "BYSCORE", "WITHSCORES", "LIMIT", "1", "2")),
		[]string{"c", "9", "d", "13"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b132zrs", "b132zs", "5", "17", "BYSCORE")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b132zrs", "0", "-1")), []string{"b", "c", "d", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b132zs", "13", "5", "WITHSCORES", "LIMIT", "0", "2")),
		[]string{"d", "13", "c", "9"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b132zs")), []string{"e", "17"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b132zu", "1", "b132zs", "WEIGHTS", "2", "AGGREGATE", "MAX")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b132zu", "0", "-1", "WITHSCORES")),
		[]string{"a", "2", "b", "10", "c", "18", "d", "26"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b132zlex", "0", "e", "0", "f", "0", "g", "0", "h")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b132zlex", "[e", "(h", "BYLEX")), []string{"e", "f", "g"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b132zrs2", "b132zlex", "[f", "(h", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYLEX", "b132zlex", "(h", "[f")), []string{"g", "f"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b132zlex", "[e", "(g")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b132zlex", "0", "-1")), []string{"g", "h"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b132z1", "4", "a", "8", "b", "6", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b132z2", "3", "b", "1", "c", "7", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b132z1", "b132z2", "WEIGHTS", "1", "1", "AGGREGATE", "MAX", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER MAX: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b132zi", "2", "b132z1", "b132z2", "WEIGHTS", "2", "1", "AGGREGATE", "MAX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b132zi", "0", "-1", "WITHSCORES")),
		[]string{"c", "12", "b", "16"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b132z1", "b132z2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b132zm", "6", "a", "15", "d", "9", "b")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b132zm", "MIN", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "a") {
		t.Fatalf("ZMPOP MIN: %s", zmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b132zm", "GT", "20", "d")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b132zm", "LT", "4", "b")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b132zm", "INCR", "1.5", "b")), "5.5")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b132zm", "d")), "20")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b132b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b132b2", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b132b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b132bo", "b132b1", "b132b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b132bo")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "NOT", "b132bn", "b132b1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b132bn")), 7)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "DIFF", "b132bd", "b132b2", "b132b1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b132bd")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b132bf", "OVERFLOW", "SAT", "INCRBY", "i8", "0", "200"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "127") {
		t.Fatalf("BITFIELD i8 SAT: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b132st", "v", "PX", "80000")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b132st", "PX", "90000")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b132st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b132st")), -1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b132h", "f1", "aa", "f2", "6", "f3", "zz")), 3)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b132h", "50", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hlt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b132h", "20", "LT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hlt) || !strings.Contains(string(hlt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE LT: %s", hlt.ToBytes())
	}
	hxx := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b132h", "90000", "XX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hxx) || !strings.Contains(string(hxx.ToBytes()), "1") {
		t.Fatalf("HPEXPIRE XX: %s", hxx.ToBytes())
	}
	he := db.Exec(nil, utils.ToCmdLine("HGETEX", "b132h", "PX", "40000", "FIELDS", "1", "f2"))
	if protocol.IsErrorReply(he) || !strings.Contains(string(he.ToBytes()), "6") {
		t.Fatalf("HGETEX PX: %s", he.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b132h", "f2", "4")), 10)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b132h")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b132sa", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b132sb", "c", "d")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SUNIONSTORE", "b132su", "b132sa", "b132sb")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b132ss", "b132sa", "b132sb")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUNION", "b132ss")), []string{"c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b132sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b132sp")), "only")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b132sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b132x", "1-0", "k", "v")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b132x", "2-0", "k", "w")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b132x", "3-0", "k", "x")), "3-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b132x", "MAXLEN", "1")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b132x")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b132g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b132g", "FROMLONLAT", "15", "37.5", "BYRADIUS", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH BYRADIUS: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b132s1", "orange")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b132s2", "range")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b132s1", "b132s2")), "range")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b132s1", "b132s2", "LEN")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b132sort", "mango", "apple", "peach")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b132sort", "ALPHA")), []string{"apple", "mango", "peach"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b132p1", "m")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b132p2", "n")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b132pm", "b132p1", "b132p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b132pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b132s1", "b132s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b132s2", "b132s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p132")), "p132")
}
