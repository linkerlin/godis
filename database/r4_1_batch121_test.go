package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 121 R4-1 extras: RPOP COUNT 2, LMOVE LEFT RIGHT, LINSERT AFTER,
// LPOS RANK 2 / MAXLEN, LMPOP RIGHT COUNT 1, BLMOVE RIGHT LEFT, RPOPLPUSH,
// LTRIM/RPUSHX, ZRANGE BYSCORE LIMIT WS, ZRANGESTORE BYLEX, ZREVRANGEBYLEX,
// ZPOPMIN, ZUNIONSTORE SUM WEIGHTS, ZINTER MIN WS, ZDIFFSTORE, ZMPOP MAX,
// BZPOPMIN, ZADD NX/XX/CH, BITOP AND/XOR/ONE, BITFIELD u8 WRAP, GETEX EX,
// KEEPTTL, GETEX PERSIST, HEXPIRE XX/NX/GT, HPEXPIRE GT, HGETEX EX/PERSIST,
// SDIFFSTORE/SINTERSTORE, XDEL/XTRIM MINID, GEOSEARCH BYBOX, SORT DESC LIMIT,
// LCS, PFMERGE.
func TestR41Batch121Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b121l", "a", "b", "c", "d", "e", "f")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b121l", "2")), []string{"f", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b121l", "0", "-1")), []string{"a", "b", "c", "d"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b121l", "b121l2", "LEFT", "RIGHT")), "a")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b121l", "0", "-1")), []string{"b", "c", "d"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b121l2", "0", "-1")), []string{"a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b121ins", "a", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b121ins", "AFTER", "a", "b")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b121ins", "0", "-1")), []string{"a", "b", "c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b121lp", "a", "a", "b", "a", "c")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b121lp", "a", "RANK", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b121lp", "a", "MAXLEN", "2")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b121lmp", "1", "2", "3", "4")), 4)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b121lmp", "RIGHT", "COUNT", "1"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "4") {
		t.Fatalf("LMPOP RIGHT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b121lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b121bl", "a", "b", "c")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b121bl", "b121bld", "RIGHT", "LEFT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "c") {
		t.Fatalf("BLMOVE RIGHT LEFT: %s", bl.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOPLPUSH", "b121bl", "b121bld")), "b")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b121bl", "0", "-1")), []string{"a"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b121bld", "0", "-1")), []string{"b", "c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b121tr", "a", "b", "c", "d", "e")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b121tr", "1", "3")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b121tr", "0", "-1")), []string{"b", "c", "d"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b121tr", "z")), 4)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b121zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b121zs", "1", "5", "BYSCORE", "WITHSCORES", "LIMIT", "1", "2")),
		[]string{"b", "2", "c", "3"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGE", "b121zs", "0", "1", "WITHSCORES")),
		[]string{"e", "5", "d", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b121zlex", "0", "a", "0", "b", "0", "c", "0", "d")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b121zrs", "b121zlex", "[a", "(d", "BYLEX")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b121zrs", "0", "-1")), []string{"a", "b", "c"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYLEX", "b121zlex", "(d", "[b")), []string{"c", "b"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCOUNT", "b121zs", "(2", "5")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "b121zs")), []string{"a", "1"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b121zu", "1", "b121zs", "WEIGHTS", "2", "AGGREGATE", "SUM")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b121zu", "0", "-1", "WITHSCORES")),
		[]string{"b", "4", "c", "6", "d", "8", "e", "10"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b121z1", "1", "a", "3", "b", "2", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b121z2", "4", "b", "8", "c", "1", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b121z1", "b121z2", "WEIGHTS", "1", "1", "AGGREGATE", "MIN", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER MIN: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFFSTORE", "b121zd", "2", "b121z1", "b121z2")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b121zd", "0", "-1")), []string{"a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b121z1", "b121z2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b121zm", "1", "a", "6", "f", "3", "c")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b121zm", "MAX", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "f") {
		t.Fatalf("ZMPOP MAX: %s", zmp.ToBytes())
	}
	bz := db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "b121zm", "0"))
	if protocol.IsErrorReply(bz) || !strings.Contains(string(bz.ToBytes()), "a") {
		t.Fatalf("BZPOPMIN: %s", bz.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b121zm", "NX", "2", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b121zm", "XX", "CH", "9", "c")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b121zm", "c")), "9")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b121b1", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b121b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b121b2", "4", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "AND", "b121ba", "b121b1", "b121b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b121ba")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b121bx", "b121b1", "b121b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b121bx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ONE", "b121bone", "b121b1", "b121b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b121bone")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b121b2", "4")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b121bf", "OVERFLOW", "WRAP", "SET", "u8", "0", "200", "GET", "u8", "0"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "200") {
		t.Fatalf("BITFIELD u8 WRAP: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b121st", "v", "EX", "50")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b121st", "EX", "80")), "v")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b121st", "w", "KEEPTTL")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b121st")), "w")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b121st", "PERSIST")), "w")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b121st")), -1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b121sr", "Hi")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b121sr", "0", "OK")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b121sr", "0", "1")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCRBY", "b121dn", "5")), 5)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b121dn", "0.25")), "5.25")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b121m1", "1")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b121m1", "9", "b121m3", "3")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b121st", "20", "NX")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b121ex", "v", "EX", "10")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b121ex", "40", "XX")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b121h", "f1", "v1", "f2", "7", "f3", "zz")), 3)
	hxx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b121h", "50", "XX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hxx) || !strings.Contains(string(hxx.ToBytes()), "0") {
		t.Fatalf("HEXPIRE XX: %s", hxx.ToBytes())
	}
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b121h", "60", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b121h", "90", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	hpg := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b121h", "200000", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hpg) || !strings.Contains(string(hpg.ToBytes()), "1") {
		t.Fatalf("HPEXPIRE GT: %s", hpg.ToBytes())
	}
	he := db.Exec(nil, utils.ToCmdLine("HGETEX", "b121h", "EX", "40", "FIELDS", "1", "f2"))
	if protocol.IsErrorReply(he) || !strings.Contains(string(he.ToBytes()), "7") {
		t.Fatalf("HGETEX EX: %s", he.ToBytes())
	}
	hp := db.Exec(nil, utils.ToCmdLine("HGETEX", "b121h", "PERSIST", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(hp) || !strings.Contains(string(hp.ToBytes()), "zz") {
		t.Fatalf("HGETEX PERSIST: %s", hp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b121h", "f2", "3")), 10)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSTRLEN", "b121h", "f1")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b121h")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b121sa", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b121sb", "c", "d")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b121sd", "b121sa", "b121sb")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b121sd")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b121ss", "b121sa", "b121sb")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUNION", "b121ss")), []string{"c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SISMEMBER", "b121sa", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b121sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b121sp")), "only")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b121sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b121x", "1-0", "k", "v")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b121x", "2-0", "k", "w")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b121x", "4-0", "k", "x")), "4-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b121x", "2-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b121x", "MINID", "4-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b121x")), 1)
	x1 := db.Exec(nil, utils.ToCmdLine("XRANGE", "b121x", "-", "+", "COUNT", "1"))
	if protocol.IsErrorReply(x1) || !strings.Contains(string(x1.ToBytes()), "4-0") {
		t.Fatalf("XRANGE: %s", x1.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b121g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b121g", "FROMLONLAT", "15", "37.5", "BYBOX", "100", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH BYBOX: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b121s1", "abcxyz")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b121s2", "abqxyz")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b121s1", "b121s2")), "abxyz")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b121s1", "b121s2", "LEN")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b121sort", "4", "1", "3", "2")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b121sort", "DESC", "LIMIT", "0", "2")), []string{"4", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b121p1", "m")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b121p2", "n")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b121pm", "b121p1", "b121p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b121pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b121s1", "b121s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b121s2", "b121s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p121")), "p121")
}
