package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 108 R4-1 extras: LPOP COUNT, LMOVE RIGHT RIGHT, LINSERT AFTER, LPOS MAXLEN,
// LMPOP RIGHT, BLMOVE RIGHT LEFT, ZREVRANGEBYSCORE WS LIMIT, ZINTERSTORE SUM WEIGHTS,
// ZUNION MIN WS, ZRANGESTORE BYLEX, ZDIFF WS, BITOP ONE/XOR, BITFIELD u8 SAT,
// GETEX EX, SET GET/KEEPTTL, HEXPIRE LT, HPEXPIRE NX, HGETEX PX, SUNIONSTORE,
// XTRIM MAXLEN, GEORADIUS, SORT LIMIT, ZADD GT/LT, LCS LEN, MSETNX, RPOPLPUSH.
func TestR41Batch108Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b108l", "a", "b", "c", "d", "e", "f")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b108l", "2")), []string{"a", "b"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b108l", "0", "-1")), []string{"c", "d", "e", "f"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b108l", "b108l", "RIGHT", "RIGHT")), "f")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b108l", "d", "MAXLEN", "3")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b108ins", "a", "c", "e")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b108ins", "AFTER", "c", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b108ins", "0", "-1")), []string{"a", "c", "d", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b108lp", "1", "2", "3", "4", "5")), 5)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b108lp", "RIGHT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "5") {
		t.Fatalf("LMPOP RIGHT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b108lp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b108bl", "a", "b", "c")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b108bl", "b108bld", "RIGHT", "LEFT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "c") {
		t.Fatalf("BLMOVE: %s", bl.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOPLPUSH", "b108bl", "b108bld")), "b")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b108bld", "0", "-1")), []string{"b", "c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b108lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b108lx", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b108lx", "b")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b108z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b108z", "5", "1", "WITHSCORES", "LIMIT", "1", "2")),
		[]string{"d", "4", "c", "3"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b108z", "0", "2", "REV")), []string{"e", "d", "c"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "b108z")), []string{"a", "1"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYRANK", "b108z", "0", "0")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b108z", "0", "-1")), []string{"c", "d", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b108z1", "2", "a", "4", "b", "6", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b108z2", "1", "a", "8", "b", "3", "d")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b108zi", "2", "b108z1", "b108z2", "WEIGHTS", "1", "2", "AGGREGATE", "SUM")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b108zi", "0", "-1", "WITHSCORES")), []string{"a", "4", "b", "20"})
	zu := db.Exec(nil, utils.ToCmdLine("ZUNION", "2", "b108z1", "b108z2", "WEIGHTS", "1", "1", "AGGREGATE", "MIN", "WITHSCORES"))
	if protocol.IsErrorReply(zu) || !strings.Contains(string(zu.ToBytes()), "d") {
		t.Fatalf("ZUNION MIN: %s", zu.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b108zrs", "b108z1", "(a", "[c", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b108zrs", "0", "-1")), []string{"b", "c"})
	zd := db.Exec(nil, utils.ToCmdLine("ZDIFF", "2", "b108z1", "b108z2", "WITHSCORES"))
	if protocol.IsErrorReply(zd) || !strings.Contains(string(zd.ToBytes()), "c") {
		t.Fatalf("ZDIFF WS: %s", zd.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b108za", "5", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b108za", "GT", "3", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b108za", "LT", "3", "a")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b108za", "a")), "3")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINCRBY", "b108za", "1.5", "a")), "4.5")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b108b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b108b1", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b108b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ONE", "b108bo", "b108b1", "b108b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b108bo")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b108bx", "b108b1", "b108b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b108bx")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b108bf", "OVERFLOW", "SAT", "INCRBY", "u8", "0", "300"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "255") {
		t.Fatalf("BITFIELD SAT u8: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b108st", "v")), "OK")
	sg := db.Exec(nil, utils.ToCmdLine("SET", "b108st", "w", "GET"))
	if protocol.IsErrorReply(sg) || !strings.Contains(string(sg.ToBytes()), "v") {
		t.Fatalf("SET GET: %s", sg.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b108st", "EX", "40")), "w")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b108st", "x", "KEEPTTL")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETDEL", "b108st")), "x")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b108nx", "v", "NX")), "OK")
	nx := db.Exec(nil, utils.ToCmdLine("SET", "b108nx", "w", "NX", "GET"))
	if protocol.IsErrorReply(nx) || !strings.Contains(string(nx.ToBytes()), "v") {
		t.Fatalf("SET NX GET exist: %s", nx.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b108m1", "1", "b108m2", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b108m1", "9", "b108m3", "3")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b108h", "a", "10", "b", "20")), 2)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b108h", "90", "NX", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hlt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b108h", "30", "LT", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hlt) || !strings.Contains(string(hlt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE LT: %s", hlt.ToBytes())
	}
	hp := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b108h", "8000", "NX", "FIELDS", "1", "b"))
	if protocol.IsErrorReply(hp) || !strings.Contains(string(hp.ToBytes()), "1") {
		t.Fatalf("HPEXPIRE NX: %s", hp.ToBytes())
	}
	hg := db.Exec(nil, utils.ToCmdLine("HGETEX", "b108h", "PX", "4000", "FIELDS", "1", "b"))
	if protocol.IsErrorReply(hg) || !strings.Contains(string(hg.ToBytes()), "20") {
		t.Fatalf("HGETEX PX: %s", hg.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b108h", "a", "5")), 15)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("HMSET", "b108h", "c", "30")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HEXISTS", "b108h", "c")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b108sa", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b108sb", "b", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SUNIONSTORE", "b108su", "b108sa", "b108sb")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b108si", "b108sa", "b108sb")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SISMEMBER", "b108si", "b")), 1)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b108x", "1-0", "f", "a")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b108x", "2-0", "f", "b")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b108x", "3-0", "f", "c")), "3-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b108x", "MAXLEN", "2")), 1)
	xr := db.Exec(nil, utils.ToCmdLine("XRANGE", "b108x", "-", "+", "COUNT", "1"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "2-0") {
		t.Fatalf("XRANGE: %s", xr.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b108g", "15.087269", "37.502669", "Catania")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEORADIUS", "b108g", "15", "37", "80", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Catania") {
		t.Fatalf("GEORADIUS: %s", geo.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b108s1", "abcde")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b108s2", "abxyz")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b108s1", "b108s2", "LEN")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b108s1", "-3", "-1")), "cde")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUBSTR", "b108s1", "1", "3")), "bcd")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b108s1", "0", "XY")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECR", "b108n")), -1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCRBY", "b108n", "5")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b108pf", "a", "b", "c")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b108sort", "9", "1", "5", "3")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b108sort", "LIMIT", "1", "2")), []string{"3", "5"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b108s2", "b108s1")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p108")), "p108")
}
