package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 111 R4-1 extras: RPOP COUNT, LMOVE LEFT LEFT, LINSERT AFTER, LPOS MAXLEN,
// LMPOP RIGHT, BLMOVE RIGHT RIGHT, LTRIM/LREM -1, ZREVRANGEBYSCORE WS LIMIT,
// ZRANGE REV, ZINTERSTORE MIN WEIGHTS, ZUNION MAX WS, ZRANGESTORE BYLEX,
// ZMPOP MIN, ZADD GT/LT, BITOP XOR/NOT, BITFIELD i8 SAT, GETEX EX, SET GET,
// KEEPTTL, HEXPIRE LT, HPEXPIRE NX, HGETEX PX, SUNIONSTORE, XTRIM MAXLEN,
// GEORADIUS, GEOSEARCH FROMMEMBER, SORT LIMIT, LCS.
func TestR41Batch111Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b111l", "a", "b", "c", "d", "e", "f")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b111l", "1")), []string{"f"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b111l", "0", "-1")), []string{"a", "b", "c", "d", "e"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b111l", "b111l2", "LEFT", "LEFT")), "a")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b111l", "0", "-1")), []string{"b", "c", "d", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b111l2", "0", "-1")), []string{"a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b111ins", "a", "c", "e")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b111ins", "AFTER", "c", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b111ins", "0", "-1")), []string{"a", "c", "d", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b111lp", "x", "y", "z", "y", "x")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b111lp", "y", "MAXLEN", "3")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b111lp", "x", "RANK", "-1")), 4)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b111lmp", "1", "2", "3", "4", "5")), 5)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b111lmp", "RIGHT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "5") {
		t.Fatalf("LMPOP RIGHT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b111lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b111bl", "a", "b", "c")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b111bl", "b111bld", "RIGHT", "RIGHT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "c") {
		t.Fatalf("BLMOVE RIGHT RIGHT: %s", bl.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b111bld", "0", "-1")), []string{"c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b111lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b111lx", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b111lx", "z")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b111lx", "0", "-1")), []string{"z", "a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b111lt", "a", "b", "c", "d", "e")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b111lt", "0", "-2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b111lt", "0", "-1")), []string{"a", "b", "c", "d"})
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b111lt", "-1", "X")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b111lt", "-1")), "X")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b111lt", "-1", "X")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b111lt", "0", "-1")), []string{"a", "b", "c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b111z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b111z", "5", "1", "WITHSCORES", "LIMIT", "1", "2")),
		[]string{"d", "4", "c", "3"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b111z", "0", "2", "REV")), []string{"e", "d", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCOUNT", "b111z", "2", "4")), 3)
	zm := db.Exec(nil, utils.ToCmdLine("ZMSCORE", "b111z", "a", "missing", "e"))
	if protocol.IsErrorReply(zm) || !strings.Contains(string(zm.ToBytes()), "1") {
		t.Fatalf("ZMSCORE: %s", zm.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANK", "b111z", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANK", "b111z", "b")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b111z")), []string{"e", "5"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYSCORE", "b111z", "0", "2")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b111z", "0", "-1")), []string{"c", "d"})
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b111z", "MIN", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "c") {
		t.Fatalf("ZMPOP MIN: %s", zmp.ToBytes())
	}
	bz := db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "b111z", "0"))
	if protocol.IsErrorReply(bz) || !strings.Contains(string(bz.ToBytes()), "d") {
		t.Fatalf("BZPOPMIN: %s", bz.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b111z1", "3", "a", "5", "b", "7", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b111z2", "4", "a", "1", "b", "9", "d")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b111zi", "2", "b111z1", "b111z2", "WEIGHTS", "1", "2", "AGGREGATE", "MIN")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b111zi", "0", "-1", "WITHSCORES")), []string{"b", "2", "a", "3"})
	zu := db.Exec(nil, utils.ToCmdLine("ZUNION", "2", "b111z1", "b111z2", "WEIGHTS", "1", "1", "AGGREGATE", "MAX", "WITHSCORES"))
	if protocol.IsErrorReply(zu) || !strings.Contains(string(zu.ToBytes()), "d") {
		t.Fatalf("ZUNION MAX: %s", zu.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b111zrs", "b111z1", "(a", "[c", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b111zrs", "0", "-1")), []string{"b", "c"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFF", "2", "b111z1", "b111z2")), []string{"c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b111z1", "b111z2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b111za", "5", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b111za", "GT", "8", "a")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b111za", "a")), "8")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b111za", "LT", "6", "a")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b111za", "a")), "6")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINCRBY", "b111za", "1.5", "a")), "7.5")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b111b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b111b1", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b111b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b111bx", "b111b1", "b111b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b111bx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "NOT", "b111bn", "b111b1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b111bn", "0")), 0)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b111bf", "OVERFLOW", "SAT", "INCRBY", "i8", "0", "200"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "127") {
		t.Fatalf("BITFIELD SAT i8: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b111st", "v")), "OK")
	sg := db.Exec(nil, utils.ToCmdLine("SET", "b111st", "w", "GET"))
	if protocol.IsErrorReply(sg) || !strings.Contains(string(sg.ToBytes()), "v") {
		t.Fatalf("SET GET: %s", sg.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b111st", "EX", "40")), "w")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b111st", "x", "KEEPTTL")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETDEL", "b111st")), "x")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b111nx", "v", "NX")), "OK")
	nx := db.Exec(nil, utils.ToCmdLine("SET", "b111nx", "w", "NX", "GET"))
	if protocol.IsErrorReply(nx) || !strings.Contains(string(nx.ToBytes()), "v") {
		t.Fatalf("SET NX GET: %s", nx.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b111m1", "1", "b111m2", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b111m1", "9", "b111m3", "3")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCR", "b111n")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECR", "b111n")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCRBY", "b111n", "5")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECRBY", "b111n", "2")), 3)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PSETEX", "b111sx", "5000", "y")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PEXPIRE", "b111sx", "3000", "XX")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PERSIST", "b111sx")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b111h", "a", "10", "b", "20")), 2)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b111h", "90", "NX", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hlt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b111h", "30", "LT", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hlt) || !strings.Contains(string(hlt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE LT: %s", hlt.ToBytes())
	}
	hp := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b111h", "8000", "NX", "FIELDS", "1", "b"))
	if protocol.IsErrorReply(hp) || !strings.Contains(string(hp.ToBytes()), "1") {
		t.Fatalf("HPEXPIRE NX: %s", hp.ToBytes())
	}
	hg := db.Exec(nil, utils.ToCmdLine("HGETEX", "b111h", "PX", "4000", "FIELDS", "1", "b"))
	if protocol.IsErrorReply(hg) || !strings.Contains(string(hg.ToBytes()), "20") {
		t.Fatalf("HGETEX PX: %s", hg.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBYFLOAT", "b111h", "a", "0.5")), "10.5")
	hd := db.Exec(nil, utils.ToCmdLine("HGETDEL", "b111h", "FIELDS", "1", "b"))
	if protocol.IsErrorReply(hd) || !strings.Contains(string(hd.ToBytes()), "20") {
		t.Fatalf("HGETDEL: %s", hd.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("HMSET", "b111h", "c", "30")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSTRLEN", "b111h", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b111hk", "f", "v")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HKEYS", "b111hk")), []string{"f"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "b111hk")), "f")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b111sa", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b111sb", "b", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SUNIONSTORE", "b111su", "b111sa", "b111sb")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b111si", "b111sa", "b111sb")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SISMEMBER", "b111si", "b")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SINTER", "b111sa", "b111sb")), []string{"b"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b111sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b111sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b111x", "1-0", "f", "a")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b111x", "2-0", "f", "b")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b111x", "3-0", "f", "c")), "3-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b111x", "MAXLEN", "2")), 1)
	xr := db.Exec(nil, utils.ToCmdLine("XRANGE", "b111x", "-", "+", "COUNT", "1"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "2-0") {
		t.Fatalf("XRANGE: %s", xr.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b111g", "15.087269", "37.502669", "Catania")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEORADIUS", "b111g", "15", "37", "80", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Catania") {
		t.Fatalf("GEORADIUS: %s", geo.ToBytes())
	}
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b111g", "FROMMEMBER", "Catania", "BYRADIUS", "1", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH FROMMEMBER: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b111s1", "abcde")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b111s2", "abxyz")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b111s1", "b111s2")), "ab")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b111s1", "b111s2", "LEN")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b111s1", "-3", "-1")), "cde")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b111sort", "9", "1", "5", "3")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b111sort", "LIMIT", "1", "2")), []string{"3", "5"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b111pf", "a", "b", "c")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b111pf")), 3)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b111s1", "b111s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b111s2", "b111s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p111")), "p111")
}
