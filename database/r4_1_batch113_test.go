package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 113 R4-1 extras: RPOP COUNT 2, LMOVE LEFT RIGHT, LINSERT AFTER,
// LPOS RANK 2 MAXLEN / COUNT RANK, LMPOP LEFT COUNT 1, BLMOVE RIGHT LEFT,
// LTRIM/LREM, ZRANGE BYSCORE LIMIT WS, ZREVRANGEBYLEX, ZINTERSTORE MAX WEIGHTS,
// ZUNIONSTORE MIN, ZDIFFSTORE, ZMPOP MAX, ZADD NX/XX/CH, BITOP OR/XOR/ANDOR,
// BITFIELD u16 SAT, GETEX EXAT, SET NX GET, HEXPIRE XX, HPEXPIRE LT, HGETEX EX,
// SDIFFSTORE, XTRIM MAXLEN, GEOSEARCH FROMMEMBER BYBOX, SORT ALPHA DESC LIMIT.
func TestR41Batch113Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b113l", "a", "b", "c", "d", "e", "f")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b113l", "2")), []string{"f", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b113l", "0", "-1")), []string{"a", "b", "c", "d"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b113l", "b113l2", "LEFT", "RIGHT")), "a")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b113l", "0", "-1")), []string{"b", "c", "d"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b113l2", "0", "-1")), []string{"a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b113ins", "a", "c", "e")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b113ins", "AFTER", "c", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b113ins", "0", "-1")), []string{"a", "c", "d", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b113lp", "a", "a", "b", "a", "c", "a")), 6)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b113lp", "a", "RANK", "2", "MAXLEN", "5")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b113lp", "a", "COUNT", "2", "RANK", "2")), []string{"1", "3"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b113lmp", "1", "2", "3", "4")), 4)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b113lmp", "LEFT", "COUNT", "1"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "1") {
		t.Fatalf("LMPOP LEFT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b113lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b113bl", "x", "y", "z")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b113bl", "b113bld", "RIGHT", "LEFT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "z") {
		t.Fatalf("BLMOVE RIGHT LEFT: %s", bl.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOPLPUSH", "b113bl", "b113bld")), "y")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b113bld", "0", "-1")), []string{"y", "z"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b113lt", "a", "b", "c", "d", "e")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b113lt", "1", "3")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b113lt", "0", "-1")), []string{"b", "c", "d"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b113lt", "2", "b")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b113lt", "0", "-1")), []string{"c", "d"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b113lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b113lx", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b113lx", "b")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b113lx", "0", "-1")), []string{"a", "b"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b113lx", "-1")), "b")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b113z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b113z", "2", "4", "BYSCORE", "LIMIT", "0", "2", "WITHSCORES")),
		[]string{"b", "2", "c", "3"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGE", "b113z", "0", "1", "WITHSCORES")),
		[]string{"e", "5", "d", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCOUNT", "b113z", "2", "4")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b113zrs", "b113z", "2", "4", "BYSCORE")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b113zrs", "0", "-1")), []string{"b", "c", "d"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYRANK", "b113z", "0", "0")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b113z", "0", "-1")), []string{"b", "c", "d", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b113zlex", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYLEX", "b113zlex", "(e", "[b")), []string{"d", "c", "b"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b113zlex", "[b", "(e")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b113z1", "3", "a", "5", "b", "7", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b113z2", "4", "a", "1", "b", "9", "d")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b113zi", "2", "b113z1", "b113z2", "WEIGHTS", "2", "1", "AGGREGATE", "MAX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b113zi", "0", "-1", "WITHSCORES")), []string{"a", "6", "b", "10"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b113zu", "2", "b113z1", "b113z2", "WEIGHTS", "1", "1", "AGGREGATE", "MIN")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b113zu", "0", "-1", "WITHSCORES")),
		[]string{"b", "1", "a", "3", "c", "7", "d", "9"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFFSTORE", "b113zd", "2", "b113z1", "b113z2")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b113zd", "0", "-1")), []string{"c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b113z1", "b113z2", "LIMIT", "1")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b113zm", "1", "a", "5", "e", "4", "d")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b113zm", "MAX", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "e") {
		t.Fatalf("ZMPOP MAX: %s", zmp.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b113zm")), []string{"d", "4"})
	bz := db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "b113zm", "0"))
	if protocol.IsErrorReply(bz) || !strings.Contains(string(bz.ToBytes()), "a") {
		t.Fatalf("BZPOPMIN: %s", bz.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b113za", "NX", "5", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b113za", "NX", "9", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b113za", "XX", "8", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b113za", "CH", "8", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b113za", "CH", "10", "a")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINCRBY", "b113za", "0.5", "a")), "10.5")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b113b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b113b1", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b113b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b113bo", "b113b1", "b113b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b113bo")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b113bx", "b113b1", "b113b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b113bx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ANDOR", "b113bao", "b113b1", "b113b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b113bao")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b113b1", "0", "0")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b113b1", "0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b113b1", "1")), 0)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b113bf", "OVERFLOW", "SAT", "INCRBY", "u16", "0", "70000"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "65535") {
		t.Fatalf("BITFIELD SAT u16: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b113st", "v")), "OK")
	sg := db.Exec(nil, utils.ToCmdLine("SET", "b113st", "w", "GET"))
	if protocol.IsErrorReply(sg) || !strings.Contains(string(sg.ToBytes()), "v") {
		t.Fatalf("SET GET: %s", sg.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b113st", "EXAT", "2000000000")), "w")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRETIME", "b113st")), 2000000000)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b113st", "x", "KEEPTTL")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETDEL", "b113st")), "x")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b113nx", "v", "NX")), "OK")
	nx := db.Exec(nil, utils.ToCmdLine("SET", "b113nx", "w", "NX", "GET"))
	if protocol.IsErrorReply(nx) || !strings.Contains(string(nx.ToBytes()), "v") {
		t.Fatalf("SET NX GET: %s", nx.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b113nx")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b113m1", "1", "b113m2", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b113m1", "9", "b113m3", "3")), 0)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b113sr", "hello")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b113sr", "0", "He")), 5)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b113sr", "0", "1")), "He")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUBSTR", "b113sr", "-2", "-1")), "lo")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b113nf", "1.5")), "1.5")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECRBY", "b113dn", "3")), -3)
	gs := db.Exec(nil, utils.ToCmdLine("GETSET", "b113gs", "old"))
	if _, ok := gs.(*protocol.NullBulkReply); !ok {
		t.Fatalf("GETSET miss: %T %s", gs, gs.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b113gs", "new")), "old")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("APPEND", "b113ap", "!")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("STRLEN", "b113ap")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b113h", "a", "10", "b", "20", "c", "hello")), 3)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b113h", "90", "NX", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hxx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b113h", "120", "XX", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hxx) || !strings.Contains(string(hxx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE XX: %s", hxx.ToBytes())
	}
	hxx0 := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b113h", "120", "XX", "FIELDS", "1", "b"))
	if protocol.IsErrorReply(hxx0) || !strings.Contains(string(hxx0.ToBytes()), "0") {
		t.Fatalf("HEXPIRE XX no-ttl: %s", hxx0.ToBytes())
	}
	hp := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b113h", "8000", "NX", "FIELDS", "1", "b"))
	if protocol.IsErrorReply(hp) || !strings.Contains(string(hp.ToBytes()), "1") {
		t.Fatalf("HPEXPIRE NX: %s", hp.ToBytes())
	}
	hlt := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b113h", "3000", "LT", "FIELDS", "1", "b"))
	if protocol.IsErrorReply(hlt) || !strings.Contains(string(hlt.ToBytes()), "1") {
		t.Fatalf("HPEXPIRE LT: %s", hlt.ToBytes())
	}
	hg := db.Exec(nil, utils.ToCmdLine("HGETEX", "b113h", "EX", "40", "FIELDS", "1", "c"))
	if protocol.IsErrorReply(hg) || !strings.Contains(string(hg.ToBytes()), "hello") {
		t.Fatalf("HGETEX EX: %s", hg.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBYFLOAT", "b113h", "a", "0.5")), "10.5")
	hd := db.Exec(nil, utils.ToCmdLine("HGETDEL", "b113h", "FIELDS", "1", "b"))
	if protocol.IsErrorReply(hd) || !strings.Contains(string(hd.ToBytes()), "20") {
		t.Fatalf("HGETDEL: %s", hd.ToBytes())
	}
	hm := db.Exec(nil, utils.ToCmdLine("HMGET", "b113h", "a", "miss", "c"))
	if protocol.IsErrorReply(hm) || !strings.Contains(string(hm.ToBytes()), "10.5") {
		t.Fatalf("HMGET: %s", hm.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSTRLEN", "b113h", "c")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b113h", "a", "x")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b113hk", "f", "v")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HKEYS", "b113hk")), []string{"f"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "b113hk")), "f")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b113sa", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b113sb", "b", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b113sd", "b113sa", "b113sb")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b113sd")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "2", "b113sa", "b113sb", "LIMIT", "1")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SINTER", "b113sa", "b113sb")), []string{"b"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SMOVE", "b113sa", "b113sb", "a")), 1)
	sm := db.Exec(nil, utils.ToCmdLine("SMISMEMBER", "b113sb", "a", "z", "b"))
	if protocol.IsErrorReply(sm) || !strings.Contains(string(sm.ToBytes()), "0") {
		t.Fatalf("SMISMEMBER: %s", sm.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b113sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b113sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b113x", "1-0", "f", "a")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b113x", "2-0", "f", "b")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b113x", "3-0", "f", "c")), "3-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b113x", "MAXLEN", "2")), 1)
	xr := db.Exec(nil, utils.ToCmdLine("XREVRANGE", "b113x", "+", "-", "COUNT", "1"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "3-0") {
		t.Fatalf("XREVRANGE: %s", xr.ToBytes())
	}
	x2 := db.Exec(nil, utils.ToCmdLine("XRANGE", "b113x", "-", "+", "COUNT", "1"))
	if protocol.IsErrorReply(x2) || !strings.Contains(string(x2.ToBytes()), "2-0") {
		t.Fatalf("XRANGE: %s", x2.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b113x", "2-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b113x")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b113g", "15.087269", "37.502669", "Catania")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEORADIUS", "b113g", "15", "37", "80", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Catania") {
		t.Fatalf("GEORADIUS: %s", geo.ToBytes())
	}
	gs2 := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b113g", "FROMMEMBER", "Catania", "BYBOX", "2", "2", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs2) || !strings.Contains(string(gs2.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH FROMMEMBER BYBOX: %s", gs2.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b113s1", "abcde")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b113s2", "abxyz")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b113s1", "b113s2")), "ab")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b113s1", "b113s2", "LEN")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b113sort", "b", "a", "d", "c")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b113sort", "ALPHA", "DESC", "LIMIT", "0", "2")), []string{"d", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b113p1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b113p2", "b")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b113pm", "b113p1", "b113p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b113pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b113s1", "b113s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b113s2", "b113s1r")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b113s1r", "80", "NX")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PEXPIRE", "b113s1r", "5000", "XX")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PERSIST", "b113s1r")), 1)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p113")), "p113")
}
