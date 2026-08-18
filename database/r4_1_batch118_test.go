package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 118 R4-1 extras: LPOP COUNT 1, LMOVE RIGHT RIGHT, LINSERT BEFORE,
// LPOS COUNT 0, LMPOP LEFT COUNT 2, BLMOVE LEFT LEFT, LSET/LREM/LPUSHX,
// ZRANGEBYSCORE WS, ZRANGESTORE BYSCORE, ZPOPMAX, ZUNIONSTORE MIN WEIGHTS,
// ZINTER MAX WS, ZDIFFSTORE, ZMPOP MIN, ZRANGE BYLEX, BITOP OR/ONE,
// BITFIELD SET u8, GETEX PERSIST, SET NX GET miss, HEXPIRE NX/GT,
// HPEXPIRE LT, HGETEX PERSIST, HGETDEL, SDIFF/SMOVE/SINTER, XTRIM MAXLEN,
// GEOSEARCH BYRADIUS, SORT ALPHA, LCS, PFMERGE.
func TestR41Batch118Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b118l", "a", "b", "c", "d", "e", "f")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b118l", "1")), []string{"a"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b118l", "0", "-1")), []string{"b", "c", "d", "e", "f"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b118l", "b118l2", "RIGHT", "RIGHT")), "f")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b118l", "0", "-1")), []string{"b", "c", "d", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b118l2", "0", "-1")), []string{"f"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b118ins", "a", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b118ins", "BEFORE", "c", "b")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b118ins", "0", "-1")), []string{"a", "b", "c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b118lp", "a", "a", "b", "a")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b118lp", "a", "COUNT", "0")), []string{"0", "1", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b118lp", "b", "RANK", "1")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b118lmp", "1", "2", "3", "4", "5")), 5)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b118lmp", "LEFT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "1") {
		t.Fatalf("LMPOP LEFT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b118lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b118bl", "a", "b", "c")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b118bl", "b118bld", "LEFT", "LEFT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "a") {
		t.Fatalf("BLMOVE LEFT LEFT: %s", bl.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b118bld", "0", "-1")), []string{"a"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b118bl", "0", "-1")), []string{"b", "c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b118lt", "a", "b", "c", "d", "e")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b118lt", "2", "X")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b118lt", "2")), "X")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b118lt", "0", "b")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b118lt", "0", "-1")), []string{"a", "X", "d", "e"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b118lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b118lx", "q")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b118lx", "z")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b118zs", "1", "a", "2", "b", "3", "c", "4", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYSCORE", "b118zs", "(1", "4", "WITHSCORES")),
		[]string{"b", "2", "c", "3", "d", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b118zrs", "b118zs", "1", "3", "BYSCORE")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b118zrs", "0", "-1")), []string{"a", "b", "c"})
	zm := db.Exec(nil, utils.ToCmdLine("ZMSCORE", "b118zs", "a", "missing", "d"))
	if protocol.IsErrorReply(zm) || !strings.Contains(string(zm.ToBytes()), "1") {
		t.Fatalf("ZMSCORE: %s", zm.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANK", "b118zs", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANK", "b118zs", "b")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b118zs")), []string{"d", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b118zu", "1", "b118zs", "WEIGHTS", "2", "AGGREGATE", "MIN")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b118zu", "0", "-1", "WITHSCORES")),
		[]string{"a", "2", "b", "4", "c", "6"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b118z1", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b118z2", "5", "b", "1", "c", "9", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b118z1", "b118z2", "WEIGHTS", "1", "1", "AGGREGATE", "MAX", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER MAX: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFFSTORE", "b118zd", "2", "b118z1", "b118z2")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b118zd", "0", "-1")), []string{"a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b118z1", "b118z2", "LIMIT", "1")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b118zm", "1", "a", "4", "d", "2", "b")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b118zm", "MIN", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "a") {
		t.Fatalf("ZMPOP MIN: %s", zmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYRANK", "b118zm", "0", "0")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b118zlex", "0", "a", "0", "b", "0", "c", "0", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b118zlex", "[a", "(d", "BYLEX", "LIMIT", "0", "2")), []string{"a", "b"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b118zlex", "[a", "(d")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b118b1", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b118b2", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b118b2", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b118bo", "b118b1", "b118b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b118bo")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ONE", "b118bone", "b118b1", "b118b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b118bone")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b118b2", "1")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b118bf", "OVERFLOW", "WRAP", "SET", "u8", "0", "200", "GET", "u8", "0"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "200") {
		t.Fatalf("BITFIELD SET u8: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b118st", "v", "EX", "50")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b118st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b118st")), -1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b118st", "w")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("APPEND", "b118st", "!")), 2)
	nx := db.Exec(nil, utils.ToCmdLine("SET", "b118nx", "v", "NX", "GET"))
	if _, ok := nx.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET NX GET miss: %T %s", nx, nx.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b118nx")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b118sr", "0", "Hi")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b118sr", "0", "1")), "Hi")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECRBY", "b118dn", "4")), -4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b118m1", "1", "b118m2", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b118m1", "9", "b118m3", "3")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIREAT", "b118st", "2000000000")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRETIME", "b118st")), 2000000000)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b118h", "f1", "v1", "f2", "20")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b118h", "f1", "x")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b118h", "f3", "v3")), 1)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b118h", "40", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b118h", "80", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	hlt := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b118h", "5000", "LT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hlt) || !strings.Contains(string(hlt.ToBytes()), "1") {
		t.Fatalf("HPEXPIRE LT: %s", hlt.ToBytes())
	}
	hp := db.Exec(nil, utils.ToCmdLine("HGETEX", "b118h", "PERSIST", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hp) || !strings.Contains(string(hp.ToBytes()), "v1") {
		t.Fatalf("HGETEX PERSIST: %s", hp.ToBytes())
	}
	hd := db.Exec(nil, utils.ToCmdLine("HGETDEL", "b118h", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(hd) || !strings.Contains(string(hd.ToBytes()), "v3") {
		t.Fatalf("HGETDEL: %s", hd.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBYFLOAT", "b118h", "f2", "0.5")), "20.5")
	hm := db.Exec(nil, utils.ToCmdLine("HMGET", "b118h", "f1", "miss", "f2"))
	if protocol.IsErrorReply(hm) || !strings.Contains(string(hm.ToBytes()), "20.5") {
		t.Fatalf("HMGET: %s", hm.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b118hk", "f", "v")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HKEYS", "b118hk")), []string{"f"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "b118hk")), "f")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b118sa", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b118sb", "b")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SDIFF", "b118sa", "b118sb")), []string{"a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SMOVE", "b118sa", "b118sb", "a")), 1)
	sm := db.Exec(nil, utils.ToCmdLine("SMISMEMBER", "b118sb", "a", "z", "b"))
	if protocol.IsErrorReply(sm) || !strings.Contains(string(sm.ToBytes()), "0") {
		t.Fatalf("SMISMEMBER: %s", sm.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SINTER", "b118sa", "b118sb")), []string{"b"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "2", "b118sa", "b118sb")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b118sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b118sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b118x", "1-0", "k", "v")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b118x", "2-0", "k", "w")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b118x", "3-0", "k", "x")), "3-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b118x", "MAXLEN", "2")), 1)
	xr := db.Exec(nil, utils.ToCmdLine("XREVRANGE", "b118x", "+", "-", "COUNT", "1"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "3-0") {
		t.Fatalf("XREVRANGE: %s", xr.ToBytes())
	}
	x2 := db.Exec(nil, utils.ToCmdLine("XRANGE", "b118x", "-", "+", "COUNT", "1"))
	if protocol.IsErrorReply(x2) || !strings.Contains(string(x2.ToBytes()), "2-0") {
		t.Fatalf("XRANGE: %s", x2.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b118g", "13.361389", "38.115556", "Palermo")), 1)
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b118g", "FROMLONLAT", "13", "38", "BYRADIUS", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Palermo") {
		t.Fatalf("GEOSEARCH BYRADIUS: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b118s1", "hello")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b118s2", "hallo")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b118s1", "b118s2")), "hllo")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b118s1", "b118s2", "LEN")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b118sort", "b", "a", "c")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b118sort", "ALPHA")), []string{"a", "b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b118p1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b118p2", "b")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b118pm", "b118p1", "b118p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b118pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b118s1", "b118s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b118s2", "b118s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p118")), "p118")
}
