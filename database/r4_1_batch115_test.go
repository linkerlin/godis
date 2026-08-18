package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 115 R4-1 extras: RPOP COUNT 3, LMOVE LEFT LEFT, LINSERT AFTER, LPOS COUNT 0,
// LMPOP LEFT COUNT 2, BLMOVE RIGHT RIGHT, RPOPLPUSH, LTRIM, ZRANGEBYSCORE WS,
// ZRANGESTORE BYSCORE, ZMSCORE, ZPOPMAX, ZUNIONSTORE MAX WEIGHTS, ZINTER MAX WS,
// ZDIFFSTORE, ZMPOP MAX, BZPOPMIN, BITOP XOR/OR, BITFIELD SET u8, GETEX PERSIST,
// SET NX GET miss, HEXPIRE NX/GT, HGETEX PERSIST, HGETDEL, SDIFF/SMOVE/SINTER,
// XTRIM MAXLEN, GEORADIUS/GEOSEARCH FROMMEMBER, SORT ALPHA, LCS, PFMERGE.
func TestR41Batch115Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b115l", "a", "b", "c", "d", "e", "f")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b115l", "3")), []string{"f", "e", "d"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b115l", "0", "-1")), []string{"a", "b", "c"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b115l", "b115l2", "LEFT", "LEFT")), "a")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b115l", "0", "-1")), []string{"b", "c"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b115l2", "0", "-1")), []string{"a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b115ins", "a", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b115ins", "AFTER", "a", "b")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b115ins", "0", "-1")), []string{"a", "b", "c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b115lp", "a", "a", "b", "a")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b115lp", "a", "COUNT", "0")), []string{"0", "1", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b115lp", "b", "RANK", "1")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b115lmp", "1", "2", "3", "4", "5")), 5)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b115lmp", "LEFT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "1") {
		t.Fatalf("LMPOP LEFT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b115lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b115bl", "a", "b", "c")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b115bl", "b115bld", "RIGHT", "RIGHT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "c") {
		t.Fatalf("BLMOVE RIGHT RIGHT: %s", bl.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOPLPUSH", "b115bl", "b115bld")), "b")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b115bld", "0", "-1")), []string{"b", "c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b115lt", "a", "b", "c", "d", "e")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b115lt", "0", "2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b115lt", "0", "-1")), []string{"a", "b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b115lt", "d")), 4)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b115zs", "1", "a", "2", "b", "3", "c", "4", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYSCORE", "b115zs", "(1", "4", "WITHSCORES")),
		[]string{"b", "2", "c", "3", "d", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b115zrs", "b115zs", "2", "4", "BYSCORE")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b115zrs", "0", "-1")), []string{"b", "c", "d"})
	zm := db.Exec(nil, utils.ToCmdLine("ZMSCORE", "b115zs", "a", "missing", "d"))
	if protocol.IsErrorReply(zm) || !strings.Contains(string(zm.ToBytes()), "1") {
		t.Fatalf("ZMSCORE: %s", zm.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANK", "b115zs", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANK", "b115zs", "b")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b115zs")), []string{"d", "4"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b115z1", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b115z2", "5", "b", "1", "c", "9", "d")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b115zu", "1", "b115z1", "WEIGHTS", "2", "AGGREGATE", "MAX")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b115zu", "0", "-1", "WITHSCORES")),
		[]string{"a", "2", "b", "4", "c", "6"})
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b115z1", "b115z2", "WEIGHTS", "1", "1", "AGGREGATE", "MAX", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER MAX: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFFSTORE", "b115zd", "2", "b115z1", "b115z2")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b115zd", "0", "-1")), []string{"a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b115z1", "b115z2", "LIMIT", "1")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b115zm", "1", "a", "4", "d", "2", "b")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b115zm", "MAX", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "d") {
		t.Fatalf("ZMPOP MAX: %s", zmp.ToBytes())
	}
	bz := db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "b115zm", "0"))
	if protocol.IsErrorReply(bz) || !strings.Contains(string(bz.ToBytes()), "a") {
		t.Fatalf("BZPOPMIN: %s", bz.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b115b1", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b115b2", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b115b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b115bx", "b115b1", "b115b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b115bx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b115bo", "b115b1", "b115b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b115bo")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b115b2", "1")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b115bf", "OVERFLOW", "WRAP", "SET", "u8", "0", "200", "GET", "u8", "0"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "200") {
		t.Fatalf("BITFIELD SET u8: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b115st", "v", "EX", "50")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b115st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b115st")), -1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b115st", "w")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("APPEND", "b115st", "!")), 2)
	nx := db.Exec(nil, utils.ToCmdLine("SET", "b115nx", "v", "NX", "GET"))
	if _, ok := nx.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET NX GET miss: %T %s", nx, nx.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b115nx")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b115sr", "0", "Hi")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b115sr", "0", "1")), "Hi")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECRBY", "b115dn", "4")), -4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIREAT", "b115st", "2000000000")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRETIME", "b115st")), 2000000000)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b115m1", "1", "b115m2", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b115m1", "9", "b115m3", "3")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b115h", "f1", "v1", "f2", "20")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b115h", "f1", "x")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b115h", "f3", "v3")), 1)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b115h", "40", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b115h", "80", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	hp := db.Exec(nil, utils.ToCmdLine("HGETEX", "b115h", "PERSIST", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hp) || !strings.Contains(string(hp.ToBytes()), "v1") {
		t.Fatalf("HGETEX PERSIST: %s", hp.ToBytes())
	}
	hd := db.Exec(nil, utils.ToCmdLine("HGETDEL", "b115h", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(hd) || !strings.Contains(string(hd.ToBytes()), "v3") {
		t.Fatalf("HGETDEL: %s", hd.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBYFLOAT", "b115h", "f2", "0.5")), "20.5")
	hm := db.Exec(nil, utils.ToCmdLine("HMGET", "b115h", "f1", "miss", "f2"))
	if protocol.IsErrorReply(hm) || !strings.Contains(string(hm.ToBytes()), "20.5") {
		t.Fatalf("HMGET: %s", hm.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b115hk", "f", "v")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HKEYS", "b115hk")), []string{"f"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "b115hk")), "f")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b115sa", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b115sb", "b")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SDIFF", "b115sa", "b115sb")), []string{"a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SMOVE", "b115sa", "b115sb", "a")), 1)
	sm := db.Exec(nil, utils.ToCmdLine("SMISMEMBER", "b115sb", "a", "z", "b"))
	if protocol.IsErrorReply(sm) || !strings.Contains(string(sm.ToBytes()), "0") {
		t.Fatalf("SMISMEMBER: %s", sm.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SINTER", "b115sa", "b115sb")), []string{"b"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "2", "b115sa", "b115sb")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b115sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b115sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b115x", "1-0", "k", "v")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b115x", "2-0", "k", "w")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b115x", "3-0", "k", "x")), "3-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b115x", "MAXLEN", "2")), 1)
	xr := db.Exec(nil, utils.ToCmdLine("XREVRANGE", "b115x", "+", "-", "COUNT", "1"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "3-0") {
		t.Fatalf("XREVRANGE: %s", xr.ToBytes())
	}
	x2 := db.Exec(nil, utils.ToCmdLine("XRANGE", "b115x", "-", "+", "COUNT", "1"))
	if protocol.IsErrorReply(x2) || !strings.Contains(string(x2.ToBytes()), "2-0") {
		t.Fatalf("XRANGE: %s", x2.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b115g", "15.087269", "37.502669", "Catania")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEORADIUS", "b115g", "15", "37", "80", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Catania") {
		t.Fatalf("GEORADIUS: %s", geo.ToBytes())
	}
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b115g", "FROMMEMBER", "Catania", "BYRADIUS", "1", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH FROMMEMBER: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b115s1", "abcde")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b115s2", "abxyz")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b115s1", "b115s2")), "ab")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b115s1", "b115s2", "LEN")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b115sort", "b", "a", "c")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b115sort", "ALPHA")), []string{"a", "b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b115p1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b115p2", "b")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b115pm", "b115p1", "b115p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b115pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b115s1", "b115s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b115s2", "b115s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p115")), "p115")
}
