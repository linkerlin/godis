package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 112 R4-1 extras: LPOP COUNT 2, LMOVE RIGHT RIGHT/LEFT RIGHT, LPOS COUNT 0,
// BLMOVE LEFT RIGHT/RIGHT LEFT, LINSERT BEFORE, ZRANGE BYLEX LIMIT, ZREMRANGEBYLEX,
// ZRANGEBYSCORE WS, ZUNIONSTORE MAX WEIGHTS, ZINTER MIN WS, BITOP AND/DIFF/ONE,
// BITFIELD i16 WRAP, GETEX PERSIST, SET XX miss, HEXPIRE NX/GT, HGETEX PERSIST,
// HGETDEL, SDIFF/SMOVE/SINTER, XTRIM MINID, GEOSEARCH BYBOX, SORT ALPHA, LCS,
// PFCOUNT multi.
func TestR41Batch112Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b112l", "a", "b", "c", "d", "e", "f")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b112l", "2")), []string{"a", "b"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b112l", "0", "-1")), []string{"c", "d", "e", "f"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b112l", "b112l", "RIGHT", "RIGHT")), "f")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b112l", "0", "-1")), []string{"c", "d", "e", "f"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b112l2", "a", "b", "c")), 3)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b112l2", "b112l3", "LEFT", "RIGHT")), "a")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b112l2", "0", "-1")), []string{"b", "c"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b112l3", "0", "-1")), []string{"a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b112lp", "a", "a", "b", "a")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b112lp", "a", "COUNT", "0")), []string{"0", "1", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b112lp", "b", "RANK", "1")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b112bl", "x", "y", "z")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b112bl", "b112bld", "LEFT", "RIGHT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "x") {
		t.Fatalf("BLMOVE LEFT RIGHT: %s", bl.ToBytes())
	}
	bl2 := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b112bl", "b112bld", "RIGHT", "LEFT", "0"))
	if protocol.IsErrorReply(bl2) || !strings.Contains(string(bl2.ToBytes()), "z") {
		t.Fatalf("BLMOVE RIGHT LEFT: %s", bl2.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b112bld", "0", "-1")), []string{"z", "x"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b112ins", "a", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b112ins", "BEFORE", "c", "b")), 3)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b112ins", "0", "1")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b112ins", "0", "-1")), []string{"a", "b"})
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b112ins", "1", "X")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b112ins", "1")), "X")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b112z", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b112z", "[b", "(e", "BYLEX")), []string{"b", "c", "d"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b112z", "[b", "(e", "BYLEX", "LIMIT", "0", "2")), []string{"b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b112z", "[b", "(e")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b112z", "[c", "(e")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b112z", "0", "-1")), []string{"a", "b", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b112zs", "1", "a", "2", "b", "3", "c", "4", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYSCORE", "b112zs", "(1", "4", "WITHSCORES")),
		[]string{"b", "2", "c", "3", "d", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b112zu", "1", "b112zs", "WEIGHTS", "2", "AGGREGATE", "MAX")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b112zu", "0", "-1", "WITHSCORES")),
		[]string{"a", "2", "b", "4", "c", "6", "d", "8"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b112zrs", "b112zs", "2", "4", "BYSCORE")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b112zrs", "0", "-1")), []string{"b", "c", "d"})
	zm := db.Exec(nil, utils.ToCmdLine("ZMSCORE", "b112zs", "a", "missing", "d"))
	if protocol.IsErrorReply(zm) || !strings.Contains(string(zm.ToBytes()), "1") {
		t.Fatalf("ZMSCORE: %s", zm.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANK", "b112zs", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANK", "b112zs", "b")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "b112zs")), []string{"a", "1"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b112zi1", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b112zi2", "5", "b", "1", "c", "9", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b112zi1", "b112zi2", "WEIGHTS", "1", "1", "AGGREGATE", "MIN", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER MIN: %s", zi.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFF", "2", "b112zi1", "b112zi2")), []string{"a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b112zi1", "b112zi2", "LIMIT", "1")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b112b1", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b112b2", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b112b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "AND", "b112ba", "b112b1", "b112b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b112ba")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "DIFF", "b112bd", "b112b2", "b112b1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b112bd")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ONE", "b112bo", "b112b1", "b112b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b112bo")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b112bf", "OVERFLOW", "WRAP", "INCRBY", "i16", "0", "40000"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "-25536") {
		t.Fatalf("BITFIELD WRAP i16: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b112st", "v", "EX", "50")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b112st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b112st")), -1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b112st", "w")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("APPEND", "b112st", "!")), 2)
	xx := db.Exec(nil, utils.ToCmdLine("SET", "b112xx", "v", "XX"))
	if _, ok := xx.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET XX miss: %T %s", xx, xx.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECRBY", "b112n", "2")), -2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIREAT", "b112st", "2000000000")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRETIME", "b112st")), 2000000000)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b112h", "f1", "v1", "f2", "20")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b112h", "f1", "x")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b112h", "f3", "v3")), 1)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b112h", "40", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b112h", "80", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	hp := db.Exec(nil, utils.ToCmdLine("HGETEX", "b112h", "PERSIST", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hp) || !strings.Contains(string(hp.ToBytes()), "v1") {
		t.Fatalf("HGETEX PERSIST: %s", hp.ToBytes())
	}
	hd := db.Exec(nil, utils.ToCmdLine("HGETDEL", "b112h", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(hd) || !strings.Contains(string(hd.ToBytes()), "v3") {
		t.Fatalf("HGETDEL: %s", hd.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b112h", "f2", "5")), 25)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b112hk", "f", "v")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HKEYS", "b112hk")), []string{"f"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "b112hk")), "f")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b112sa", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b112sb", "b")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SDIFF", "b112sa", "b112sb")), []string{"a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SMOVE", "b112sa", "b112sb", "a")), 1)
	sm := db.Exec(nil, utils.ToCmdLine("SMISMEMBER", "b112sb", "a", "z", "b"))
	if protocol.IsErrorReply(sm) || !strings.Contains(string(sm.ToBytes()), "0") {
		t.Fatalf("SMISMEMBER: %s", sm.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SINTER", "b112sa", "b112sb")), []string{"b"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b112sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b112sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b112x", "1-0", "k", "v")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b112x", "2-0", "k", "w")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b112x", "3-0", "k", "x")), "3-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b112x", "MINID", "2-0")), 1)
	xr := db.Exec(nil, utils.ToCmdLine("XRANGE", "b112x", "-", "+", "COUNT", "1"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "2-0") {
		t.Fatalf("XRANGE: %s", xr.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b112x", "2-0")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b112g", "13.361389", "38.115556", "Palermo")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b112g", "FROMLONLAT", "13", "38", "BYBOX", "100", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Palermo") {
		t.Fatalf("GEOSEARCH BYBOX: %s", geo.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b112s1", "hello")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b112s2", "hallo")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b112s1", "b112s2")), "hllo")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b112s1", "b112s2", "LEN")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b112sort", "b", "a", "c")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b112sort", "ALPHA")), []string{"a", "b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b112p1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b112p2", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b112p1", "b112p2")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "b112s1", "b112s2", "b112miss")), 2)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p112")), "p112")
}
