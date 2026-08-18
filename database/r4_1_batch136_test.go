package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 136 R4-1 extras: LPOP COUNT 1, LMOVE RIGHT LEFT, LINSERT BEFORE,
// LPOS COUNT 0 / RANK -1 / COUNT RANK, LMPOP LEFT COUNT 1, BLMOVE LEFT RIGHT,
// LSET/LREM/LTRIM/LPUSHX, ZRANGE BYSCORE LIMIT WS, ZRANGESTORE BYSCORE,
// ZREVRANGEBYSCORE, ZPOPMAX, ZUNIONSTORE MAX WEIGHTS, ZRANGE BYLEX,
// ZREVRANGEBYLEX, ZREMRANGEBYLEX, ZINTER MAX WS, ZINTERSTORE MAX, ZDIFF WS,
// ZMPOP MIN, ZADD GT/LT/INCR, BITOP OR/ONE/XOR, BITFIELD SET u8,
// GETEX EX/PERSIST, HEXPIRE NX/GT/LT, HPEXPIRE GT, HGETEX EX/PERSIST,
// SDIFFSTORE/SINTERSTORE, XTRIM MAXLEN, GEOSEARCH BYBOX, SORT DESC LIMIT,
// LCS, PFMERGE.
func TestR41Batch136Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b136l", "11", "22", "33", "44", "55", "66")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b136l", "1")), []string{"11"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b136l", "0", "-1")), []string{"22", "33", "44", "55", "66"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b136l", "b136l2", "RIGHT", "LEFT")), "66")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b136l", "0", "-1")), []string{"22", "33", "44", "55"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b136l2", "0", "-1")), []string{"66"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b136ins", "start", "end")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b136ins", "BEFORE", "end", "core")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b136ins", "0", "-1")), []string{"start", "core", "end"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b136lp", "f", "g", "f", "g", "f")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b136lp", "f", "COUNT", "0")), []string{"0", "2", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b136lp", "f", "RANK", "-1")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b136lp", "f", "COUNT", "2", "RANK", "2")), []string{"2", "4"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b136lmp", "foo", "bar", "baz", "qux")), 4)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b136lmp", "LEFT", "COUNT", "1"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "foo") {
		t.Fatalf("LMPOP LEFT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b136lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b136bl", "one", "two", "three")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b136bl", "b136bld", "LEFT", "RIGHT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "one") {
		t.Fatalf("BLMOVE LEFT RIGHT: %s", bl.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b136bl", "0", "-1")), []string{"two", "three"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b136bld", "0", "-1")), []string{"one"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b136lt", "w", "x", "y", "z", "v")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b136lt", "0", "K")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b136lt", "1", "z")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b136lt", "0", "2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b136lt", "0", "-1")), []string{"K", "x", "y"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b136lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b136lx", "n")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b136lx", "m")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b136lx", "o")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b136zs", "3", "a", "8", "b", "13", "c", "18", "d", "23", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b136zs", "8", "18", "BYSCORE", "WITHSCORES", "LIMIT", "1", "2")),
		[]string{"c", "13", "d", "18"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b136zrs", "b136zs", "8", "23", "BYSCORE")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b136zrs", "0", "-1")), []string{"b", "c", "d", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b136zs", "18", "8", "WITHSCORES", "LIMIT", "0", "2")),
		[]string{"d", "18", "c", "13"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b136zs")), []string{"e", "23"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b136zu", "1", "b136zs", "WEIGHTS", "5", "AGGREGATE", "MAX")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b136zu", "0", "-1", "WITHSCORES")),
		[]string{"a", "15", "b", "40", "c", "65", "d", "90"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b136zlex", "0", "w", "0", "x", "0", "y", "0", "z")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b136zlex", "[w", "(z", "BYLEX")), []string{"w", "x", "y"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b136zrs2", "b136zlex", "[x", "(z", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYLEX", "b136zlex", "(z", "[x")), []string{"y", "x"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b136zlex", "[w", "(y")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b136zlex", "0", "-1")), []string{"y", "z"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b136z1", "9", "a", "15", "b", "7", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b136z2", "6", "b", "3", "c", "11", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b136z1", "b136z2", "WEIGHTS", "1", "1", "AGGREGATE", "MAX", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER MAX: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b136zi", "2", "b136z1", "b136z2", "WEIGHTS", "4", "1", "AGGREGATE", "MAX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b136zi", "0", "-1", "WITHSCORES")),
		[]string{"c", "28", "b", "60"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b136z1", "b136z2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b136zm", "8", "a", "17", "k", "11", "b")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b136zm", "MIN", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "a") {
		t.Fatalf("ZMPOP MIN: %s", zmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b136zm", "GT", "40", "k")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b136zm", "LT", "3", "b")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b136zm", "INCR", "1.25", "b")), "4.25")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b136zm", "k")), "40")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b136b1", "7", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b136b2", "7", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b136b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b136bo", "b136b1", "b136b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b136bo")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ONE", "b136bn", "b136b1", "b136b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b136bn")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b136bx", "b136b1", "b136b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b136bx")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b136bf", "SET", "u8", "0", "91"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "0") {
		t.Fatalf("BITFIELD SET u8: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b136st", "v", "EX", "40")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b136st", "EX", "70")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b136st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b136st")), -1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b136h", "f1", "dd", "f2", "12", "f3", "vv")), 3)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b136h", "55", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b136h", "25", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "0") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	hpgt := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b136h", "15000", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hpgt) || !strings.Contains(string(hpgt.ToBytes()), "0") {
		t.Fatalf("HPEXPIRE GT: %s", hpgt.ToBytes())
	}
	hlt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b136h", "18", "LT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hlt) || !strings.Contains(string(hlt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE LT: %s", hlt.ToBytes())
	}
	he := db.Exec(nil, utils.ToCmdLine("HGETEX", "b136h", "EX", "25", "FIELDS", "1", "f2"))
	if protocol.IsErrorReply(he) || !strings.Contains(string(he.ToBytes()), "12") {
		t.Fatalf("HGETEX EX: %s", he.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b136h", "f2", "7")), 19)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b136h")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b136sa", "x", "y", "z")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b136sb", "z", "w")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b136sd", "b136sa", "b136sb")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b136ss", "b136sa", "b136sb")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUNION", "b136ss")), []string{"z"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b136sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b136sp")), "only")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b136sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b136x", "12-0", "k", "v")), "12-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b136x", "24-0", "k", "w")), "24-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b136x", "36-0", "k", "x")), "36-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b136x", "MAXLEN", "1")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b136x")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b136g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b136g", "FROMLONLAT", "15", "37.5", "BYBOX", "100", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH BYBOX: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b136s1", "shipping")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b136s2", "ping")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b136s1", "b136s2")), "ping")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b136s1", "b136s2", "LEN")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b136sort", "12", "4", "16", "8")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b136sort", "DESC", "LIMIT", "0", "2")), []string{"16", "12"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b136p1", "m")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b136p2", "n")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b136pm", "b136p1", "b136p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b136pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b136s1", "b136s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b136s2", "b136s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p136")), "p136")
}
