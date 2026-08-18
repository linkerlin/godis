package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 122 R4-1 extras: LPOP COUNT 2, LMOVE RIGHT RIGHT, LINSERT BEFORE,
// LPOS COUNT 0 / RANK -1 / COUNT RANK, LMPOP LEFT COUNT 1, BLMOVE LEFT LEFT,
// LSET/LREM/LTRIM/LPUSHX, ZRANGE BYSCORE LIMIT WS, ZRANGESTORE BYSCORE,
// ZPOPMAX, ZUNIONSTORE MIN WEIGHTS, ZRANGE BYLEX, ZREVRANGEBYLEX,
// ZINTER MIN WS, ZINTERSTORE SUM, ZDIFF WS, ZMPOP MIN, ZADD GT/LT/INCR,
// BITOP OR/ONE/XOR, BITFIELD SET u8, GETEX EX/PERSIST, HEXPIRE NX/GT,
// HGETEX EX/PERSIST, SDIFFSTORE/SINTERSTORE, XDEL/XTRIM MINID,
// GEOSEARCH BYBOX, SORT DESC LIMIT, LCS, PFMERGE.
func TestR41Batch122Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b122l", "a", "b", "c", "d", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b122l", "2")), []string{"a", "b"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b122l", "0", "-1")), []string{"c", "d", "e"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b122l", "b122l2", "RIGHT", "RIGHT")), "e")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b122l", "0", "-1")), []string{"c", "d"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b122l2", "0", "-1")), []string{"e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b122ins", "w", "y")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b122ins", "BEFORE", "y", "x")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b122ins", "0", "-1")), []string{"w", "x", "y"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b122lp", "a", "b", "a", "b", "a")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b122lp", "a", "COUNT", "0")), []string{"0", "2", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b122lp", "a", "RANK", "-1")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b122lp", "a", "COUNT", "2", "RANK", "2")), []string{"2", "4"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b122lmp", "9", "8", "7", "6")), 4)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b122lmp", "LEFT", "COUNT", "1"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "9") {
		t.Fatalf("LMPOP LEFT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b122lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b122bl", "q", "w", "e")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b122bl", "b122bld", "LEFT", "LEFT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "q") {
		t.Fatalf("BLMOVE LEFT LEFT: %s", bl.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b122bl", "0", "-1")), []string{"w", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b122bld", "0", "-1")), []string{"q"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b122lt", "1", "2", "3", "4", "5")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b122lt", "2", "Z")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b122lt", "1", "4")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b122lt", "0", "2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b122lt", "0", "-1")), []string{"1", "2", "Z"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b122lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b122lx", "q")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b122lx", "z")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b122zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b122zs", "1", "4", "BYSCORE", "WITHSCORES", "LIMIT", "0", "2")),
		[]string{"a", "1", "b", "2"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b122zrs", "b122zs", "2", "5", "BYSCORE")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b122zrs", "0", "-1")), []string{"b", "c", "d", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b122zs")), []string{"e", "5"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b122zu", "1", "b122zs", "WEIGHTS", "3", "AGGREGATE", "MIN")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b122zu", "0", "-1", "WITHSCORES")),
		[]string{"a", "3", "b", "6", "c", "9", "d", "12"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b122zlex", "0", "a", "0", "b", "0", "c", "0", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b122zlex", "[a", "(d", "BYLEX")), []string{"a", "b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b122zrs2", "b122zlex", "[b", "(d", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b122zrs2", "0", "-1")), []string{"b", "c"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYLEX", "b122zlex", "(d", "[b")), []string{"c", "b"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b122z1", "1", "a", "4", "b", "2", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b122z2", "3", "b", "8", "c", "1", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b122z1", "b122z2", "WEIGHTS", "1", "1", "AGGREGATE", "MIN", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER MIN: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b122zi", "2", "b122z1", "b122z2", "WEIGHTS", "2", "1", "AGGREGATE", "SUM")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b122zi", "0", "-1", "WITHSCORES")),
		[]string{"b", "11", "c", "12"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b122z1", "b122z2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b122zm", "1", "a", "4", "d", "2", "b")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b122zm", "MIN", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "a") {
		t.Fatalf("ZMPOP MIN: %s", zmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b122zm", "GT", "9", "d")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b122zm", "LT", "0", "b")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b122zm", "INCR", "0.5", "b")), "0.5")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b122zm", "d")), "9")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b122b1", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b122b2", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b122b2", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b122bo", "b122b1", "b122b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b122bo")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ONE", "b122bone", "b122b1", "b122b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b122bone")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b122bx", "b122b1", "b122b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b122bx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b122b2", "3")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b122bf", "SET", "u8", "0", "42", "GET", "u8", "0"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "42") {
		t.Fatalf("BITFIELD SET u8: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b122st", "v", "EX", "50")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b122st", "EX", "80")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b122st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b122st")), -1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b122sr", "Hi")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b122sr", "0", "OK")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b122sr", "0", "1")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCRBY", "b122dn", "5")), 5)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b122dn", "0.25")), "5.25")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b122m1", "1")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b122m1", "9", "b122m3", "3")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b122st", "20", "NX")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b122ex", "v", "EX", "10")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b122ex", "40", "XX")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b122h", "f1", "v1", "f2", "7", "f3", "zz")), 3)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b122h", "50", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b122h", "90", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	he := db.Exec(nil, utils.ToCmdLine("HGETEX", "b122h", "EX", "40", "FIELDS", "1", "f2"))
	if protocol.IsErrorReply(he) || !strings.Contains(string(he.ToBytes()), "7") {
		t.Fatalf("HGETEX EX: %s", he.ToBytes())
	}
	hp := db.Exec(nil, utils.ToCmdLine("HGETEX", "b122h", "PERSIST", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(hp) || !strings.Contains(string(hp.ToBytes()), "zz") {
		t.Fatalf("HGETEX PERSIST: %s", hp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b122h", "f2", "3")), 10)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSTRLEN", "b122h", "f1")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b122h")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b122sa", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b122sb", "c", "d")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b122sd", "b122sa", "b122sb")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b122sd")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b122ss", "b122sa", "b122sb")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUNION", "b122ss")), []string{"c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SISMEMBER", "b122sa", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b122sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b122sp")), "only")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b122sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b122x", "1-0", "k", "v")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b122x", "2-0", "k", "w")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b122x", "4-0", "k", "x")), "4-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b122x", "2-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b122x", "MINID", "4-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b122x")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b122g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b122g", "FROMLONLAT", "15", "37.5", "BYBOX", "100", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH BYBOX: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b122s1", "abcxyz")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b122s2", "abqxyz")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b122s1", "b122s2")), "abxyz")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b122s1", "b122s2", "LEN")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b122sort", "4", "1", "3", "2")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b122sort", "DESC", "LIMIT", "0", "2")), []string{"4", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b122p1", "m")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b122p2", "n")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b122pm", "b122p1", "b122p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b122pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b122s1", "b122s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b122s2", "b122s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p122")), "p122")
}
