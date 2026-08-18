package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 126 R4-1 extras: LPOP COUNT 3, LMOVE RIGHT RIGHT, LINSERT BEFORE,
// LPOS COUNT 0 / RANK -1 / COUNT RANK, LMPOP LEFT COUNT 2, BLMOVE LEFT LEFT,
// LSET/LREM/LTRIM/LPUSHX, ZRANGE BYSCORE LIMIT WS, ZRANGESTORE BYSCORE,
// ZPOPMAX, ZUNIONSTORE MIN WEIGHTS, ZRANGE BYLEX, ZREVRANGEBYLEX,
// ZINTER MIN WS, ZINTERSTORE SUM, ZDIFF WS, ZMPOP MIN, ZADD GT/LT/INCR,
// BITOP OR/ONE/XOR, BITFIELD SET u8, GETEX EX/PERSIST, HEXPIRE NX/GT,
// HGETEX EX/PERSIST, SDIFFSTORE/SINTERSTORE, XDEL/XTRIM MINID,
// GEOSEARCH BYBOX, SORT DESC LIMIT, LCS, PFMERGE.
func TestR41Batch126Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b126l", "j", "k", "l", "m", "n", "o")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b126l", "3")), []string{"j", "k", "l"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b126l", "0", "-1")), []string{"m", "n", "o"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b126l", "b126l2", "RIGHT", "RIGHT")), "o")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b126l", "0", "-1")), []string{"m", "n"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b126l2", "0", "-1")), []string{"o"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b126ins", "u", "w")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b126ins", "BEFORE", "w", "v")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b126ins", "0", "-1")), []string{"u", "v", "w"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b126lp", "r", "t", "r", "t", "r")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b126lp", "r", "COUNT", "0")), []string{"0", "2", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b126lp", "r", "RANK", "-1")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b126lp", "r", "COUNT", "2", "RANK", "2")), []string{"2", "4"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b126lmp", "1", "2", "3", "4", "5")), 5)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b126lmp", "LEFT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "1") {
		t.Fatalf("LMPOP LEFT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b126lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b126bl", "x", "y", "z")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b126bl", "b126bld", "LEFT", "LEFT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "x") {
		t.Fatalf("BLMOVE LEFT LEFT: %s", bl.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b126bl", "0", "-1")), []string{"y", "z"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b126bld", "0", "-1")), []string{"x"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b126lt", "1", "2", "3", "4", "5")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b126lt", "1", "Y")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b126lt", "1", "4")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b126lt", "0", "2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b126lt", "0", "-1")), []string{"1", "Y", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b126lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b126lx", "q")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b126lx", "z")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b126zs", "2", "a", "4", "b", "6", "c", "8", "d", "10", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b126zs", "2", "8", "BYSCORE", "WITHSCORES", "LIMIT", "1", "2")),
		[]string{"b", "4", "c", "6"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b126zrs", "b126zs", "4", "10", "BYSCORE")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b126zrs", "0", "-1")), []string{"b", "c", "d", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b126zs")), []string{"e", "10"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b126zu", "1", "b126zs", "WEIGHTS", "2", "AGGREGATE", "MIN")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b126zu", "0", "-1", "WITHSCORES")),
		[]string{"a", "4", "b", "8", "c", "12", "d", "16"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b126zlex", "0", "w", "0", "x", "0", "y", "0", "z")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b126zlex", "[w", "(z", "BYLEX")), []string{"w", "x", "y"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b126zrs2", "b126zlex", "[x", "(z", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b126zrs2", "0", "-1")), []string{"x", "y"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYLEX", "b126zlex", "(z", "[x")), []string{"y", "x"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b126z1", "3", "a", "6", "b", "4", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b126z2", "5", "b", "2", "c", "9", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b126z1", "b126z2", "WEIGHTS", "1", "1", "AGGREGATE", "MIN", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER MIN: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b126zi", "2", "b126z1", "b126z2", "WEIGHTS", "1", "2", "AGGREGATE", "SUM")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b126zi", "0", "-1", "WITHSCORES")),
		[]string{"c", "8", "b", "16"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b126z1", "b126z2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b126zm", "3", "a", "7", "d", "5", "b")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b126zm", "MIN", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "a") {
		t.Fatalf("ZMPOP MIN: %s", zmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b126zm", "GT", "11", "d")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b126zm", "LT", "1", "b")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b126zm", "INCR", "0.5", "b")), "1.5")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b126zm", "d")), "11")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b126b1", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b126b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b126b2", "6", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b126bo", "b126b1", "b126b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b126bo")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ONE", "b126bone", "b126b1", "b126b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b126bone")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b126bx", "b126b1", "b126b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b126bx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b126b2", "6")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b126bf", "SET", "u8", "0", "99", "GET", "u8", "0"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "99") {
		t.Fatalf("BITFIELD SET u8: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b126st", "v", "EX", "50")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b126st", "EX", "80")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b126st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b126st")), -1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b126sr", "Hi")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b126sr", "0", "OK")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b126sr", "0", "1")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCRBY", "b126dn", "5")), 5)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b126dn", "0.25")), "5.25")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b126m1", "1")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b126m1", "9", "b126m3", "3")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b126st", "20", "NX")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b126ex", "v", "EX", "10")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b126ex", "40", "XX")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b126h", "f1", "v1", "f2", "7", "f3", "zz")), 3)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b126h", "50", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b126h", "90", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	he := db.Exec(nil, utils.ToCmdLine("HGETEX", "b126h", "EX", "40", "FIELDS", "1", "f2"))
	if protocol.IsErrorReply(he) || !strings.Contains(string(he.ToBytes()), "7") {
		t.Fatalf("HGETEX EX: %s", he.ToBytes())
	}
	hp := db.Exec(nil, utils.ToCmdLine("HGETEX", "b126h", "PERSIST", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(hp) || !strings.Contains(string(hp.ToBytes()), "zz") {
		t.Fatalf("HGETEX PERSIST: %s", hp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b126h", "f2", "3")), 10)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSTRLEN", "b126h", "f1")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b126h")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b126sa", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b126sb", "c", "d")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b126sd", "b126sa", "b126sb")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b126sd")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b126ss", "b126sa", "b126sb")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUNION", "b126ss")), []string{"c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SISMEMBER", "b126sa", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b126sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b126sp")), "only")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b126sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b126x", "1-0", "k", "v")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b126x", "3-0", "k", "w")), "3-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b126x", "6-0", "k", "x")), "6-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b126x", "3-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b126x", "MINID", "6-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b126x")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b126g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b126g", "FROMLONLAT", "15", "37.5", "BYBOX", "100", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH BYBOX: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b126s1", "house")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b126s2", "horse")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b126s1", "b126s2")), "hose")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b126s1", "b126s2", "LEN")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b126sort", "8", "3", "6", "1")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b126sort", "DESC", "LIMIT", "0", "2")), []string{"8", "6"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b126p1", "m")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b126p2", "n")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b126pm", "b126p1", "b126p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b126pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b126s1", "b126s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b126s2", "b126s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p126")), "p126")
}
