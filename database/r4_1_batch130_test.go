package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 130 R4-1 extras: LPOP COUNT 1, LMOVE RIGHT RIGHT, LINSERT BEFORE,
// LPOS COUNT 0 / RANK -1 / COUNT RANK, LMPOP LEFT COUNT 2, BLMOVE LEFT LEFT,
// LSET/LREM/LTRIM/LPUSHX, ZRANGE BYSCORE LIMIT WS, ZRANGESTORE BYSCORE,
// ZREVRANGEBYSCORE, ZPOPMAX, ZUNIONSTORE MIN WEIGHTS, ZRANGE BYLEX,
// ZREVRANGEBYLEX, ZREMRANGEBYLEX, ZINTER MIN WS, ZINTERSTORE SUM, ZDIFF WS,
// ZMPOP MIN, ZADD GT/LT/INCR, BITOP OR/ONE/XOR, BITFIELD SET u8,
// GETEX EX/PERSIST, HEXPIRE NX/GT, HGETEX EX/PERSIST, SDIFFSTORE/SINTERSTORE,
// XDEL/XTRIM MINID, GEOSEARCH BYBOX, SORT DESC LIMIT, LCS, PFMERGE.
func TestR41Batch130Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b130l", "2", "4", "6", "8", "10", "12")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b130l", "1")), []string{"2"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b130l", "0", "-1")), []string{"4", "6", "8", "10", "12"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b130l", "b130l2", "RIGHT", "RIGHT")), "12")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b130l", "0", "-1")), []string{"4", "6", "8", "10"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b130l2", "0", "-1")), []string{"12"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b130ins", "k", "m")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b130ins", "BEFORE", "m", "l")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b130ins", "0", "-1")), []string{"k", "l", "m"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b130lp", "z", "x", "z", "x", "z")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b130lp", "z", "COUNT", "0")), []string{"0", "2", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b130lp", "z", "RANK", "-1")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b130lp", "z", "COUNT", "2", "RANK", "2")), []string{"2", "4"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b130lmp", "a", "b", "c", "d", "e")), 5)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b130lmp", "LEFT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "a") {
		t.Fatalf("LMPOP LEFT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b130lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b130bl", "m", "n", "o")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b130bl", "b130bld", "LEFT", "LEFT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "m") {
		t.Fatalf("BLMOVE LEFT LEFT: %s", bl.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b130bl", "0", "-1")), []string{"n", "o"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b130bld", "0", "-1")), []string{"m"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b130lt", "1", "2", "3", "4", "5")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b130lt", "1", "Q")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b130lt", "1", "4")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b130lt", "0", "2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b130lt", "0", "-1")), []string{"1", "Q", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b130lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b130lx", "q")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b130lx", "z")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b130lx", "y")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b130zs", "2", "a", "6", "b", "10", "c", "14", "d", "18", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b130zs", "6", "14", "BYSCORE", "WITHSCORES", "LIMIT", "1", "2")),
		[]string{"c", "10", "d", "14"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b130zrs", "b130zs", "6", "18", "BYSCORE")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b130zrs", "0", "-1")), []string{"b", "c", "d", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b130zs", "14", "6", "WITHSCORES", "LIMIT", "0", "2")),
		[]string{"d", "14", "c", "10"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b130zs")), []string{"e", "18"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b130zu", "1", "b130zs", "WEIGHTS", "2", "AGGREGATE", "MIN")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b130zu", "0", "-1", "WITHSCORES")),
		[]string{"a", "4", "b", "12", "c", "20", "d", "28"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b130zlex", "0", "a", "0", "b", "0", "c", "0", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b130zlex", "[a", "(d", "BYLEX")), []string{"a", "b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b130zrs2", "b130zlex", "[b", "(d", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYLEX", "b130zlex", "(d", "[b")), []string{"c", "b"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b130zlex", "[a", "(c")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b130zlex", "0", "-1")), []string{"c", "d"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b130z1", "5", "a", "9", "b", "7", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b130z2", "4", "b", "2", "c", "8", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b130z1", "b130z2", "WEIGHTS", "1", "1", "AGGREGATE", "MIN", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER MIN: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b130zi", "2", "b130z1", "b130z2", "WEIGHTS", "1", "2", "AGGREGATE", "SUM")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b130zi", "0", "-1", "WITHSCORES")),
		[]string{"c", "11", "b", "17"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b130z1", "b130z2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b130zm", "5", "a", "12", "d", "8", "b")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b130zm", "MIN", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "a") {
		t.Fatalf("ZMPOP MIN: %s", zmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b130zm", "GT", "16", "d")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b130zm", "LT", "3", "b")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b130zm", "INCR", "0.5", "b")), "3.5")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b130zm", "d")), "16")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b130b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b130b2", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b130b2", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b130bo", "b130b1", "b130b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b130bo")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ONE", "b130bone", "b130b1", "b130b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b130bone")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b130bx", "b130b1", "b130b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b130bx")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b130bf", "SET", "u8", "0", "77", "GET", "u8", "0"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "77") {
		t.Fatalf("BITFIELD SET u8: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b130st", "v", "EX", "60")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b130st", "EX", "70")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b130st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b130st")), -1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b130h", "f1", "v1", "f2", "9", "f3", "zz")), 3)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b130h", "45", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b130h", "70", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	he := db.Exec(nil, utils.ToCmdLine("HGETEX", "b130h", "EX", "30", "FIELDS", "1", "f2"))
	if protocol.IsErrorReply(he) || !strings.Contains(string(he.ToBytes()), "9") {
		t.Fatalf("HGETEX EX: %s", he.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b130h", "f2", "5")), 14)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b130h")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b130sa", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b130sb", "c", "d")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b130sd", "b130sa", "b130sb")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b130ss", "b130sa", "b130sb")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUNION", "b130ss")), []string{"c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b130sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b130sp")), "only")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b130sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b130x", "2-0", "k", "v")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b130x", "4-0", "k", "w")), "4-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b130x", "8-0", "k", "x")), "8-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b130x", "4-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b130x", "MINID", "8-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b130x")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b130g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b130g", "FROMLONLAT", "15", "37.5", "BYBOX", "100", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH BYBOX: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b130s1", "recursion")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b130s2", "excursion")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b130s1", "b130s2")), "ecursion")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b130s1", "b130s2", "LEN")), 8)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b130sort", "4", "9", "1", "6")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b130sort", "DESC", "LIMIT", "0", "2")), []string{"9", "6"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b130p1", "m")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b130p2", "n")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b130pm", "b130p1", "b130p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b130pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b130s1", "b130s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b130s2", "b130s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p130")), "p130")
}
