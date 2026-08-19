package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 158 R4-1 extras: LPOP COUNT 2, LMOVE RIGHT RIGHT, LINSERT BEFORE,
// LPOS COUNT 0 / RANK -2 / COUNT RANK 2, LMPOP LEFT COUNT 1, BLMOVE LEFT LEFT,
// LSET/LREM/LTRIM/LPUSHX, ZRANGE BYSCORE LIMIT WS, ZRANGESTORE BYSCORE,
// ZREVRANGEBYSCORE LIMIT 1 1, ZPOPMAX, ZUNIONSTORE MAX WEIGHTS, ZRANGE BYLEX,
// ZREVRANGEBYLEX, ZREMRANGEBYLEX, ZINTER SUM WS, ZINTERSTORE SUM, ZDIFF WS,
// ZMPOP MIN, ZADD GT/LT 不更新/INCR, BITOP OR/XOR/ONE, BITFIELD SET u8,
// GETEX PX/PERSIST, HEXPIRE NX/GT/LT, HPEXPIRE GT 短于现 TTL→0,
// HGETEX PX/PERSIST, SDIFFSTORE/SINTERSTORE, XTRIM MAXLEN,
// GEOSEARCH BYBOX, SORT DESC LIMIT 0 2, LCS, PFMERGE.
func TestR41Batch158Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b158l", "amber", "jade", "onyx", "opal", "pearl", "ruby", "topaz")), 7)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b158l", "2")), []string{"amber", "jade"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b158l", "0", "-1")), []string{"onyx", "opal", "pearl", "ruby", "topaz"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b158l", "b158l2", "RIGHT", "RIGHT")), "topaz")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b158l", "0", "-1")), []string{"onyx", "opal", "pearl", "ruby"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b158l2", "0", "-1")), []string{"topaz"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b158ins", "north", "south")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b158ins", "BEFORE", "south", "equator")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b158ins", "0", "-1")), []string{"north", "equator", "south"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b158lp", "x", "y", "x", "y", "x", "y")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b158lp", "x", "COUNT", "0")), []string{"0", "2", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b158lp", "x", "RANK", "-2")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b158lp", "x", "COUNT", "2", "RANK", "2")), []string{"2", "4"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b158lmp", "p", "q", "r", "s")), 4)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b158lmp", "LEFT", "COUNT", "1"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "p") {
		t.Fatalf("LMPOP LEFT COUNT 1: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b158lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b158bl", "oak", "pine", "elm")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b158bl", "b158bld", "LEFT", "LEFT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "oak") {
		t.Fatalf("BLMOVE LEFT LEFT: %s", bl.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b158bl", "0", "-1")), []string{"pine", "elm"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b158bld", "0", "-1")), []string{"oak"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b158lt", "p", "q", "r", "s", "t")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b158lt", "2", "R")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b158lt", "1", "s")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b158lt", "0", "2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b158lt", "0", "-1")), []string{"p", "q", "R"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b158lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b158lx", "w")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b158lx", "v")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b158lx", "x")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b158lx", "0", "-1")), []string{"v", "w", "x"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b158zs", "7", "a", "14", "b", "21", "c", "28", "d", "35", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b158zs", "14", "28", "BYSCORE", "WITHSCORES", "LIMIT", "0", "2")),
		[]string{"b", "14", "c", "21"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b158zrs", "b158zs", "14", "35", "BYSCORE")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b158zrs", "0", "-1")), []string{"b", "c", "d", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b158zs", "28", "14", "WITHSCORES", "LIMIT", "1", "1")),
		[]string{"c", "21"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b158zs")), []string{"e", "35"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b158zu", "1", "b158zs", "WEIGHTS", "3", "AGGREGATE", "MAX")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b158zu", "0", "-1", "WITHSCORES")),
		[]string{"a", "21", "b", "42", "c", "63", "d", "84"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b158zlex", "0", "p", "0", "q", "0", "r", "0", "s")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b158zlex", "[p", "(s", "BYLEX")), []string{"p", "q", "r"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b158zrs2", "b158zlex", "[q", "(s", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b158zrs2", "0", "-1")), []string{"q", "r"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYLEX", "b158zlex", "(s", "[q")), []string{"r", "q"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b158zlex", "[p", "(r")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b158zlex", "0", "-1")), []string{"r", "s"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b158z1", "11", "a", "25", "b", "7", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b158z2", "18", "b", "5", "c", "30", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b158z1", "b158z2", "WEIGHTS", "1", "1", "AGGREGATE", "SUM", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER SUM: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b158zi", "2", "b158z1", "b158z2", "WEIGHTS", "2", "1", "AGGREGATE", "SUM")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b158zi", "0", "-1", "WITHSCORES")),
		[]string{"c", "19", "b", "68"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b158z1", "b158z2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b158zm", "4", "a", "77", "k", "19", "b")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b158zm", "MIN", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "a") {
		t.Fatalf("ZMPOP MIN: %s", zmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b158zm", "GT", "70", "k")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b158zm", "LT", "30", "b")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b158zm", "INCR", "3.5", "b")), "22.5")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b158zm", "k")), "77")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b158b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b158b1", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b158b2", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b158bo", "b158b1", "b158b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b158bo")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b158bx", "b158b1", "b158b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b158bx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ONE", "b158bo1", "b158b1", "b158b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b158bo1")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b158bf", "SET", "u8", "0", "91"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "0") {
		t.Fatalf("BITFIELD SET u8: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b158st", "v", "PX", "83000")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b158st", "PX", "96000")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b158st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b158st")), -1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b158h", "f1", "ww", "f2", "38", "f3", "xx")), 3)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b158h", "82", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b158h", "18", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "0") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	hpgt := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b158h", "4000", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hpgt) || !strings.Contains(string(hpgt.ToBytes()), "0") {
		t.Fatalf("HPEXPIRE GT: %s", hpgt.ToBytes())
	}
	hlt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b158h", "35", "LT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hlt) || !strings.Contains(string(hlt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE LT: %s", hlt.ToBytes())
	}
	he := db.Exec(nil, utils.ToCmdLine("HGETEX", "b158h", "PX", "42000", "FIELDS", "1", "f2"))
	if protocol.IsErrorReply(he) || !strings.Contains(string(he.ToBytes()), "38") {
		t.Fatalf("HGETEX PX: %s", he.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b158h", "f2", "9")), 47)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b158h")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b158sa", "ua", "ub", "uc")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b158sb", "uc", "ud")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b158sd", "b158sa", "b158sb")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b158ss", "b158sa", "b158sb")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUNION", "b158ss")), []string{"uc"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b158sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b158sp")), "only")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b158sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b158x", "28-0", "k", "v")), "28-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b158x", "38-0", "k", "w")), "38-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b158x", "48-0", "k", "x")), "48-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b158x", "MAXLEN", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b158x")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b158g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b158g", "FROMLONLAT", "15", "37.5", "BYBOX", "100", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH BYBOX: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b158s1", "moonlight")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b158s2", "moon")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b158s1", "b158s2")), "moon")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b158s1", "b158s2", "LEN")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b158sort", "93", "17", "61", "28", "44")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b158sort", "DESC", "LIMIT", "0", "2")), []string{"93", "61"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b158p1", "e")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b158p2", "f")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b158pm", "b158p1", "b158p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b158pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b158s1", "b158s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b158s2", "b158s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p158")), "p158")
}
