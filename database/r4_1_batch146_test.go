package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 146 R4-1 extras: LPOP COUNT 3, LMOVE RIGHT RIGHT, LINSERT BEFORE,
// LPOS COUNT 0 / RANK -2 / COUNT RANK, LMPOP LEFT COUNT 3, BLMOVE LEFT LEFT,
// LSET/LREM/LTRIM/LPUSHX, ZRANGE BYSCORE LIMIT WS, ZRANGESTORE BYSCORE,
// ZREVRANGEBYSCORE, ZPOPMAX, ZUNIONSTORE MIN WEIGHTS, ZRANGE BYLEX,
// ZREVRANGEBYLEX, ZREMRANGEBYLEX, ZINTER MIN WS, ZINTERSTORE MAX, ZDIFF WS,
// ZMPOP MIN, ZADD GT/LT/INCR, BITOP OR/XOR/DIFF, BITFIELD SET u8,
// GETEX EX/PERSIST, HEXPIRE NX/GT/LT, HPEXPIRE GT 短于现 TTL→0,
// HGETEX EX/PERSIST, SDIFFSTORE/SINTERSTORE, XTRIM MAXLEN,
// GEOSEARCH BYBOX, SORT DESC LIMIT 0 2, LCS, PFMERGE.
func TestR41Batch146Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b146l", "oak", "pine", "maple", "elm", "ash", "yew", "fir")), 7)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b146l", "3")), []string{"oak", "pine", "maple"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b146l", "0", "-1")), []string{"elm", "ash", "yew", "fir"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b146l", "b146l2", "RIGHT", "RIGHT")), "fir")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b146l", "0", "-1")), []string{"elm", "ash", "yew"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b146l2", "0", "-1")), []string{"fir"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b146ins", "first", "last")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b146ins", "BEFORE", "last", "inner")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b146ins", "0", "-1")), []string{"first", "inner", "last"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b146lp", "w", "x", "w", "x", "w", "x")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b146lp", "w", "COUNT", "0")), []string{"0", "2", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b146lp", "w", "RANK", "-2")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b146lp", "w", "COUNT", "2", "RANK", "1")), []string{"0", "2"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b146lmp", "p", "q", "r", "s", "t")), 5)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b146lmp", "LEFT", "COUNT", "3"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "p") {
		t.Fatalf("LMPOP LEFT COUNT 3: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b146lmp")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b146bl", "north", "east", "south")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b146bl", "b146bld", "LEFT", "LEFT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "north") {
		t.Fatalf("BLMOVE LEFT LEFT: %s", bl.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b146bl", "0", "-1")), []string{"east", "south"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b146bld", "0", "-1")), []string{"north"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b146lt", "a", "b", "c", "d", "e")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b146lt", "2", "C")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b146lt", "1", "b")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b146lt", "1", "3")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b146lt", "0", "-1")), []string{"C", "d", "e"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b146lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b146lx", "k")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b146lx", "j")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b146lx", "l")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b146lx", "0", "-1")), []string{"j", "k", "l"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b146zs", "8", "a", "13", "b", "18", "c", "23", "d", "28", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b146zs", "13", "23", "BYSCORE", "WITHSCORES", "LIMIT", "1", "2")),
		[]string{"c", "18", "d", "23"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b146zrs", "b146zs", "13", "28", "BYSCORE")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b146zrs", "0", "-1")), []string{"b", "c", "d", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b146zs", "23", "13", "WITHSCORES", "LIMIT", "1", "2")),
		[]string{"c", "18", "b", "13"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b146zs")), []string{"e", "28"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b146zu", "1", "b146zs", "WEIGHTS", "3", "AGGREGATE", "MIN")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b146zu", "0", "-1", "WITHSCORES")),
		[]string{"a", "24", "b", "39", "c", "54", "d", "69"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b146zlex", "0", "m", "0", "n", "0", "o", "0", "p")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b146zlex", "[m", "(p", "BYLEX")), []string{"m", "n", "o"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b146zrs2", "b146zlex", "[n", "(p", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b146zrs2", "0", "-1")), []string{"n", "o"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYLEX", "b146zlex", "(p", "[n")), []string{"o", "n"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b146zlex", "[m", "(o")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b146zlex", "0", "-1")), []string{"o", "p"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b146z1", "18", "a", "27", "b", "9", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b146z2", "14", "b", "6", "c", "20", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b146z1", "b146z2", "WEIGHTS", "1", "1", "AGGREGATE", "MIN", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER MIN: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b146zi", "2", "b146z1", "b146z2", "WEIGHTS", "3", "1", "AGGREGATE", "MAX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b146zi", "0", "-1", "WITHSCORES")),
		[]string{"c", "27", "b", "81"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b146z1", "b146z2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b146zm", "4", "a", "25", "m", "11", "b")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b146zm", "MIN", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "a") {
		t.Fatalf("ZMPOP MIN: %s", zmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b146zm", "GT", "40", "m")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b146zm", "LT", "3", "b")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b146zm", "INCR", "2.5", "b")), "5.5")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b146zm", "m")), "40")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b146b1", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b146b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b146b2", "5", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b146bo", "b146b1", "b146b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b146bo")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b146bx", "b146b1", "b146b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b146bx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "DIFF", "b146bd", "b146b2", "b146b1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b146bd")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b146bf", "SET", "u8", "0", "91"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "0") {
		t.Fatalf("BITFIELD SET u8: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b146st", "v", "EX", "80")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b146st", "EX", "95")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b146st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b146st")), -1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b146h", "f1", "aa", "f2", "19", "f3", "cc")), 3)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b146h", "55", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b146h", "14", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "0") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	hpgt := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b146h", "3000", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hpgt) || !strings.Contains(string(hpgt.ToBytes()), "0") {
		t.Fatalf("HPEXPIRE GT: %s", hpgt.ToBytes())
	}
	hlt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b146h", "20", "LT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hlt) || !strings.Contains(string(hlt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE LT: %s", hlt.ToBytes())
	}
	he := db.Exec(nil, utils.ToCmdLine("HGETEX", "b146h", "EX", "40", "FIELDS", "1", "f2"))
	if protocol.IsErrorReply(he) || !strings.Contains(string(he.ToBytes()), "19") {
		t.Fatalf("HGETEX EX: %s", he.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b146h", "f2", "7")), 26)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b146h")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b146sa", "k", "l", "m")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b146sb", "m", "n")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b146sd", "b146sa", "b146sb")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b146ss", "b146sa", "b146sb")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUNION", "b146ss")), []string{"m"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b146sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b146sp")), "only")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b146sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b146x", "16-0", "k", "v")), "16-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b146x", "26-0", "k", "w")), "26-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b146x", "36-0", "k", "x")), "36-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b146x", "MAXLEN", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b146x")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b146g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b146g", "FROMLONLAT", "15", "37.5", "BYBOX", "90", "90", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH BYBOX: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b146s1", "notebook")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b146s2", "book")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b146s1", "b146s2")), "book")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b146s1", "b146s2", "LEN")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b146sort", "21", "7", "28", "11", "16")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b146sort", "DESC", "LIMIT", "0", "2")), []string{"28", "21"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b146p1", "g")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b146p2", "h")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b146pm", "b146p1", "b146p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b146pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b146s1", "b146s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b146s2", "b146s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p146")), "p146")
}
