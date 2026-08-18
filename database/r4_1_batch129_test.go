package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 129 R4-1 extras: RPOP COUNT 3, LMOVE LEFT RIGHT, LINSERT AFTER,
// LPOS RANK 2 / MAXLEN / COUNT RANK, LMPOP RIGHT COUNT 2, BLMOVE RIGHT LEFT,
// RPOPLPUSH, LSET/LREM/LTRIM/RPUSHX, ZRANGE BYSCORE LIMIT WS, ZRANGESTORE BYLEX,
// ZREVRANGEBYLEX, ZPOPMIN, ZUNIONSTORE SUM WEIGHTS, ZINTER MIN WS,
// ZINTERSTORE SUM, ZDIFF WS, ZMPOP MAX, BZPOPMIN, ZADD NX/XX/CH,
// BITOP AND/XOR/ONE, BITFIELD u8 WRAP, GETEX EX/PERSIST, SET XX GET, KEEPTTL,
// HEXPIRE XX/NX, HPEXPIRE GT, HGETEX EX/PERSIST, SDIFFSTORE/SINTERSTORE,
// XDEL/XTRIM MINID, GEOSEARCH BYBOX, SORT DESC LIMIT, LCS, PFMERGE.
func TestR41Batch129Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b129l", "g", "h", "i", "j", "k", "l")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b129l", "3")), []string{"l", "k", "j"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b129l", "0", "-1")), []string{"g", "h", "i"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b129l", "b129l2", "LEFT", "RIGHT")), "g")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b129l", "0", "-1")), []string{"h", "i"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b129l2", "0", "-1")), []string{"g"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b129ins", "d", "f")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b129ins", "AFTER", "d", "e")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b129ins", "0", "-1")), []string{"d", "e", "f"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b129lp", "b", "b", "c", "b", "d", "b")), 6)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b129lp", "b", "RANK", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b129lp", "b", "MAXLEN", "1")), 0)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b129lp", "b", "COUNT", "2", "RANK", "2")), []string{"1", "3"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b129lmp", "1", "2", "3", "4", "5")), 5)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b129lmp", "RIGHT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "5") {
		t.Fatalf("LMPOP RIGHT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b129lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b129bl", "p", "q", "r")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b129bl", "b129bld", "RIGHT", "LEFT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "r") {
		t.Fatalf("BLMOVE RIGHT LEFT: %s", bl.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOPLPUSH", "b129bl", "b129bld")), "q")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b129bl", "0", "-1")), []string{"p"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b129bld", "0", "-1")), []string{"q", "r"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b129lt", "a", "b", "c", "d", "e")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b129lt", "2", "X")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b129lt", "-1", "d")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b129lt", "0", "2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b129lt", "0", "-1")), []string{"a", "b", "X"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b129lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b129lx", "q")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b129lx", "z")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b129lx", "w")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b129zs", "4", "a", "8", "b", "12", "c", "16", "d", "20", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b129zs", "8", "16", "BYSCORE", "WITHSCORES", "LIMIT", "1", "2")),
		[]string{"c", "12", "d", "16"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b129zlex", "0", "w", "0", "x", "0", "y", "0", "z")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b129zrs", "b129zlex", "[x", "(z", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b129zrs", "0", "-1")), []string{"x", "y"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYLEX", "b129zlex", "(z", "[w", "LIMIT", "0", "2")), []string{"y", "x"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "b129zs")), []string{"a", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b129zu", "1", "b129zs", "WEIGHTS", "2", "AGGREGATE", "SUM")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b129zu", "0", "-1", "WITHSCORES")),
		[]string{"b", "16", "c", "24", "d", "32", "e", "40"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYSCORE", "b129zs", "8", "12")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCARD", "b129zs")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b129z1", "5", "a", "9", "b", "3", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b129z2", "2", "b", "7", "c", "4", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b129z1", "b129z2", "WEIGHTS", "1", "1", "AGGREGATE", "MIN", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "b") {
		t.Fatalf("ZINTER MIN: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b129zi", "2", "b129z1", "b129z2", "WEIGHTS", "2", "1", "AGGREGATE", "SUM")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b129zi", "0", "-1", "WITHSCORES")),
		[]string{"c", "13", "b", "20"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b129z1", "b129z2", "LIMIT", "2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b129zm", "3", "a", "11", "g", "5", "c")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b129zm", "MAX", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "g") {
		t.Fatalf("ZMPOP MAX: %s", zmp.ToBytes())
	}
	bz := db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "b129zm", "0"))
	if protocol.IsErrorReply(bz) || !strings.Contains(string(bz.ToBytes()), "a") {
		t.Fatalf("BZPOPMIN: %s", bz.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b129zm", "NX", "6", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b129zm", "XX", "CH", "9", "c")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b129zm", "CH", "4", "d")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b129zm", "c")), "9")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b129b1", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b129b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b129b2", "7", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "AND", "b129ba", "b129b1", "b129b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b129ba")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b129bx", "b129b1", "b129b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b129bx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ONE", "b129bone", "b129b1", "b129b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b129bone")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b129b2", "7")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b129bf", "OVERFLOW", "WRAP", "INCRBY", "u8", "0", "300"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "44") {
		t.Fatalf("BITFIELD u8 WRAP: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b129st", "v", "EX", "50")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b129st", "EX", "80")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b129st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b129st")), -1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b129st", "w")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("APPEND", "b129st", "!")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b129st", "z", "XX", "GET")), "w!")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b129st", "keep", "KEEPTTL")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b129st", "10", "NX")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b129h", "f1", "v1", "f2", "11", "f3", "v3")), 3)
	hxx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b129h", "50", "XX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hxx) || !strings.Contains(string(hxx.ToBytes()), "0") {
		t.Fatalf("HEXPIRE XX: %s", hxx.ToBytes())
	}
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b129h", "40", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b129h", "50000", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "1") {
		t.Fatalf("HPEXPIRE GT: %s", hgt.ToBytes())
	}
	he := db.Exec(nil, utils.ToCmdLine("HGETEX", "b129h", "EX", "30", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(he) || !strings.Contains(string(he.ToBytes()), "v3") {
		t.Fatalf("HGETEX EX: %s", he.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b129h", "f2", "4")), 15)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b129h")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b129sa", "p", "q", "r")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b129sb", "r", "s")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b129sd", "b129sa", "b129sb")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b129ss", "b129sa", "b129sb")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUNION", "b129ss")), []string{"r"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b129sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b129sp")), "only")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b129sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b129x", "1-0", "k", "v")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b129x", "5-0", "k", "w")), "5-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b129x", "9-0", "k", "x")), "9-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b129x", "5-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b129x", "MINID", "9-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b129x")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b129g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b129g", "FROMLONLAT", "15", "37.5", "BYBOX", "100", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH BYBOX: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b129s1", "repayment")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b129s2", "payment")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b129s1", "b129s2")), "payment")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b129s1", "b129s2", "LEN")), 7)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b129sort", "5", "1", "8", "3")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b129sort", "DESC", "LIMIT", "0", "2")), []string{"8", "5"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b129p1", "m")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b129p2", "n")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b129pm", "b129p1", "b129p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b129pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b129s1", "b129s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b129s2", "b129s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p129")), "p129")
}
