package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 135 R4-1 extras: RPOP COUNT 3, LMOVE LEFT LEFT, LINSERT AFTER,
// LPOS RANK 2 / MAXLEN / COUNT RANK, LMPOP RIGHT COUNT 1, BLMOVE RIGHT RIGHT,
// RPOPLPUSH, LSET/LREM/LTRIM/RPUSHX, ZRANGE BYSCORE LIMIT WS, ZRANGESTORE BYLEX,
// ZREVRANGEBYLEX, ZPOPMIN, ZUNIONSTORE MAX WEIGHTS, ZINTER MAX WS,
// ZINTERSTORE MAX, ZDIFF WS, ZMPOP MAX, BZPOPMIN, ZADD NX/XX/CH,
// BITOP AND/XOR/ANDOR, BITFIELD u16 SAT, GETEX EXAT/PERSIST, SET XX GET, KEEPTTL,
// HEXPIRE XX/NX/GT, HPEXPIRE GT, HGETEX EXAT/PERSIST, SDIFFSTORE/SINTERSTORE,
// XDEL/XTRIM MINID, GEOSEARCH BYBOX, SORT DESC LIMIT, LCS, PFMERGE.
func TestR41Batch135Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b135l", "h", "i", "j", "k", "l", "m", "n")), 7)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b135l", "3")), []string{"n", "m", "l"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b135l", "0", "-1")), []string{"h", "i", "j", "k"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b135l", "b135l2", "LEFT", "LEFT")), "h")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b135l", "0", "-1")), []string{"i", "j", "k"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b135l2", "0", "-1")), []string{"h"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b135ins", "cat", "dog")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b135ins", "AFTER", "cat", "bird")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b135ins", "0", "-1")), []string{"cat", "bird", "dog"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b135lp", "z", "z", "a", "z", "b", "z")), 6)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b135lp", "z", "RANK", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b135lp", "z", "MAXLEN", "1")), 0)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b135lp", "z", "COUNT", "2", "RANK", "2")), []string{"1", "3"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b135lmp", "9", "8", "7", "6")), 4)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b135lmp", "RIGHT", "COUNT", "1"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "6") {
		t.Fatalf("LMPOP RIGHT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b135lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b135bl", "p", "q", "r")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b135bl", "b135bld", "RIGHT", "RIGHT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "r") {
		t.Fatalf("BLMOVE RIGHT RIGHT: %s", bl.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOPLPUSH", "b135bl", "b135bld")), "q")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b135bl", "0", "-1")), []string{"p"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b135bld", "0", "-1")), []string{"q", "r"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b135lt", "a", "b", "c", "d", "e")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b135lt", "3", "Q")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b135lt", "1", "b")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b135lt", "0", "2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b135lt", "0", "-1")), []string{"a", "c", "Q"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b135lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b135lx", "q")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b135lx", "z")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b135lx", "w")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b135zs", "3", "a", "8", "b", "12", "c", "16", "d", "22", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b135zs", "8", "16", "BYSCORE", "WITHSCORES", "LIMIT", "1", "2")),
		[]string{"c", "12", "d", "16"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b135zlex", "0", "q", "0", "r", "0", "s", "0", "t")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b135zrs", "b135zlex", "[r", "(t", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b135zrs", "0", "-1")), []string{"r", "s"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYLEX", "b135zlex", "(t", "[q", "LIMIT", "0", "2")), []string{"s", "r"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "b135zs")), []string{"a", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b135zu", "1", "b135zs", "WEIGHTS", "2", "AGGREGATE", "MAX")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b135zu", "0", "-1", "WITHSCORES")),
		[]string{"b", "16", "c", "24", "d", "32", "e", "44"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b135z1", "8", "a", "11", "b", "4", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b135z2", "3", "b", "9", "c", "5", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b135z1", "b135z2", "WEIGHTS", "1", "1", "AGGREGATE", "MAX", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER MAX: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b135zi", "2", "b135z1", "b135z2", "WEIGHTS", "2", "1", "AGGREGATE", "MAX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b135zi", "0", "-1", "WITHSCORES")),
		[]string{"c", "9", "b", "22"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b135z1", "b135z2", "LIMIT", "2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b135zm", "3", "a", "21", "g", "8", "c")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b135zm", "MAX", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "g") {
		t.Fatalf("ZMPOP MAX: %s", zmp.ToBytes())
	}
	bz := db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "b135zm", "0"))
	if protocol.IsErrorReply(bz) || !strings.Contains(string(bz.ToBytes()), "a") {
		t.Fatalf("BZPOPMIN: %s", bz.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b135zm", "NX", "10", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b135zm", "XX", "CH", "15", "c")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b135zm", "CH", "4", "d")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b135zm", "c")), "15")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b135b1", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b135b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b135b2", "5", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "AND", "b135ba", "b135b1", "b135b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b135ba")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b135bx", "b135b1", "b135b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b135bx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ANDOR", "b135bo", "b135b1", "b135b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b135bo")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b135bf", "OVERFLOW", "SAT", "INCRBY", "u16", "0", "80000"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "65535") {
		t.Fatalf("BITFIELD u16 SAT: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b135st", "v", "EX", "60")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b135st", "EXAT", "2000000000")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b135st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b135st")), -1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b135st", "w")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("APPEND", "b135st", "!")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b135st", "z", "XX", "GET")), "w!")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b135st", "keep", "KEEPTTL")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b135st", "14", "NX")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b135h", "f1", "v1", "f2", "21", "f3", "v3")), 3)
	hxx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b135h", "50", "XX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hxx) || !strings.Contains(string(hxx.ToBytes()), "0") {
		t.Fatalf("HEXPIRE XX: %s", hxx.ToBytes())
	}
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b135h", "40", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hpgt := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b135h", "8000", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hpgt) || !strings.Contains(string(hpgt.ToBytes()), "0") {
		t.Fatalf("HPEXPIRE GT: %s", hpgt.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b135h", "90", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	he := db.Exec(nil, utils.ToCmdLine("HGETEX", "b135h", "EXAT", "2000000000", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(he) || !strings.Contains(string(he.ToBytes()), "v3") {
		t.Fatalf("HGETEX EXAT: %s", he.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b135h", "f2", "4")), 25)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b135h")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b135sa", "p", "q", "r")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b135sb", "r", "s")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b135sd", "b135sa", "b135sb")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b135ss", "b135sa", "b135sb")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUNION", "b135ss")), []string{"r"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b135sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b135sp")), "only")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b135sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b135x", "5-0", "k", "v")), "5-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b135x", "8-0", "k", "w")), "8-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b135x", "13-0", "k", "x")), "13-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b135x", "8-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b135x", "MINID", "13-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b135x")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b135g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b135g", "FROMLONLAT", "15", "37.5", "BYBOX", "100", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH BYBOX: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b135s1", "lighthouse")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b135s2", "house")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b135s1", "b135s2")), "house")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b135s1", "b135s2", "LEN")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b135sort", "4", "10", "2", "8")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b135sort", "DESC", "LIMIT", "0", "2")), []string{"10", "8"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b135p1", "m")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b135p2", "n")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b135pm", "b135p1", "b135p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b135pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b135s1", "b135s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b135s2", "b135s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p135")), "p135")
}
