package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 125 R4-1 extras: RPOP COUNT 3, LMOVE LEFT RIGHT, LINSERT AFTER,
// LPOS RANK 3 / MAXLEN miss / COUNT 0, LMPOP RIGHT COUNT 2, BLMOVE RIGHT LEFT,
// RPOPLPUSH, LTRIM/LSET/LREM/RPUSHX/LPUSHX, ZRANGE BYSCORE LIMIT WS,
// ZRANGESTORE BYLEX, ZREVRANGEBYLEX, ZPOPMIN, ZUNIONSTORE SUM WEIGHTS,
// ZINTER MIN WS, ZDIFFSTORE, ZMPOP MAX, BZPOPMIN, ZADD NX/XX/CH, ZMSCORE,
// ZINCRBY, BITOP AND/XOR/ONE, BITFIELD u8 WRAP, GETEX EX, KEEPTTL,
// GETEX PERSIST, SET XX GET, HEXPIRE XX/NX/GT, HPEXPIRE GT, HGETEX EX/PERSIST,
// HGETDEL, SDIFFSTORE/SINTERSTORE/SINTER, XDEL/XTRIM MINID, GEOSEARCH BYBOX,
// GEORADIUS, SORT DESC LIMIT, LCS, PFMERGE.
func TestR41Batch125Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b125l", "p", "q", "r", "s", "t", "u")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b125l", "3")), []string{"u", "t", "s"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b125l", "0", "-1")), []string{"p", "q", "r"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b125l", "b125l2", "LEFT", "RIGHT")), "p")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b125l", "0", "-1")), []string{"q", "r"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b125l2", "0", "-1")), []string{"p"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b125l", "-1")), "r")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b125ins", "w", "y")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b125ins", "AFTER", "w", "x")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b125ins", "0", "-1")), []string{"w", "x", "y"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b125lp", "x", "x", "y", "x", "z")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b125lp", "x", "RANK", "3")), 3)
	lpMiss := db.Exec(nil, utils.ToCmdLine("LPOS", "b125lp", "y", "MAXLEN", "1"))
	if _, ok := lpMiss.(*protocol.NullBulkReply); !ok {
		t.Fatalf("LPOS MAXLEN miss: %T %s", lpMiss, lpMiss.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b125lp", "x", "COUNT", "0")), []string{"0", "1", "3"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b125lmp", "10", "20", "30", "40", "50")), 5)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b125lmp", "RIGHT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "50") {
		t.Fatalf("LMPOP RIGHT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b125lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b125bl", "m", "n", "o")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b125bl", "b125bld", "RIGHT", "LEFT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "o") {
		t.Fatalf("BLMOVE RIGHT LEFT: %s", bl.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOPLPUSH", "b125bl", "b125bld")), "n")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b125bl", "0", "-1")), []string{"m"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b125bld", "0", "-1")), []string{"n", "o"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b125tr", "a", "b", "c", "d", "e")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b125tr", "2", "4")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b125tr", "0", "C")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b125tr", "1", "d")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b125tr", "z")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b125tr", "0", "-1")), []string{"C", "e", "z"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b125lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b125lx", "q")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b125lx", "w")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b125zs", "10", "a", "20", "b", "30", "c", "40", "d", "50", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b125zs", "10", "40", "BYSCORE", "WITHSCORES", "LIMIT", "1", "2")),
		[]string{"b", "20", "c", "30"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGE", "b125zs", "0", "1", "WITHSCORES")),
		[]string{"e", "50", "d", "40"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b125zs", "a")), "10")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b125zlex", "0", "m", "0", "n", "0", "o", "0", "p")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b125zrs", "b125zlex", "[m", "(p", "BYLEX")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b125zrs", "0", "-1")), []string{"m", "n", "o"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYLEX", "b125zlex", "(p", "[n")), []string{"o", "n"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCOUNT", "b125zs", "(20", "50")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "b125zs")), []string{"a", "10"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b125zu", "1", "b125zs", "WEIGHTS", "3", "AGGREGATE", "SUM")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b125zu", "0", "-1", "WITHSCORES")),
		[]string{"b", "60", "c", "90", "d", "120", "e", "150"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b125z1", "2", "a", "5", "b", "4", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b125z2", "9", "b", "1", "c", "7", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b125z1", "b125z2", "WEIGHTS", "1", "1", "AGGREGATE", "MIN", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER MIN: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFFSTORE", "b125zd", "2", "b125z1", "b125z2")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b125zd", "0", "-1")), []string{"a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b125z1", "b125z2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b125zm", "2", "a", "8", "h", "4", "c")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b125zm", "MAX", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "h") {
		t.Fatalf("ZMPOP MAX: %s", zmp.ToBytes())
	}
	bz := db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "b125zm", "0"))
	if protocol.IsErrorReply(bz) || !strings.Contains(string(bz.ToBytes()), "a") {
		t.Fatalf("BZPOPMIN: %s", bz.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b125zm", "NX", "3", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b125zm", "XX", "CH", "10", "c")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b125zm", "c")), "10")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINCRBY", "b125zm", "0.5", "b")), "3.5")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b125b1", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b125b2", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b125b2", "5", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "AND", "b125ba", "b125b1", "b125b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b125ba")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b125bx", "b125b1", "b125b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b125bx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ONE", "b125bone", "b125b1", "b125b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b125bone")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b125b2", "5")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b125b2", "1")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b125bf", "OVERFLOW", "WRAP", "SET", "u8", "0", "180", "GET", "u8", "0"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "180") {
		t.Fatalf("BITFIELD u8 WRAP: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b125st", "v", "EX", "50")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b125st", "EX", "80")), "v")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b125st", "w", "KEEPTTL")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b125st")), "w")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b125st", "PERSIST")), "w")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b125st")), -1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b125sr", "Hi")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b125sr", "0", "OK")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b125sr", "0", "1")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b125hello", "hello")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b125hello", "-3", "-1")), "llo")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCRBY", "b125dn", "5")), 5)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b125dn", "0.25")), "5.25")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b125m1", "1")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b125m1", "9", "b125m3", "3")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b125st", "20", "NX")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b125ex", "v", "EX", "10")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b125ex", "40", "XX")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b125sg", "old")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b125sg", "new", "XX", "GET")), "old")
	sg2 := db.Exec(nil, utils.ToCmdLine("SET", "b125sg2", "v", "GET"))
	if _, ok := sg2.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET GET miss: %T %s", sg2, sg2.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b125h", "f1", "v1", "f2", "7", "f3", "zz")), 3)
	hxx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b125h", "50", "XX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hxx) || !strings.Contains(string(hxx.ToBytes()), "0") {
		t.Fatalf("HEXPIRE XX: %s", hxx.ToBytes())
	}
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b125h", "60", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b125h", "90", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	hpg := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b125h", "200000", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hpg) || !strings.Contains(string(hpg.ToBytes()), "1") {
		t.Fatalf("HPEXPIRE GT: %s", hpg.ToBytes())
	}
	he := db.Exec(nil, utils.ToCmdLine("HGETEX", "b125h", "EX", "40", "FIELDS", "1", "f2"))
	if protocol.IsErrorReply(he) || !strings.Contains(string(he.ToBytes()), "7") {
		t.Fatalf("HGETEX EX: %s", he.ToBytes())
	}
	hp := db.Exec(nil, utils.ToCmdLine("HGETEX", "b125h", "PERSIST", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(hp) || !strings.Contains(string(hp.ToBytes()), "zz") {
		t.Fatalf("HGETEX PERSIST: %s", hp.ToBytes())
	}
	hd := db.Exec(nil, utils.ToCmdLine("HGETDEL", "b125h", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(hd) || !strings.Contains(string(hd.ToBytes()), "zz") {
		t.Fatalf("HGETDEL: %s", hd.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b125h", "f2", "3")), 10)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBYFLOAT", "b125h", "f2", "0.25")), "10.25")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b125h")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b125sa", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b125sb", "c", "d")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b125sd", "b125sa", "b125sb")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b125sd")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b125ss", "b125sa", "b125sb")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUNION", "b125ss")), []string{"c"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SINTER", "b125sa", "b125sb")), []string{"c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b125sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b125sp")), "only")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b125sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b125x", "1-0", "k", "v")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b125x", "2-0", "k", "w")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b125x", "4-0", "k", "x")), "4-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b125x", "2-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b125x", "MINID", "4-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b125x")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b125g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b125g", "FROMLONLAT", "15", "37.5", "BYBOX", "100", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH BYBOX: %s", gs.ToBytes())
	}
	gr := db.Exec(nil, utils.ToCmdLine("GEORADIUS", "b125g", "15.08", "37.5", "50", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gr) || !strings.Contains(string(gr.ToBytes()), "Catania") {
		t.Fatalf("GEORADIUS: %s", gr.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b125s1", "algorithm")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b125s2", "altruistic")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b125s1", "b125s2")), "alrit")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b125s1", "b125s2", "LEN")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b125sort", "9", "2", "7", "4")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b125sort", "DESC", "LIMIT", "0", "2")), []string{"9", "7"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b125p1", "m")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b125p2", "n")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b125pm", "b125p1", "b125p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b125pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b125s1", "b125s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b125s2", "b125s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p125")), "p125")
}
