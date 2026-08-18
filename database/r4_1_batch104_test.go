package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 104 R4-1 extras: LINSERT AFTER/LSET/LREM/LMOVE/LMPOP LEFT, ZRANGEBYSCORE
// WS+LIMIT, ZUNIONSTORE MIN WEIGHTS, ZINTER WEIGHTS, ZRANGESTORE BYSCORE,
// BITOP DIFF1/ANDOR/ONE, GETEX PX, HEXPIRE NX/GT, HGETDEL/HGETEX, SINTERCARD
// LIMIT, GEOSEARCH BYBOX, LCS, SORT ALPHA, ZPOPMIN, ZADD NX/XX/CH/GT, KEEPTTL.
func TestR41Batch104Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b104l", "a", "b", "c", "d")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b104l", "AFTER", "b", "B")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b104l", "0", "-1")), []string{"a", "b", "B", "c", "d"})
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b104l", "2", "X")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b104l", "2")), "X")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b104l", "1", "X")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b104l", "0", "-1")), []string{"a", "b", "c", "d"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b104l", "AFTER", "missing", "Y")), -1)
	asserts.AssertNullBulk(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b104l", "missing")))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b104l", "1", "2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b104l", "0", "-1")), []string{"b", "c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b104lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b104lx", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b104lx", "z")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b104lx", "0", "-1")), []string{"z", "a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b104lm", "a", "b", "c")), 3)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b104lm", "b104lm", "LEFT", "RIGHT")), "a")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b104lm", "0", "-1")), []string{"b", "c", "a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b104lp", "1", "2", "3", "4")), 4)
	lm := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b104lp", "LEFT", "COUNT", "2"))
	if protocol.IsErrorReply(lm) || !strings.Contains(string(lm.ToBytes()), "1") {
		t.Fatalf("LMPOP: %s", lm.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b104lp")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b104z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYSCORE", "b104z", "2", "4", "WITHSCORES")),
		[]string{"b", "2", "c", "3", "d", "4"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYSCORE", "b104z", "(2", "4", "LIMIT", "0", "1")), []string{"c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCOUNT", "b104z", "2", "4")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANK", "b104z", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANK", "b104z", "b")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b104z", "0", "-1", "REV")),
		[]string{"e", "d", "c", "b", "a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b104z1", "2", "a", "4", "b", "6", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b104z2", "1", "a", "8", "b", "3", "d")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b104zu", "2", "b104z1", "b104z2", "WEIGHTS", "2", "1", "AGGREGATE", "MIN")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b104zu", "0", "-1", "WITHSCORES")),
		[]string{"a", "1", "d", "3", "b", "8", "c", "12"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b104zi", "2", "b104z1", "b104z2", "WEIGHTS", "1", "1", "AGGREGATE", "MAX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b104zi", "0", "-1", "WITHSCORES")),
		[]string{"a", "2", "b", "8"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b104z1", "b104z2", "WEIGHTS", "1", "2", "WITHSCORES")),
		[]string{"a", "4", "b", "20"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b104zs", "1", "a", "2", "b", "3", "c", "4", "d")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b104zst", "b104zs", "2", "3", "BYSCORE")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b104zst", "0", "-1")), []string{"b", "c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b104zd1", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b104zd2", "1", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFFSTORE", "b104zdd", "2", "b104zd1", "b104zd2")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b104zdd", "0", "-1")), []string{"a", "c"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFF", "2", "b104zd1", "b104zd2", "WITHSCORES")),
		[]string{"a", "1", "c", "3"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b104b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b104b1", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b104b2", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b104b2", "4", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "DIFF1", "b104bd1", "b104b1", "b104b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b104bd1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b104bd1", "4")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ANDOR", "b104bao", "b104b1", "b104b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b104bao")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "DIFF", "b104bd", "b104b1", "b104b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b104bd")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ONE", "b104bo", "b104b1", "b104b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b104bo")), 2)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b104st", "v", "PX", "5000")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b104st", "PX", "8000")), "v")
	xx := db.Exec(nil, utils.ToCmdLine("SET", "b104nx", "v", "XX"))
	if _, ok := xx.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET XX miss: %T %s", xx, xx.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b104nx", "v", "NX")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b104nx", "w", "GET")), "v")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SETEX", "b104sx", "10", "old")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b104sx", "new")), "old")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b104lcs1", "abcdef")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b104lcs2", "abxyz")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b104lcs1", "b104lcs2")), "ab")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b104lcs1", "b104lcs2", "LEN")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b104g", "13.361389", "38.115556", "Palermo")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b104g", "FROMLONLAT", "13", "38", "BYBOX", "200", "200", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Palermo") {
		t.Fatalf("GEOSEARCH BYBOX: %s", geo.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b104sa", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b104sb", "b", "c", "d")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "2", "b104sa", "b104sb", "LIMIT", "1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b104sd", "b104sa", "b104sb")), 1)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b104x", "10-0", "f", "a")), "10-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b104x", "11-0", "f", "b")), "11-0")
	xr := db.Exec(nil, utils.ToCmdLine("XRANGE", "b104x", "-", "+", "COUNT", "1"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "10-0") {
		t.Fatalf("XRANGE: %s", xr.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b104sort", "d", "a", "c", "b")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b104sort", "ALPHA")), []string{"a", "b", "c", "d"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b104sort", "ALPHA", "DESC", "LIMIT", "0", "2")), []string{"d", "c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b104zp", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "b104zp")), []string{"a", "1"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b104zlex", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYLEX", "b104zlex", "(a", "[d")), []string{"b", "c", "d"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b104zlex", "[b", "(e")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b104zlex", "[c", "[d")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b104h", "f1", "v1", "f2", "v2", "f3", "v3")), 3)
	hd := db.Exec(nil, utils.ToCmdLine("HGETDEL", "b104h", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(hd) || !strings.Contains(string(hd.ToBytes()), "v3") {
		t.Fatalf("HGETDEL: %s", hd.ToBytes())
	}
	hxx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b104h", "90", "XX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hxx) {
		t.Fatalf("HEXPIRE XX: %s", hxx.ToBytes())
	}
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b104h", "90", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt0 := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b104h", "30", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt0) {
		t.Fatalf("HEXPIRE GT fail: %s", hgt0.ToBytes())
	}
	hgt1 := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b104h", "120", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt1) {
		t.Fatalf("HEXPIRE GT: %s", hgt1.ToBytes())
	}
	hg := db.Exec(nil, utils.ToCmdLine("HGETEX", "b104h", "EX", "40", "FIELDS", "1", "f2"))
	if protocol.IsErrorReply(hg) || !strings.Contains(string(hg.ToBytes()), "v2") {
		t.Fatalf("HGETEX: %s", hg.ToBytes())
	}

	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b104bf", "GET", "u8", "0"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "0") {
		t.Fatalf("BITFIELD GET: %s", bf.ToBytes())
	}
	bf2 := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b104bf", "OVERFLOW", "WRAP", "INCRBY", "u4", "0", "2"))
	if protocol.IsErrorReply(bf2) || !strings.Contains(string(bf2.ToBytes()), "2") {
		t.Fatalf("BITFIELD WRAP: %s", bf2.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b104zc", "1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b104zc", "NX", "2", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b104zc", "XX", "CH", "3", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b104zc", "GT", "4", "a")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b104zc", "a")), "4")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b104kt", "v", "EX", "100")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b104kt", "w", "KEEPTTL")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b104kt")), "w")
	asserts.AssertIntReplyGreaterThan(t, db.Exec(nil, utils.ToCmdLine("TTL", "b104kt")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b104bl", "a", "b")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("BLMOVE", "b104bl", "b104bl2", "RIGHT", "LEFT", "0")), "b")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("MSET", "b104m1", "1")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b104m1", "2", "b104m2", "3")), 0)

	asserts.AssertNullBulk(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b104lmiss", "x")))
	asserts.AssertNullBulk(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b104zmiss", "m")))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("STRLEN", "b104smiss")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b104miss", "10", "XX")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PERSIST", "b104miss")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "b104")), "b104")
}
