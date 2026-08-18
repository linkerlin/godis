package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 106 R4-1 extras: LTRIM/RPOPLPUSH/BLMOVE, ZRANGESTORE BYLEX, ZINTERCARD LIMIT,
// BITOP AND/DIFF, BITFIELD u16 SAT, GETEX PERSIST, HGETDEL, HEXPIRE NX/GT,
// SDIFFSTORE/SINTERCARD, ZPOPMIN/BZPOPMIN, XRANGE, GEOSEARCH BYBOX, MSETNX,
// LPUSHX, ZADD CH, SORT ALPHA, PFCOUNT multi, BITCOUNT range. Crosses 3000.
func TestR41Batch106Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b106l", "a", "b", "c", "d", "e")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b106l", "1", "3")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b106l", "0", "-1")), []string{"b", "c", "d"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOPLPUSH", "b106l", "b106l2")), "d")
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b106l", "b106l2", "LEFT", "RIGHT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "b") {
		t.Fatalf("BLMOVE: %s", bl.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b106l2", "0", "-1")), []string{"d", "b"})
	lpos := db.Exec(nil, utils.ToCmdLine("LPOS", "b106l2", "d", "COUNT", "0"))
	if protocol.IsErrorReply(lpos) || !strings.Contains(string(lpos.ToBytes()), "0") {
		t.Fatalf("LPOS COUNT: %s", lpos.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b106zl", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b106zls", "b106zl", "(a", "[d", "BYLEX")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b106zls", "0", "-1")), []string{"b", "c", "d"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b106zl", "[b", "(e")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b106zu", "1", "b106zl", "AGGREGATE", "MAX")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "1", "b106zl", "LIMIT", "2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b106b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b106b1", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b106b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "AND", "b106ba", "b106b1", "b106b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b106ba")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "DIFF", "b106bd", "b106b1", "b106b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b106bd")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b106bf", "OVERFLOW", "SAT", "INCRBY", "u16", "0", "70000"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "65535") {
		t.Fatalf("BITFIELD SAT u16: %s", bf.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b106h", "f1", "v1", "f2", "v2")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b106h", "f1", "x")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b106h", "f3", "v3")), 1)
	hd := db.Exec(nil, utils.ToCmdLine("HGETDEL", "b106h", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(hd) || !strings.Contains(string(hd.ToBytes()), "v3") {
		t.Fatalf("HGETDEL: %s", hd.ToBytes())
	}
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b106h", "60", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b106h", "120", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HDEL", "b106h", "f2")), 1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b106st", "v", "EX", "50")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b106st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b106st")), -1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b106st", "w", "KEEPTTL")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b106st", "x")), "w")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SETEX", "b106sx", "20", "y")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("APPEND", "b106sx", "!")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b106sx", "0", "1")), "y!")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b106sa", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b106sb", "b", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b106sd", "b106sa", "b106sb")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "2", "b106sa", "b106sb")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b106zp", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "b106zp")), []string{"a", "1"})
	bz := db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "b106zp", "0"))
	if protocol.IsErrorReply(bz) || !strings.Contains(string(bz.ToBytes()), "b") {
		t.Fatalf("BZPOPMIN: %s", bz.ToBytes())
	}

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b106x", "8-0", "k", "v")), "8-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b106x", "9-0", "k", "w")), "9-0")
	xr := db.Exec(nil, utils.ToCmdLine("XRANGE", "b106x", "-", "+", "COUNT", "1"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "8-0") {
		t.Fatalf("XRANGE: %s", xr.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b106g", "13.361389", "38.115556", "Palermo")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b106g", "FROMLONLAT", "13", "38", "BYBOX", "100", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Palermo") {
		t.Fatalf("GEOSEARCH BYBOX: %s", geo.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b106m1", "1", "b106m2", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b106m1", "9", "b106m3", "3")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b106lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSH", "b106lx", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b106lx", "z")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b106lx", "0", "-1")), []string{"z", "a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b106zc", "1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b106zc", "CH", "2", "a", "3", "b")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b106zc", "a")), "2")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b106lx", "ALPHA")), []string{"a", "z"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b106p1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b106p2", "a", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b106p1", "b106p2")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b106bit", "0", "0")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b106bit", "8", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b106bit", "1", "1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b106bit", "0")), 0)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TYPE", "b106lx")), "list")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "b106m1", "b106m2", "b106miss")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETDEL", "b106m2")), "2")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b106hk", "f", "v")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HKEYS", "b106hk")), []string{"f"})
	spMiss := db.Exec(nil, utils.ToCmdLine("SPOP", "b106sp"))
	if _, ok := spMiss.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SPOP miss: %T %s", spMiss, spMiss.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b106sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b106sp")), "only")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b106zrnd", "1", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANDMEMBER", "b106zrnd")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p106")), "p106")
}
