package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 103 R4-1 extras: LINSERT BEFORE/RPOPLPUSH/LMOVE, ZLEX*, ZUNIONSTORE MAX,
// ZINTERSTORE SUM, BITOP XOR/NOT/ONE, GETEX PERSIST, SET XX miss, HEXPIRE NX,
// HGETEX, ZPOPMAX COUNT, BZPOPMIN, BITFIELD WRAP, LPOS MAXLEN, GEOSEARCH.
func TestR41Batch103Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b103l", "w", "x", "y", "z")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b103l", "BEFORE", "y", "Y")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b103l", "0", "2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b103l", "0", "-1")), []string{"w", "x", "Y"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOPLPUSH", "b103l", "b103l2")), "Y")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b103lm", "a", "b", "c")), 3)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b103lm", "b103lm", "RIGHT", "LEFT")), "c")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b103lm", "0", "-1")), []string{"c", "a", "b"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b103lp", "a", "x", "a", "x", "a")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b103lp", "a", "COUNT", "0", "MAXLEN", "4")), []string{"0", "2"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b103zlex", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e", "0", "f")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYLEX", "b103zlex", "(b", "[e")), []string{"c", "d", "e"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b103zlex", "[c", "(f")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b103zlex", "[d", "[e")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b103z1", "2", "a", "4", "b", "6", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b103z2", "1", "a", "8", "b", "3", "d")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b103zu", "2", "b103z1", "b103z2", "WEIGHTS", "1", "2", "AGGREGATE", "MAX")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b103zu", "0", "-1", "WITHSCORES")),
		[]string{"a", "2", "c", "6", "d", "6", "b", "16"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b103zi", "2", "b103z1", "b103z2", "AGGREGATE", "SUM")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b103zi", "0", "-1", "WITHSCORES")),
		[]string{"a", "3", "b", "12"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b103b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b103b1", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b103b2", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b103bx", "b103b1", "b103b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b103bx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "NOT", "b103bn", "b103b1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b103bn", "0")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ONE", "b103bo", "b103b1", "b103b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b103bo")), 1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b103s1", "abcde")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b103s2", "abxyz")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b103s1", "b103s2", "LEN")), 2)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b103st", "v", "PX", "5000")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b103st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b103st")), -1)

	xx := db.Exec(nil, utils.ToCmdLine("SET", "b103nx1", "v", "XX"))
	if _, ok := xx.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET XX miss: %T %s", xx, xx.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b103nx1", "v", "NX")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b103nx1", "w", "GET")), "v")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b103h", "f1", "v1", "f2", "v2")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b103h", "f1", "x")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b103h", "f3", "v3")), 1)
	hexp := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b103h", "90", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hexp) {
		t.Fatalf("HEXPIRE NX: %s", hexp.ToBytes())
	}
	hg := db.Exec(nil, utils.ToCmdLine("HGETEX", "b103h", "EX", "50", "FIELDS", "1", "f2"))
	if protocol.IsErrorReply(hg) || !strings.Contains(string(hg.ToBytes()), "v2") {
		t.Fatalf("HGETEX: %s", hg.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b103sa", "a")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b103sa")), "a")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b103zr", "3", "m")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANDMEMBER", "b103zr")), "m")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b103zp", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b103zp")), []string{"c", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCARD", "b103zp")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b103zc", "1", "a", "2", "b", "3", "c", "4", "d")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYRANK", "b103zc", "0", "1")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b103zc", "0", "-1")), []string{"c", "d"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b103zm", "1", "a", "2", "b", "3", "c")), 3)
	bz := db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "b103zm", "0"))
	if protocol.IsErrorReply(bz) || !strings.Contains(string(bz.ToBytes()), "a") {
		t.Fatalf("BZPOPMIN: %s", bz.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b103n", "5")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECR", "b103n")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCR", "b103n")), 5)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b103x", "5-0", "f", "a")), "5-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b103x", "6-0", "f", "b")), "6-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b103x", "5-0")), 1)
	xr := db.Exec(nil, utils.ToCmdLine("XREVRANGE", "b103x", "+", "-", "COUNT", "1"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "6-0") {
		t.Fatalf("XREVRANGE: %s", xr.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b103g", "15.087269", "37.502669", "Catania")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b103g", "FROMLONLAT", "15", "37", "BYRADIUS", "80", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH: %s", geo.ToBytes())
	}

	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b103bf", "OVERFLOW", "WRAP", "INCRBY", "u4", "0", "15"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "15") {
		t.Fatalf("BITFIELD WRAP: %s", bf.ToBytes())
	}
	bf2 := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b103bf", "OVERFLOW", "WRAP", "INCRBY", "u4", "0", "2"))
	if protocol.IsErrorReply(bf2) || !strings.Contains(string(bf2.ToBytes()), "1") {
		t.Fatalf("BITFIELD WRAP2: %s", bf2.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b103bit", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b103bit", "0")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETNX", "b103sn", "v")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETNX", "b103sn", "w")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETDEL", "b103sn")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b103miss", "10", "XX")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PERSIST", "b103miss")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "b103")), "b103")
}
