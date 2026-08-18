package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 105 R4-1 extras: LPOP/RPOP COUNT, LINSERT BEFORE, LMOVE RIGHT LEFT,
// LPOS RANK/MAXLEN, ZREVRANGE WS, ZUNION SUM WEIGHTS, ZINTERSTORE MIN,
// BITOP XOR/OR/NOT, BITFIELD i8 SAT, GETEX EX, HEXPIRE LT, HPEXPIRE XX,
// SUNIONSTORE, LMPOP RIGHT, ZMPOP MIN, INCRBYFLOAT, SORT LIMIT.
func TestR41Batch105Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b105l", "a", "b", "c", "d", "e", "f")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b105l", "2")), []string{"a", "b"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b105l")), "f")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b105l", "BEFORE", "d", "X")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b105l", "0", "-1")), []string{"c", "X", "d", "e"})
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b105l", "1", "Y")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b105l", "-1")), "e")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b105l", "1", "c")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b105l", "0", "-1")), []string{"Y", "d", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b105lp", "a", "x", "a", "x", "a")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b105lp", "a", "COUNT", "0", "MAXLEN", "4")), []string{"0", "2"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b105lp", "a", "RANK", "-1")), 4)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b105lm", "a", "b", "c")), 3)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b105lm", "b105lm2", "RIGHT", "LEFT")), "c")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b105lm", "0", "-1")), []string{"a", "b"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b105lm2", "0", "-1")), []string{"c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b105z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGE", "b105z", "0", "2", "WITHSCORES")),
		[]string{"e", "5", "d", "4", "c", "3"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b105z", "5", "1", "LIMIT", "1", "2")),
		[]string{"d", "c"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b105z", "1", "3", "WITHSCORES")),
		[]string{"b", "2", "c", "3", "d", "4"})
	zm := db.Exec(nil, utils.ToCmdLine("ZMSCORE", "b105z", "a", "c", "missing"))
	if protocol.IsErrorReply(zm) || !strings.Contains(string(zm.ToBytes()), "1") {
		t.Fatalf("ZMSCORE: %s", zm.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYSCORE", "b105z", "(1", "4")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b105z", "0", "-1")), []string{"a", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b105z1", "2", "a", "4", "b", "6", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b105z2", "1", "a", "8", "b", "3", "d")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZUNION", "2", "b105z1", "b105z2", "WEIGHTS", "1", "2", "AGGREGATE", "SUM", "WITHSCORES")),
		[]string{"a", "4", "c", "6", "d", "6", "b", "20"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b105zi", "2", "b105z1", "b105z2", "WEIGHTS", "2", "1", "AGGREGATE", "MIN")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b105zi", "0", "-1", "WITHSCORES")),
		[]string{"a", "1", "b", "8"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFF", "2", "b105z1", "b105z2", "WITHSCORES")),
		[]string{"c", "6"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b105b1", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b105b1", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b105b2", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b105bx", "b105b1", "b105b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b105bx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b105bo", "b105b1", "b105b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b105bo")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "NOT", "b105bn", "b105b1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b105bn", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b105b1", "1")), 1)

	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b105bf", "OVERFLOW", "SAT", "INCRBY", "i8", "0", "200"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "127") {
		t.Fatalf("BITFIELD SAT i8: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b105st", "hello", "EX", "90")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b105st", "EX", "50")), "hello")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETDEL", "b105st")), "hello")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b105nx", "v", "NX")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b105nx", "w", "NX", "GET")), "v")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b105s1", "abcxyz")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b105s2", "ab12yz")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b105s1", "b105s2", "LEN")), 4)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b105h", "a", "10", "b", "20", "c", "30")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HMGET", "b105h", "a", "miss", "c")), []string{"10", "", "30"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b105h", "a", "5")), 15)
	hexp := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b105h", "80", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hexp) {
		t.Fatalf("HEXPIRE: %s", hexp.ToBytes())
	}
	hlt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b105h", "20", "LT", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hlt) || !strings.Contains(string(hlt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE LT: %s", hlt.ToBytes())
	}
	hpxx := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b105h", "6000", "XX", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hpxx) || !strings.Contains(string(hpxx.ToBytes()), "1") {
		t.Fatalf("HPEXPIRE XX: %s", hpxx.ToBytes())
	}
	hg := db.Exec(nil, utils.ToCmdLine("HGETEX", "b105h", "PX", "4000", "FIELDS", "1", "b"))
	if protocol.IsErrorReply(hg) || !strings.Contains(string(hg.ToBytes()), "20") {
		t.Fatalf("HGETEX PX: %s", hg.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b105sa", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b105sb", "b", "c", "d")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SUNIONSTORE", "b105su", "b105sa", "b105sb")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b105si", "b105sa", "b105sb")), 2)
	sm := db.Exec(nil, utils.ToCmdLine("SMISMEMBER", "b105sa", "a", "z", "c"))
	if protocol.IsErrorReply(sm) || !strings.Contains(string(sm.ToBytes()), "0") {
		t.Fatalf("SMISMEMBER: %s", sm.ToBytes())
	}

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b105x", "1-0", "f", "a")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b105x", "2-0", "f", "b")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b105x", "3-0", "f", "c")), "3-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b105x", "MAXLEN", "2")), 1)
	xr := db.Exec(nil, utils.ToCmdLine("XREVRANGE", "b105x", "+", "-", "COUNT", "1"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "3-0") {
		t.Fatalf("XREVRANGE: %s", xr.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b105g", "15.087269", "37.502669", "Catania")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEORADIUS", "b105g", "15", "37", "80", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Catania") {
		t.Fatalf("GEORADIUS: %s", geo.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b105lmp", "1", "2", "3", "4", "5")), 5)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b105lmp", "RIGHT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "5") {
		t.Fatalf("LMPOP RIGHT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b105zm", "1", "a", "2", "b", "3", "c", "4", "d")), 4)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b105zm", "MIN", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "a") {
		t.Fatalf("ZMPOP MIN: %s", zmp.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b105n", "4")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECR", "b105n")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCRBY", "b105n", "3")), 6)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b105n", "0.5")), "6.5")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBYFLOAT", "b105hf", "f", "1.25")), "1.25")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINCRBY", "b105zi", "1.5", "m")), "1.5")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b105pf", "a", "b", "c")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b105c1", "hello")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b105c1", "b105c4")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PEXPIRE", "b105c4", "8000", "NX")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b105rx", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b105rx", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b105sort", "9", "1", "5", "3")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b105sort", "LIMIT", "1", "2")), []string{"3", "5"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b105sa1", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b105sa1")), "only")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b105zr", "3", "m")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANDMEMBER", "b105zr")), "m")

	xx := db.Exec(nil, utils.ToCmdLine("SET", "b105xx", "v", "XX"))
	if _, ok := xx.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET XX miss: %T %s", xx, xx.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b105miss", "10", "XX")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PERSIST", "b105miss")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "b105")), "b105")
}
