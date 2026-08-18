package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 110 R4-1 extras: LPOP COUNT, LMOVE RIGHT LEFT, LINSERT BEFORE, LPOS RANK 2,
// LMPOP LEFT, BLMOVE LEFT LEFT, LTRIM mid, RPOPLPUSH, ZRANGE BYSCORE WS LIMIT,
// ZREVRANGE WS, ZUNIONSTORE SUM WEIGHTS, ZINTER MAX WS, ZRANGESTORE BYSCORE,
// ZDIFFSTORE, ZPOPMIN/ZPOPMAX/BZPOPMIN, BITOP OR, BITFIELD SET u8, GETEX PX,
// SET XX GET, HEXPIRE XX, HPEXPIRE GT, HGETEX EX, SDIFFSTORE, SINTERCARD,
// XRANGE COUNT, GEOSEARCH BYRADIUS, SORT, LCS, PFMERGE.
func TestR41Batch110Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b110l", "a", "b", "c", "d", "e", "f")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b110l", "1")), []string{"a"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b110l", "0", "-1")), []string{"b", "c", "d", "e", "f"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b110l", "b110l2", "RIGHT", "LEFT")), "f")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b110l", "0", "-1")), []string{"b", "c", "d", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b110l2", "0", "-1")), []string{"f"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b110ins", "a", "c", "e")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b110ins", "BEFORE", "c", "b")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b110ins", "0", "-1")), []string{"a", "b", "c", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b110lp", "a", "a", "b", "a")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b110lp", "a", "RANK", "2")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b110lp", "a", "COUNT", "2")), []string{"0", "1"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b110lmp", "1", "2", "3", "4", "5")), 5)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b110lmp", "LEFT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "1") {
		t.Fatalf("LMPOP LEFT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b110lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b110bl", "a", "b", "c")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b110bl", "b110bld", "LEFT", "LEFT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "a") {
		t.Fatalf("BLMOVE LEFT LEFT: %s", bl.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOPLPUSH", "b110bl", "b110bld")), "c")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b110bld", "0", "-1")), []string{"c", "a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b110lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b110lx", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b110lx", "b")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b110lx", "0", "0")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b110lx", "-1")), "a")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b110lt", "a", "b", "c", "d", "e")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b110lt", "1", "-2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b110lt", "0", "-1")), []string{"b", "c", "d"})
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b110lt", "1", "X")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b110lt", "1")), "X")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b110lt", "1", "X")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b110lt", "0", "-1")), []string{"b", "d"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b110z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b110z", "2", "4", "BYSCORE", "WITHSCORES", "LIMIT", "0", "2")),
		[]string{"b", "2", "c", "3"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGE", "b110z", "0", "2", "WITHSCORES")),
		[]string{"e", "5", "d", "4", "c", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCOUNT", "b110z", "(1", "4")), 3)
	zm := db.Exec(nil, utils.ToCmdLine("ZMSCORE", "b110z", "a", "missing", "e"))
	if protocol.IsErrorReply(zm) || !strings.Contains(string(zm.ToBytes()), "1") {
		t.Fatalf("ZMSCORE: %s", zm.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANK", "b110z", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANK", "b110z", "b")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "b110z")), []string{"a", "1"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYRANK", "b110z", "0", "0")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b110z", "0", "-1")), []string{"c", "d", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b110z")), []string{"e", "5"})
	bz := db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "b110z", "0"))
	if protocol.IsErrorReply(bz) || !strings.Contains(string(bz.ToBytes()), "c") {
		t.Fatalf("BZPOPMIN: %s", bz.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b110z1", "2", "a", "4", "b", "6", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b110z2", "1", "a", "8", "b", "3", "d")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b110zu", "2", "b110z1", "b110z2", "WEIGHTS", "2", "1", "AGGREGATE", "SUM")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b110zu", "0", "-1", "WITHSCORES")),
		[]string{"d", "3", "a", "5", "c", "12", "b", "16"})
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b110z1", "b110z2", "WEIGHTS", "1", "1", "AGGREGATE", "MAX", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "a") {
		t.Fatalf("ZINTER MAX: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b110zrs", "b110z1", "2", "6", "BYSCORE")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b110zrs", "0", "-1")), []string{"a", "b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFFSTORE", "b110zd", "2", "b110z1", "b110z2")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b110zd", "0", "-1")), []string{"c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b110z1", "b110z2", "LIMIT", "1")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b110za", "1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b110za", "NX", "9", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b110za", "CH", "4", "a", "9", "e")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b110za", "INCR", "1.5", "a")), "5.5")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b110zx", "0", "a", "0", "b", "0", "c", "0", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b110zx", "[b", "(d", "BYLEX")), []string{"b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b110zx", "[b", "(d")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b110zrl", "b110zx", "(a", "[c", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b110zrl", "0", "-1")), []string{"b", "c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b110b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b110b1", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b110b2", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b110b2", "4", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b110bo", "b110b1", "b110b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b110bo")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b110bo", "0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b110bo", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b110b1", "0", "0")), 2)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b110bf", "SET", "u8", "0", "42"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "0") {
		t.Fatalf("BITFIELD SET u8: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b110st", "v", "PX", "5000")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b110st", "PX", "8000")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b110st", "w")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b110st", "0", "Wo")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b110st", "0", "1")), "Wo")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUBSTR", "b110st", "-2", "-1")), "Wo")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("APPEND", "b110st", "!")), 3)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b110xx", "v")), "OK")
	xxg := db.Exec(nil, utils.ToCmdLine("SET", "b110xx", "w", "XX", "GET"))
	if protocol.IsErrorReply(xxg) || !strings.Contains(string(xxg.ToBytes()), "v") {
		t.Fatalf("SET XX GET: %s", xxg.ToBytes())
	}
	xx := db.Exec(nil, utils.ToCmdLine("SET", "b110missxx", "v", "XX"))
	if _, ok := xx.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET XX miss: %T %s", xx, xx.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("MSET", "b110m1", "1", "b110m2", "2")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b110n", "0.5")), "0.5")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SETEX", "b110sx", "100", "y")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b110sx", "50", "NX")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b110sx", "50", "XX")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETDEL", "b110sx")), "y")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETNX", "b110nx", "v")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETNX", "b110nx", "w")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIREAT", "b110xx", "2000000000")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRETIME", "b110xx")), 2000000000)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b110h", "a", "10", "b", "20")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b110h", "a", "x")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b110h", "c", "30")), 1)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b110h", "40", "NX", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hxx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b110h", "80", "XX", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hxx) || !strings.Contains(string(hxx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE XX: %s", hxx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b110h", "90000", "GT", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "1") {
		t.Fatalf("HPEXPIRE GT: %s", hgt.ToBytes())
	}
	hg := db.Exec(nil, utils.ToCmdLine("HGETEX", "b110h", "EX", "50", "FIELDS", "1", "b"))
	if protocol.IsErrorReply(hg) || !strings.Contains(string(hg.ToBytes()), "20") {
		t.Fatalf("HGETEX EX: %s", hg.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b110h", "a", "5")), 15)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSTRLEN", "b110h", "a")), 2)
	hd := db.Exec(nil, utils.ToCmdLine("HGETDEL", "b110h", "FIELDS", "1", "c"))
	if protocol.IsErrorReply(hd) || !strings.Contains(string(hd.ToBytes()), "30") {
		t.Fatalf("HGETDEL: %s", hd.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b110hk", "f", "v")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HKEYS", "b110hk")), []string{"f"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "b110hk")), "f")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b110sa", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b110sb", "b", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b110sd", "b110sa", "b110sb")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SDIFF", "b110sa", "b110sb")), []string{"a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "2", "b110sa", "b110sb", "LIMIT", "1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SMOVE", "b110sa", "b110sb", "a")), 1)
	sm := db.Exec(nil, utils.ToCmdLine("SMISMEMBER", "b110sb", "a", "z", "b"))
	if protocol.IsErrorReply(sm) || !strings.Contains(string(sm.ToBytes()), "0") {
		t.Fatalf("SMISMEMBER: %s", sm.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SINTER", "b110sd", "b110sb")), []string{"a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b110sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b110sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b110x", "10-0", "k", "v")), "10-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b110x", "11-0", "k", "w")), "11-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b110x", "12-0", "k", "x")), "12-0")
	xr := db.Exec(nil, utils.ToCmdLine("XRANGE", "b110x", "-", "+", "COUNT", "2"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "10-0") {
		t.Fatalf("XRANGE: %s", xr.ToBytes())
	}
	xrev := db.Exec(nil, utils.ToCmdLine("XREVRANGE", "b110x", "+", "-", "COUNT", "1"))
	if protocol.IsErrorReply(xrev) || !strings.Contains(string(xrev.ToBytes()), "12-0") {
		t.Fatalf("XREVRANGE: %s", xrev.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b110x", "10-0")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b110g", "13.361389", "38.115556", "Palermo")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b110g", "FROMLONLAT", "13", "38", "BYRADIUS", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Palermo") {
		t.Fatalf("GEOSEARCH BYRADIUS: %s", geo.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b110s1", "abcxyz")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b110s2", "abcdef")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b110s1", "b110s2")), "abc")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b110s1", "b110s2", "LEN")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b110sort", "3", "1", "2")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b110sort")), []string{"1", "2", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b110sorta", "c", "a", "b")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b110sorta", "ALPHA", "DESC")), []string{"c", "b", "a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b110p1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b110p2", "a", "b")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b110pm", "b110p1", "b110p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b110pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b110s1", "b110s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b110s2", "b110s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p110")), "p110")
}
