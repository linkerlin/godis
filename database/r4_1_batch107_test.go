package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 107 R4-1 extras: RPOP COUNT, LMOVE LEFT LEFT, LPOS RANK, LREM 0,
// BLMOVE LEFT RIGHT, ZRANGE BYLEX LIMIT/REV WS, ZUNIONSTORE MAX WEIGHTS,
// ZINTER MIN WS, BITOP DIFF1/ANDOR, BITFIELD i16 WRAP, GETEX PX, SET NX GET,
// HGETEX PERSIST, HEXPIRE XX, SDIFFSTORE, ZPOPMAX, XTRIM MINID,
// GEOSEARCH FROMMEMBER, LCS, SORT DESC, ZMPOP MAX, SINTERCARD LIMIT.
func TestR41Batch107Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b107l", "a", "b", "c", "d", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b107l", "2")), []string{"e", "d"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b107l", "0", "-1")), []string{"a", "b", "c"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b107l", "b107l", "LEFT", "LEFT")), "a")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b107l", "0", "-1")), []string{"a", "b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b107l", "c", "RANK", "1")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b107l", "0", "a")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b107l", "0", "-1")), []string{"b", "c"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b107l", "-1")), "c")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b107l2", "x", "y", "z")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b107l2", "b107l2d", "LEFT", "RIGHT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "x") {
		t.Fatalf("BLMOVE: %s", bl.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b107l2", "0", "-1")), []string{"y", "z"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b107l2d", "0", "-1")), []string{"x"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b107l3", "1", "2", "3", "4")), 4)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b107l3", "LEFT", "COUNT", "1"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "1") {
		t.Fatalf("LMPOP: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b107l3")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b107ins", "a", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b107ins", "BEFORE", "c", "b")), 3)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b107ins", "0", "1")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b107ins", "0", "-1")), []string{"a", "b"})
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b107ins", "1", "X")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b107ins", "1")), "X")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b107lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b107lx", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b107lx", "z")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b107lx", "0", "-1")), []string{"z", "a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b107lx", "b")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b107z", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b107z", "[b", "(e", "BYLEX")), []string{"b", "c", "d"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b107z", "[b", "(e", "BYLEX", "LIMIT", "0", "2")), []string{"b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b107z", "[b", "(e")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b107z", "0", "-1", "REV", "WITHSCORES")),
		[]string{"e", "0", "d", "0", "c", "0", "b", "0", "a", "0"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b107z", "[c", "(e")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b107z", "0", "-1")), []string{"a", "b", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b107zs", "1", "a", "2", "b", "3", "c", "4", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYSCORE", "b107zs", "(1", "4", "WITHSCORES", "LIMIT", "0", "2")),
		[]string{"b", "2", "c", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCOUNT", "b107zs", "(1", "4")), 3)
	zm := db.Exec(nil, utils.ToCmdLine("ZMSCORE", "b107zs", "a", "c", "missing"))
	if protocol.IsErrorReply(zm) || !strings.Contains(string(zm.ToBytes()), "1") {
		t.Fatalf("ZMSCORE: %s", zm.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANK", "b107zs", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANK", "b107zs", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b107zu", "1", "b107zs", "WEIGHTS", "2", "AGGREGATE", "MAX")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b107zu", "0", "-1", "WITHSCORES")),
		[]string{"a", "2", "b", "4", "c", "6", "d", "8"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b107zrs", "b107zs", "2", "4", "BYSCORE")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b107zrs", "0", "-1")), []string{"b", "c", "d"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b107zs")), []string{"d", "4"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b107zi1", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b107zi2", "5", "b", "1", "c", "9", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b107zi1", "b107zi2", "WEIGHTS", "1", "1", "AGGREGATE", "MIN", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER MIN: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b107zi1", "b107zi2", "LIMIT", "1")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFF", "2", "b107zi1", "b107zi2")), []string{"a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b107zp", "1", "a", "2", "b", "3", "c")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b107zp", "MAX", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "c") {
		t.Fatalf("ZMPOP MAX: %s", zmp.ToBytes())
	}
	bz := db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "b107zp", "0"))
	if protocol.IsErrorReply(bz) || !strings.Contains(string(bz.ToBytes()), "a") {
		t.Fatalf("BZPOPMIN: %s", bz.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b107za", "1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b107za", "NX", "9", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b107za", "XX", "5", "d")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b107za", "CH", "4", "a", "9", "e")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b107za", "INCR", "1.5", "a")), "5.5")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b107b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b107b1", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b107b2", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b107b2", "4", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "DIFF1", "b107bd1", "b107b1", "b107b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b107bd1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b107bd1", "4")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ANDOR", "b107bao", "b107b1", "b107b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b107bao")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b107bf", "OVERFLOW", "WRAP", "INCRBY", "i16", "0", "40000"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "-25536") {
		t.Fatalf("BITFIELD WRAP i16: %s", bf.ToBytes())
	}

	nx := db.Exec(nil, utils.ToCmdLine("SET", "b107st", "hello", "NX", "GET"))
	if _, ok := nx.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET NX GET: %T %s", nx, nx.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b107st", "PX", "5000")), "hello")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b107st", "world")), "hello")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b107st", "0", "Wo")), 5)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b107st", "0", "1")), "Wo")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUBSTR", "b107st", "-2", "-1")), "ld")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("APPEND", "b107st", "!")), 6)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECRBY", "b107n", "3")), -3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCR", "b107n")), -2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b107n", "0.5")), "-1.5")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b107ex", "v", "EX", "100")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b107ex", "50", "XX")), 1)
	xxg := db.Exec(nil, utils.ToCmdLine("SET", "b107ex", "w", "XX", "GET"))
	if protocol.IsErrorReply(xxg) || !strings.Contains(string(xxg.ToBytes()), "v") {
		t.Fatalf("SET XX GET: %s", xxg.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETDEL", "b107ex")), "w")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b107h", "f1", "v1", "f2", "20")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b107h", "f1", "x")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b107h", "f3", "v3")), 1)
	hg := db.Exec(nil, utils.ToCmdLine("HGETEX", "b107h", "EX", "40", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hg) || !strings.Contains(string(hg.ToBytes()), "v1") {
		t.Fatalf("HGETEX: %s", hg.ToBytes())
	}
	hxx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b107h", "80", "XX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hxx) || !strings.Contains(string(hxx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE XX: %s", hxx.ToBytes())
	}
	hp := db.Exec(nil, utils.ToCmdLine("HGETEX", "b107h", "PERSIST", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hp) || !strings.Contains(string(hp.ToBytes()), "v1") {
		t.Fatalf("HGETEX PERSIST: %s", hp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b107h", "f2", "5")), 25)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBYFLOAT", "b107h", "f2", "0.5")), "25.5")
	hd := db.Exec(nil, utils.ToCmdLine("HGETDEL", "b107h", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(hd) || !strings.Contains(string(hd.ToBytes()), "v3") {
		t.Fatalf("HGETDEL: %s", hd.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HDEL", "b107h", "f2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b107hk", "f", "v")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HKEYS", "b107hk")), []string{"f"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "b107hk")), "f")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b107sa", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b107sb", "b", "c", "d")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b107sd", "b107sa", "b107sb")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "2", "b107sa", "b107sb", "LIMIT", "1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SMOVE", "b107sa", "b107sb", "a")), 1)
	sm := db.Exec(nil, utils.ToCmdLine("SMISMEMBER", "b107sb", "a", "z", "b"))
	if protocol.IsErrorReply(sm) || !strings.Contains(string(sm.ToBytes()), "0") {
		t.Fatalf("SMISMEMBER: %s", sm.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b107sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b107sp")), "only")
	spMiss := db.Exec(nil, utils.ToCmdLine("SPOP", "b107sp"))
	if _, ok := spMiss.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SPOP miss: %T %s", spMiss, spMiss.ToBytes())
	}

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b107x", "1-0", "k", "v")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b107x", "2-0", "k", "w")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b107x", "3-0", "k", "x")), "3-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b107x", "MINID", "2-0")), 1)
	xr := db.Exec(nil, utils.ToCmdLine("XRANGE", "b107x", "-", "+", "COUNT", "1"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "2-0") {
		t.Fatalf("XRANGE: %s", xr.ToBytes())
	}
	xrev := db.Exec(nil, utils.ToCmdLine("XREVRANGE", "b107x", "+", "-", "COUNT", "1"))
	if protocol.IsErrorReply(xrev) || !strings.Contains(string(xrev.ToBytes()), "3-0") {
		t.Fatalf("XREVRANGE: %s", xrev.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b107g", "13.361389", "38.115556", "Palermo")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b107g", "FROMMEMBER", "Palermo", "BYRADIUS", "1", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Palermo") {
		t.Fatalf("GEOSEARCH FROMMEMBER: %s", geo.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b107p1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b107p2", "a", "b")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b107pm", "b107p1", "b107p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b107pm")), 2)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b107lcs1", "abcdef")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b107lcs2", "abcdxy")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b107lcs1", "b107lcs2")), "abcd")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b107lcs1", "b107lcs2", "LEN")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b107sort", "c", "a", "b")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b107sort", "ALPHA", "DESC")), []string{"c", "b", "a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b107zr", "1", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANDMEMBER", "b107zr")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p107")), "p107")
}
