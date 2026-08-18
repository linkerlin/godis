package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 114 R4-1 extras: LPOP COUNT 3, LMOVE RIGHT RIGHT, LINSERT BEFORE,
// LPOS MAXLEN / RANK -1, LMPOP RIGHT COUNT 1, BLMOVE LEFT LEFT, LSET/LREM 0,
// ZRANGE BYLEX LIMIT, ZREMRANGEBYLEX, ZREVRANGEBYSCORE WS LIMIT, ZRANGE REV,
// ZRANGESTORE BYLEX, ZUNION MAX WS, ZINTERSTORE SUM WEIGHTS, ZINTER MIN WS,
// ZADD GT/LT/INCR, ZMPOP MIN, BITOP NOT/ONE, BITFIELD i8 SAT/WRAP, GETEX PX,
// SET XX miss, HEXPIRE LT, HPEXPIRE XX, HGETEX PX, SUNIONSTORE, XTRIM MINID,
// GEOSEARCH BYRADIUS, SORT LIMIT, LCS.
func TestR41Batch114Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b114l", "a", "b", "c", "d", "e", "f")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b114l", "3")), []string{"a", "b", "c"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b114l", "0", "-1")), []string{"d", "e", "f"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b114l", "b114l2", "RIGHT", "RIGHT")), "f")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b114l", "0", "-1")), []string{"d", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b114l2", "0", "-1")), []string{"f"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b114ins", "a", "c", "e")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b114ins", "BEFORE", "c", "b")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b114ins", "0", "-1")), []string{"a", "b", "c", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b114lp", "x", "y", "z", "y", "x")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b114lp", "y", "MAXLEN", "3")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b114lp", "x", "RANK", "-1")), 4)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b114lmp", "1", "2", "3", "4", "5")), 5)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b114lmp", "RIGHT", "COUNT", "1"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "5") {
		t.Fatalf("LMPOP RIGHT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b114lmp")), 4)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b114bl", "a", "b", "c")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b114bl", "b114bld", "LEFT", "LEFT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "a") {
		t.Fatalf("BLMOVE LEFT LEFT: %s", bl.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b114bld", "0", "-1")), []string{"a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b114lt", "a", "b", "c", "d")), 4)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b114lt", "0", "X")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b114lt", "0")), "X")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b114lt", "0", "b")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b114lt", "0", "-1")), []string{"X", "c", "d"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b114lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b114lx", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b114lx", "z")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b114lx", "0", "-1")), []string{"z", "a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b114z", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b114z", "[b", "(e", "BYLEX", "LIMIT", "1", "2")), []string{"c", "d"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b114z", "[c", "(e")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b114z", "0", "-1")), []string{"a", "b", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b114zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b114zs", "4", "2", "WITHSCORES", "LIMIT", "0", "2")),
		[]string{"d", "4", "c", "3"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b114zs", "0", "2", "REV")), []string{"e", "d", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b114zrs", "b114zs", "(a", "[c", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b114zrs", "0", "-1")), []string{"b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCOUNT", "b114zs", "2", "4")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANK", "b114zs", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANK", "b114zs", "b")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b114z1", "3", "a", "5", "b", "7", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b114z2", "4", "a", "1", "b", "9", "d")), 3)
	zu := db.Exec(nil, utils.ToCmdLine("ZUNION", "2", "b114z1", "b114z2", "WEIGHTS", "1", "1", "AGGREGATE", "MAX", "WITHSCORES"))
	if protocol.IsErrorReply(zu) || !strings.Contains(string(zu.ToBytes()), "d") {
		t.Fatalf("ZUNION MAX: %s", zu.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b114zi", "2", "b114z1", "b114z2", "WEIGHTS", "1", "2", "AGGREGATE", "SUM")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b114zi", "0", "-1", "WITHSCORES")), []string{"b", "7", "a", "11"})
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b114z1", "b114z2", "AGGREGATE", "MIN", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "a") {
		t.Fatalf("ZINTER MIN: %s", zi.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFF", "2", "b114z1", "b114z2")), []string{"c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b114z1", "b114z2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b114za", "5", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b114za", "GT", "8", "a")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b114za", "a")), "8")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b114za", "LT", "6", "a")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b114za", "a")), "6")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b114za", "INCR", "1.5", "a")), "7.5")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b114zm", "1", "a", "3", "c", "2", "b")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b114zm", "MIN", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "a") {
		t.Fatalf("ZMPOP MIN: %s", zmp.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "b114zm")), []string{"b", "2"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b114b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b114b1", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "NOT", "b114bn", "b114b1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b114bn", "0")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ONE", "b114bo", "b114b1", "b114b1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b114bo")), 0)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b114bf", "OVERFLOW", "SAT", "INCRBY", "i8", "0", "200"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "127") {
		t.Fatalf("BITFIELD SAT i8: %s", bf.ToBytes())
	}
	bf2 := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b114bf2", "OVERFLOW", "WRAP", "INCRBY", "i8", "0", "200"))
	if protocol.IsErrorReply(bf2) || !strings.Contains(string(bf2.ToBytes()), "-56") {
		t.Fatalf("BITFIELD WRAP i8: %s", bf2.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b114st", "v")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b114st", "PX", "40000")), "v")
	xx := db.Exec(nil, utils.ToCmdLine("SET", "b114xx", "v", "XX"))
	if _, ok := xx.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET XX miss: %T %s", xx, xx.ToBytes())
	}
	sg := db.Exec(nil, utils.ToCmdLine("SET", "b114st", "w", "GET"))
	if protocol.IsErrorReply(sg) || !strings.Contains(string(sg.ToBytes()), "v") {
		t.Fatalf("SET GET: %s", sg.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETDEL", "b114st")), "w")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PSETEX", "b114sx", "8000", "y")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PEXPIRE", "b114sx", "5000", "XX")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PERSIST", "b114sx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCR", "b114n")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCRBY", "b114n", "4")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECR", "b114n")), 4)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b114h", "a", "10", "b", "20")), 2)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b114h", "90", "NX", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hlt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b114h", "30", "LT", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hlt) || !strings.Contains(string(hlt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE LT: %s", hlt.ToBytes())
	}
	hxx := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b114h", "9000", "XX", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hxx) || !strings.Contains(string(hxx.ToBytes()), "1") {
		t.Fatalf("HPEXPIRE XX: %s", hxx.ToBytes())
	}
	hp := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b114h", "8000", "NX", "FIELDS", "1", "b"))
	if protocol.IsErrorReply(hp) || !strings.Contains(string(hp.ToBytes()), "1") {
		t.Fatalf("HPEXPIRE NX: %s", hp.ToBytes())
	}
	hg := db.Exec(nil, utils.ToCmdLine("HGETEX", "b114h", "PX", "4000", "FIELDS", "1", "b"))
	if protocol.IsErrorReply(hg) || !strings.Contains(string(hg.ToBytes()), "20") {
		t.Fatalf("HGETEX PX: %s", hg.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b114h", "a", "5")), 15)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b114h", "c", "30")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("HMSET", "b114h", "d", "40")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSTRLEN", "b114h", "d")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b114hk", "f", "v")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HKEYS", "b114hk")), []string{"f"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "b114hk")), "f")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b114sa", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b114sb", "b", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SUNIONSTORE", "b114su", "b114sa", "b114sb")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b114su")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SDIFF", "b114sa", "b114sb")), []string{"a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b114si", "b114sa", "b114sb")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SISMEMBER", "b114si", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b114sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b114sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b114x", "1-0", "k", "v")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b114x", "2-0", "k", "w")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b114x", "3-0", "k", "x")), "3-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b114x", "MINID", "2-0")), 1)
	xr := db.Exec(nil, utils.ToCmdLine("XRANGE", "b114x", "-", "+", "COUNT", "1"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "2-0") {
		t.Fatalf("XRANGE: %s", xr.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b114x", "2-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b114x")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b114g", "13.361389", "38.115556", "Palermo")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b114g", "FROMLONLAT", "13", "38", "BYRADIUS", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Palermo") {
		t.Fatalf("GEOSEARCH BYRADIUS: %s", geo.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b114s1", "hello")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b114s2", "hallo")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b114s1", "b114s2")), "hllo")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b114s1", "b114s2", "LEN")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b114sort", "9", "1", "5", "3")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b114sort", "LIMIT", "1", "2")), []string{"3", "5"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b114pf", "a", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b114pf")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b114s1", "b114s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b114s2", "b114s1r")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b114s1r", "80", "NX")), 1)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p114")), "p114")
}
