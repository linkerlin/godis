package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 117 R4-1 extras: RPOP COUNT 1, LMOVE LEFT RIGHT, LINSERT AFTER,
// LPOS MAXLEN / RANK -1, LMPOP RIGHT COUNT 2, BLMOVE RIGHT LEFT, RPOPLPUSH,
// LTRIM, ZRANGE BYLEX LIMIT, ZREMRANGEBYLEX, ZREVRANGEBYSCORE WS LIMIT,
// ZRANGESTORE BYLEX, ZUNION MAX WS, ZINTERSTORE MAX, ZMPOP MAX, BZPOPMIN,
// ZADD GT/LT/INCR, BITOP XOR/NOT, BITFIELD u8/i8 SAT, GETEX PX, SET GET,
// HEXPIRE LT, HPEXPIRE XX, HGETEX PX, SUNIONSTORE, XTRIM MINID,
// GEORADIUS/GEOSEARCH FROMMEMBER, SORT ALPHA DESC LIMIT, LCS.
func TestR41Batch117Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b117l", "a", "b", "c", "d", "e", "f")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b117l", "1")), []string{"f"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b117l", "0", "-1")), []string{"a", "b", "c", "d", "e"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b117l", "b117l2", "LEFT", "RIGHT")), "a")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b117l", "0", "-1")), []string{"b", "c", "d", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b117l2", "0", "-1")), []string{"a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b117ins", "a", "c", "e")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b117ins", "AFTER", "c", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b117ins", "0", "-1")), []string{"a", "c", "d", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b117lp", "x", "y", "z", "y", "x")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b117lp", "y", "MAXLEN", "3")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b117lp", "x", "RANK", "-1")), 4)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b117lmp", "1", "2", "3", "4", "5")), 5)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b117lmp", "RIGHT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "5") {
		t.Fatalf("LMPOP RIGHT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b117lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b117bl", "a", "b", "c")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b117bl", "b117bld", "RIGHT", "LEFT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "c") {
		t.Fatalf("BLMOVE RIGHT LEFT: %s", bl.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOPLPUSH", "b117bl", "b117bld")), "b")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b117bld", "0", "-1")), []string{"b", "c"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b117bl", "0", "-1")), []string{"a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b117lt", "a", "b", "c", "d", "e")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b117lt", "1", "3")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b117lt", "0", "-1")), []string{"b", "c", "d"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b117lt", "z")), 4)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b117z", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b117z", "[b", "(e", "BYLEX", "LIMIT", "1", "2")), []string{"c", "d"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b117z", "[c", "(e")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b117z", "0", "-1")), []string{"a", "b", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b117zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b117zs", "4", "2", "WITHSCORES", "LIMIT", "0", "2")),
		[]string{"d", "4", "c", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b117zrs", "b117zs", "(a", "[c", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b117zrs", "0", "-1")), []string{"b", "c"})
	zm := db.Exec(nil, utils.ToCmdLine("ZMSCORE", "b117zs", "a", "missing", "e"))
	if protocol.IsErrorReply(zm) || !strings.Contains(string(zm.ToBytes()), "5") {
		t.Fatalf("ZMSCORE: %s", zm.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANK", "b117zs", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANK", "b117zs", "c")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b117z1", "3", "a", "5", "b", "7", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b117z2", "4", "a", "1", "b", "9", "d")), 3)
	zu := db.Exec(nil, utils.ToCmdLine("ZUNION", "2", "b117z1", "b117z2", "WEIGHTS", "1", "1", "AGGREGATE", "MAX", "WITHSCORES"))
	if protocol.IsErrorReply(zu) || !strings.Contains(string(zu.ToBytes()), "d") {
		t.Fatalf("ZUNION MAX: %s", zu.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b117zi", "2", "b117z1", "b117z2", "WEIGHTS", "1", "1", "AGGREGATE", "MAX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b117zi", "0", "-1", "WITHSCORES")),
		[]string{"a", "4", "b", "5"})
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b117z1", "b117z2", "WEIGHTS", "1", "1", "AGGREGATE", "MIN", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "b") {
		t.Fatalf("ZINTER MIN: %s", zi.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b117zm", "1", "a", "4", "d", "2", "b")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b117zm", "MAX", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "d") {
		t.Fatalf("ZMPOP MAX: %s", zmp.ToBytes())
	}
	bz := db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "b117zm", "0"))
	if protocol.IsErrorReply(bz) || !strings.Contains(string(bz.ToBytes()), "a") {
		t.Fatalf("BZPOPMIN: %s", bz.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b117za", "5", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b117za", "GT", "8", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b117za", "LT", "1", "a")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b117za", "INCR", "0.5", "a")), "1.5")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b117b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b117b1", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b117b2", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b117b2", "2", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b117bx", "b117b1", "b117b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b117bx")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "NOT", "b117bn", "b117b1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b117bn", "0")), 0)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b117bf", "OVERFLOW", "SAT", "INCRBY", "u8", "0", "300"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "255") {
		t.Fatalf("BITFIELD u8 SAT: %s", bf.ToBytes())
	}
	bf2 := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b117bf2", "OVERFLOW", "SAT", "INCRBY", "i8", "0", "200"))
	if protocol.IsErrorReply(bf2) || !strings.Contains(string(bf2.ToBytes()), "127") {
		t.Fatalf("BITFIELD i8 SAT: %s", bf2.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b117st", "v")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b117st", "PX", "40000")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b117st", "w", "GET")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETDEL", "b117st")), "w")
	xx := db.Exec(nil, utils.ToCmdLine("SET", "b117xx", "v", "XX"))
	if _, ok := xx.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET XX miss: %T %s", xx, xx.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PSETEX", "b117sx", "8000", "y")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PEXPIRE", "b117sx", "5000", "XX")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PERSIST", "b117sx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCR", "b117n")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCRBY", "b117n", "4")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECR", "b117n")), 4)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b117h", "a", "10", "b", "20")), 2)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b117h", "90", "NX", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hlt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b117h", "30", "LT", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hlt) || !strings.Contains(string(hlt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE LT: %s", hlt.ToBytes())
	}
	hxx := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b117h", "9000", "XX", "FIELDS", "1", "a"))
	if protocol.IsErrorReply(hxx) || !strings.Contains(string(hxx.ToBytes()), "1") {
		t.Fatalf("HPEXPIRE XX: %s", hxx.ToBytes())
	}
	hp := db.Exec(nil, utils.ToCmdLine("HGETEX", "b117h", "PX", "4000", "FIELDS", "1", "b"))
	if protocol.IsErrorReply(hp) || !strings.Contains(string(hp.ToBytes()), "20") {
		t.Fatalf("HGETEX PX: %s", hp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b117h", "a", "5")), 15)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b117h", "c", "30")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("HMSET", "b117h", "d", "40")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSTRLEN", "b117h", "d")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b117hk", "f", "v")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HKEYS", "b117hk")), []string{"f"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "b117hk")), "f")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b117sa", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b117sb", "b", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SUNIONSTORE", "b117su", "b117sa", "b117sb")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b117su")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SDIFF", "b117sa", "b117sb")), []string{"a"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b117si", "b117sa", "b117sb")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SISMEMBER", "b117si", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b117sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b117sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b117x", "1-0", "k", "v")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b117x", "2-0", "k", "w")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b117x", "3-0", "k", "x")), "3-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b117x", "MINID", "2-0")), 1)
	x1 := db.Exec(nil, utils.ToCmdLine("XRANGE", "b117x", "-", "+", "COUNT", "1"))
	if protocol.IsErrorReply(x1) || !strings.Contains(string(x1.ToBytes()), "2-0") {
		t.Fatalf("XRANGE: %s", x1.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b117x", "2-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b117x")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b117g", "15.087269", "37.502669", "Catania")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEORADIUS", "b117g", "15", "37", "80", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Catania") {
		t.Fatalf("GEORADIUS: %s", geo.ToBytes())
	}
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b117g", "FROMMEMBER", "Catania", "BYRADIUS", "1", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH FROMMEMBER: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b117s1", "abcde")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b117s2", "abxyz")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b117s1", "b117s2")), "ab")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b117s1", "b117s2", "LEN")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b117sort", "b", "a", "c", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b117sort", "ALPHA", "DESC", "LIMIT", "0", "2")), []string{"d", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b117pf", "a", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b117pf")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b117s1", "b117s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b117s2", "b117s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p117")), "p117")
}
