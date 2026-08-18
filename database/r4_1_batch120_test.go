package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 120 R4-1 extras: LPOP COUNT 3, LMOVE RIGHT LEFT, LINSERT BEFORE,
// LPOS COUNT 0 / RANK -1, LMPOP LEFT COUNT 2, BLMOVE LEFT RIGHT, LSET/LREM,
// LPUSHX, ZREVRANGEBYSCORE WS LIMIT, ZRANGESTORE BYSCORE, ZPOPMAX,
// ZUNION MAX WS, ZRANGE BYLEX LIMIT, ZREMRANGEBYLEX, ZINTER MAX WS,
// ZINTERSTORE MAX, ZMPOP MIN, ZADD GT/LT/INCR, BITOP OR/NOT/DIFF,
// BITFIELD i8 SAT, GETEX PX, SET GET miss, HEXPIRE LT, HPEXPIRE XX,
// HGETEX PX, HGETDEL, SUNIONSTORE, XTRIM MAXLEN, GEOSEARCH BYRADIUS,
// GEORADIUS, SORT ALPHA, LCS, PFMERGE.
func TestR41Batch120Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b120l", "a", "b", "c", "d", "e", "f")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b120l", "3")), []string{"a", "b", "c"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b120l", "0", "-1")), []string{"d", "e", "f"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b120l", "b120l2", "RIGHT", "LEFT")), "f")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b120l", "0", "-1")), []string{"d", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b120l2", "0", "-1")), []string{"f"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b120ins", "a", "c", "e")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b120ins", "BEFORE", "c", "b")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b120ins", "0", "-1")), []string{"a", "b", "c", "e"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b120lp", "x", "y", "z", "y", "x")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b120lp", "y", "COUNT", "0")), []string{"1", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b120lp", "x", "RANK", "-1")), 4)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b120lmp", "1", "2", "3", "4", "5")), 5)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b120lmp", "LEFT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "1") {
		t.Fatalf("LMPOP LEFT: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b120lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b120bl", "a", "b", "c")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b120bl", "b120bld", "LEFT", "RIGHT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "a") {
		t.Fatalf("BLMOVE LEFT RIGHT: %s", bl.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b120bl", "0", "-1")), []string{"b", "c"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b120bld", "0", "-1")), []string{"a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b120lt", "a", "b", "c", "d", "e")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b120lt", "0", "X")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b120lt", "0", "c")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b120lt", "0", "-1")), []string{"X", "b", "d", "e"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b120lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b120lx", "q")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b120lx", "z")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b120zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b120zs", "4", "(1", "WITHSCORES", "LIMIT", "0", "2")),
		[]string{"d", "4", "c", "3"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b120zrs", "b120zs", "2", "4", "BYSCORE")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b120zrs", "0", "-1")), []string{"b", "c", "d"})
	zm := db.Exec(nil, utils.ToCmdLine("ZMSCORE", "b120zs", "a", "missing", "d"))
	if protocol.IsErrorReply(zm) || !strings.Contains(string(zm.ToBytes()), "1") {
		t.Fatalf("ZMSCORE: %s", zm.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "b120zs")), []string{"e", "5"})
	zu := db.Exec(nil, utils.ToCmdLine("ZUNION", "1", "b120zs", "WEIGHTS", "2", "AGGREGATE", "MAX", "WITHSCORES"))
	if protocol.IsErrorReply(zu) || !strings.Contains(string(zu.ToBytes()), "a") {
		t.Fatalf("ZUNION MAX: %s", zu.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYRANK", "b120zs", "0", "0")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b120zlex", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b120zlex", "[b", "(e", "BYLEX", "LIMIT", "0", "2")), []string{"b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "b120zlex", "[a", "(c")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b120zlex", "0", "-1")), []string{"c", "d", "e"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b120zlex", "[c", "+")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b120z1", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b120z2", "5", "b", "1", "c", "9", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b120z1", "b120z2", "WEIGHTS", "1", "1", "AGGREGATE", "MAX", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER MAX: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b120zi", "2", "b120z1", "b120z2", "WEIGHTS", "1", "2", "AGGREGATE", "MAX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b120zi", "0", "-1", "WITHSCORES")),
		[]string{"c", "3", "b", "10"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b120z1", "b120z2", "LIMIT", "1")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b120zm", "1", "a", "4", "d", "2", "b")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b120zm", "MIN", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "a") {
		t.Fatalf("ZMPOP MIN: %s", zmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b120zm", "GT", "9", "d")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b120zm", "LT", "0", "b")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b120zm", "INCR", "0.5", "b")), "0.5")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b120zm", "d")), "9")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b120b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b120b2", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b120b2", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b120bo", "b120b1", "b120b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b120bo")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "NOT", "b120bn", "b120b1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b120bn", "0")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "DIFF", "b120bd", "b120b2", "b120b1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b120bd")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b120b2", "1")), 0)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b120bf", "OVERFLOW", "SAT", "INCRBY", "i8", "0", "200"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "127") {
		t.Fatalf("BITFIELD i8 SAT: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b120st", "v")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b120st", "PX", "40000")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b120st", "w")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("APPEND", "b120st", "!")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b120st")), "w!")
	sg := db.Exec(nil, utils.ToCmdLine("SET", "b120sg", "v", "GET"))
	if _, ok := sg.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET GET miss: %T %s", sg, sg.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b120sg")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECRBY", "b120dn", "3")), -3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b120m1", "1", "b120m2", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PEXPIREAT", "b120st", "2000000000000")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PEXPIRETIME", "b120st")), 2000000000000)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PSETEX", "b120kt", "30000", "hello")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PERSIST", "b120kt")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b120h", "f1", "v1", "f2", "20", "f3", "v3")), 3)
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b120h", "80", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hlt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b120h", "30", "LT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hlt) || !strings.Contains(string(hlt.ToBytes()), "1") {
		t.Fatalf("HEXPIRE LT: %s", hlt.ToBytes())
	}
	hxx := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b120h", "7000", "XX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hxx) || !strings.Contains(string(hxx.ToBytes()), "1") {
		t.Fatalf("HPEXPIRE XX: %s", hxx.ToBytes())
	}
	hp := db.Exec(nil, utils.ToCmdLine("HGETEX", "b120h", "PX", "5000", "FIELDS", "1", "f2"))
	if protocol.IsErrorReply(hp) || !strings.Contains(string(hp.ToBytes()), "20") {
		t.Fatalf("HGETEX PX: %s", hp.ToBytes())
	}
	hd := db.Exec(nil, utils.ToCmdLine("HGETDEL", "b120h", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(hd) || !strings.Contains(string(hd.ToBytes()), "v3") {
		t.Fatalf("HGETDEL: %s", hd.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBYFLOAT", "b120h", "f2", "0.5")), "20.5")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b120h", "f4", "v4")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b120hk", "f", "v")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HVALS", "b120hk")), []string{"v"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HRANDFIELD", "b120hk")), "f")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b120sa", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b120sb", "b", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SUNIONSTORE", "b120su", "b120sa", "b120sb")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b120su")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SMOVE", "b120sa", "b120sb", "a")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SINTER", "b120sa", "b120sb")), []string{"b"})
	sm := db.Exec(nil, utils.ToCmdLine("SMISMEMBER", "b120sb", "a", "z", "b"))
	if protocol.IsErrorReply(sm) || !strings.Contains(string(sm.ToBytes()), "0") {
		t.Fatalf("SMISMEMBER: %s", sm.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b120sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b120sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b120x", "1-0", "k", "v")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b120x", "2-0", "k", "w")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b120x", "5-0", "k", "x")), "5-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b120x", "MAXLEN", "2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b120x")), 2)
	xr := db.Exec(nil, utils.ToCmdLine("XREVRANGE", "b120x", "+", "-", "COUNT", "1"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "5-0") {
		t.Fatalf("XREVRANGE: %s", xr.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b120g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b120g", "FROMLONLAT", "13", "38", "BYRADIUS", "100", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Palermo") {
		t.Fatalf("GEOSEARCH BYRADIUS: %s", gs.ToBytes())
	}
	gr := db.Exec(nil, utils.ToCmdLine("GEORADIUS", "b120g", "15.08", "37.5", "50", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gr) || !strings.Contains(string(gr.ToBytes()), "Catania") {
		t.Fatalf("GEORADIUS: %s", gr.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b120s1", "kitten")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b120s2", "sitting")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b120s1", "b120s2")), "ittn")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b120s1", "b120s2", "LEN")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b120sort", "c", "a", "b")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b120sort", "ALPHA")), []string{"a", "b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b120p1", "x")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b120p2", "y")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b120pm", "b120p1", "b120p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b120pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b120s1", "b120s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b120s2", "b120s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p120")), "p120")
}
