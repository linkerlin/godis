package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 147 R4-1 extras: RPOP COUNT 2, LMOVE LEFT RIGHT, LINSERT AFTER,
// LPOS RANK 2 / MAXLEN / COUNT RANK / COUNT 0, LMPOP RIGHT COUNT 2, BLMOVE RIGHT RIGHT,
// RPOPLPUSH, LSET/LREM/LTRIM/RPUSHX, ZRANGE BYSCORE LIMIT WS, ZRANGESTORE BYLEX,
// ZREVRANGEBYLEX, ZPOPMIN, ZUNIONSTORE SUM WEIGHTS, ZINTER MAX WS,
// ZINTERSTORE MIN, ZDIFF WS, ZMPOP MAX, BZPOPMIN, ZADD NX/XX/CH,
// ZRANGESTORE BYSCORE, ZREVRANGEBYSCORE, ZREMRANGEBYSCORE, ZINCRBY,
// BITOP AND/XOR/ANDOR/NOT, BITFIELD u16 SAT, GETEX EXAT/PERSIST, SET XX GET, KEEPTTL,
// HEXPIRE XX/NX/GT, HPEXPIRE GT 短于现 TTL→0,
// HGETEX EXAT/PERSIST, SUNIONSTORE/SINTERSTORE, XDEL/XTRIM MINID,
// GEOSEARCH BYRADIUS, SORT ALPHA, LCS, PFMERGE.
func TestR41Batch147Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b147l", "ruby", "jade", "opal", "onyx", "pearl", "amber", "coral")), 7)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b147l", "2")), []string{"coral", "amber"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b147l", "0", "-1")), []string{"ruby", "jade", "opal", "onyx", "pearl"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b147l", "b147l2", "LEFT", "RIGHT")), "ruby")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b147l", "0", "-1")), []string{"jade", "opal", "onyx", "pearl"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b147l2", "0", "-1")), []string{"ruby"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b147ins", "begin", "finish")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b147ins", "AFTER", "begin", "mid")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b147ins", "0", "-1")), []string{"begin", "mid", "finish"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b147lp", "q", "r", "q", "r", "q", "r")), 6)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b147lp", "q", "RANK", "2")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b147lp", "r", "MAXLEN", "2")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b147lp", "q", "COUNT", "2", "RANK", "1")), []string{"0", "2"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b147lp", "q", "COUNT", "0")), []string{"0", "2", "4"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b147lmp", "f", "g", "h", "i")), 4)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b147lmp", "RIGHT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "i") {
		t.Fatalf("LMPOP RIGHT COUNT 2: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b147lmp")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b147bl", "in", "mid", "out")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b147bl", "b147bld", "RIGHT", "RIGHT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "out") {
		t.Fatalf("BLMOVE RIGHT RIGHT: %s", bl.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOPLPUSH", "b147bl", "b147bld")), "mid")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b147bl", "0", "-1")), []string{"in"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b147bld", "0", "-1")), []string{"mid", "out"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b147lt", "f", "g", "h", "i", "j")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b147lt", "0", "F")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b147lt", "1", "g")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b147lt", "0", "2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b147lt", "0", "-1")), []string{"F", "h", "i"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b147lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b147lx", "p")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b147lx", "q")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b147lx", "o")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b147lx", "0", "-1")), []string{"o", "p", "q"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b147zs", "6", "a", "11", "b", "16", "c", "21", "d", "26", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b147zs", "11", "21", "BYSCORE", "WITHSCORES", "LIMIT", "0", "2")),
		[]string{"b", "11", "c", "16"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b147zlex", "0", "e", "0", "f", "0", "g", "0", "h")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b147zrs", "b147zlex", "[f", "(h", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b147zrs", "0", "-1")), []string{"f", "g"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYLEX", "b147zlex", "(h", "[e", "LIMIT", "0", "2")), []string{"g", "f"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "b147zs")), []string{"a", "6"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b147zu", "1", "b147zs", "WEIGHTS", "2", "AGGREGATE", "SUM")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b147zu", "0", "-1", "WITHSCORES")),
		[]string{"b", "22", "c", "32", "d", "42", "e", "52"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b147zbs", "b147zs", "11", "21", "BYSCORE")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b147zbs", "0", "-1")), []string{"b", "c", "d"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b147zs", "21", "11", "WITHSCORES", "LIMIT", "0", "2")),
		[]string{"d", "21", "c", "16"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYSCORE", "b147zs", "22", "40")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINCRBY", "b147zs", "4", "b")), "15")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b147z1", "20", "a", "30", "b", "12", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b147z2", "18", "b", "7", "c", "25", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b147z1", "b147z2", "WEIGHTS", "1", "1", "AGGREGATE", "MAX", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER MAX: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b147zi", "2", "b147z1", "b147z2", "WEIGHTS", "1", "2", "AGGREGATE", "MIN")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b147zi", "0", "-1", "WITHSCORES")),
		[]string{"c", "12", "b", "30"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b147z1", "b147z2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b147zm", "5", "a", "41", "z", "19", "c")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b147zm", "MAX", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "z") {
		t.Fatalf("ZMPOP MAX: %s", zmp.ToBytes())
	}
	bz := db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "b147zm", "0"))
	if protocol.IsErrorReply(bz) || !strings.Contains(string(bz.ToBytes()), "a") {
		t.Fatalf("BZPOPMIN: %s", bz.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b147zm", "NX", "15", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b147zm", "XX", "CH", "28", "c")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b147zm", "CH", "8", "d")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b147zm", "c")), "28")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b147b1", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b147b2", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b147b2", "6", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "AND", "b147ba", "b147b1", "b147b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b147ba")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b147bx", "b147b1", "b147b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b147bx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ANDOR", "b147bo", "b147b1", "b147b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b147bo")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b147bf", "OVERFLOW", "SAT", "INCRBY", "u16", "0", "70000"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "65535") {
		t.Fatalf("BITFIELD u16 SAT: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b147st", "v", "PX", "60000")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b147st", "EXAT", "2000000002")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b147st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b147st")), -1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b147h", "f1", "s1", "f2", "31", "f3", "s3")), 3)
	hxx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b147h", "45", "XX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hxx) || !strings.Contains(string(hxx.ToBytes()), "0") {
		t.Fatalf("HEXPIRE XX: %s", hxx.ToBytes())
	}
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b147h", "80", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b147h", "22", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "0") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	hpgt := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b147h", "8000", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hpgt) || !strings.Contains(string(hpgt.ToBytes()), "0") {
		t.Fatalf("HPEXPIRE GT: %s", hpgt.ToBytes())
	}
	he := db.Exec(nil, utils.ToCmdLine("HGETEX", "b147h", "EXAT", "2000000000", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(he) || !strings.Contains(string(he.ToBytes()), "s3") {
		t.Fatalf("HGETEX EXAT: %s", he.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b147h", "f2", "9")), 40)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b147h")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b147sa", "pa", "pb", "pc")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b147sb", "pc", "pd")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SUNIONSTORE", "b147su", "b147sa", "b147sb")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b147ss", "b147sa", "b147sb")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUNION", "b147ss")), []string{"pc"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b147sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b147sp")), "only")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b147sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b147x", "17-0", "k", "v")), "17-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b147x", "27-0", "k", "w")), "27-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b147x", "37-0", "k", "x")), "37-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b147x", "17-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b147x", "MINID", "32-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b147x")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b147g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b147g", "FROMLONLAT", "15", "37.5", "BYRADIUS", "80", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH BYRADIUS: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b147s1", "waterfall")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b147s2", "water")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b147s1", "b147s2")), "water")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b147s1", "b147s2", "LEN")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b147sort", "zebra", "yak", "fox", "elk")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b147sort", "ALPHA", "LIMIT", "0", "2")), []string{"elk", "fox"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b147p1", "i")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b147p2", "j")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b147pm", "b147p1", "b147p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b147pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b147s1", "b147s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b147s2", "b147s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p147")), "p147")
}
