package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 157 R4-1 extras: RPOP COUNT 3, LMOVE LEFT LEFT, LINSERT AFTER,
// LPOS RANK 3 / MAXLEN / COUNT RANK / COUNT 0, LMPOP RIGHT COUNT 1, BLMOVE RIGHT LEFT,
// RPOPLPUSH, LSET/LREM/LTRIM/RPUSHX, ZRANGE BYSCORE LIMIT WS, ZRANGESTORE BYLEX,
// ZREVRANGEBYLEX, ZPOPMIN, ZUNIONSTORE MIN WEIGHTS, ZINTER SUM WS,
// ZINTERSTORE MAX, ZDIFF WS, ZMPOP MAX, BZPOPMIN, ZADD NX/XX/CH,
// ZRANGESTORE BYSCORE, ZREVRANGEBYSCORE, ZREMRANGEBYSCORE, ZINCRBY,
// BITOP AND/XOR/ANDOR/NOT, BITFIELD u16 SAT, GETEX EXAT/PERSIST, SET XX GET, KEEPTTL,
// HEXPIRE XX/NX/GT, HPEXPIRE GT 短于现 TTL→0,
// HGETEX EXAT/PERSIST, SUNIONSTORE/SINTERSTORE, XDEL/XTRIM MINID,
// GEOSEARCH BYRADIUS, SORT ALPHA, LCS, PFMERGE.
func TestR41Batch157Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b157l", "cedar", "spruce", "fir", "larch", "hemlock", "yew", "sequoia")), 7)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b157l", "3")), []string{"sequoia", "yew", "hemlock"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b157l", "0", "-1")), []string{"cedar", "spruce", "fir", "larch"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b157l", "b157l2", "LEFT", "LEFT")), "cedar")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b157l", "0", "-1")), []string{"spruce", "fir", "larch"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b157l2", "0", "-1")), []string{"cedar"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b157ins", "begin", "finish")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b157ins", "AFTER", "begin", "center")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b157ins", "0", "-1")), []string{"begin", "center", "finish"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b157lp", "r", "s", "r", "s", "r", "s")), 6)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b157lp", "r", "RANK", "3")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b157lp", "s", "MAXLEN", "3")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b157lp", "r", "COUNT", "2", "RANK", "1")), []string{"0", "2"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b157lp", "r", "COUNT", "0")), []string{"0", "2", "4"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b157lmp", "n", "o", "p", "q")), 4)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b157lmp", "RIGHT", "COUNT", "1"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "q") {
		t.Fatalf("LMPOP RIGHT COUNT 1: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b157lmp")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b157bl", "top", "mid", "bot")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b157bl", "b157bld", "RIGHT", "LEFT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "bot") {
		t.Fatalf("BLMOVE RIGHT LEFT: %s", bl.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOPLPUSH", "b157bl", "b157bld")), "mid")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b157bl", "0", "-1")), []string{"top"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b157bld", "0", "-1")), []string{"mid", "bot"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b157lt", "k", "l", "m", "n", "o")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b157lt", "4", "O")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b157lt", "1", "l")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b157lt", "0", "2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b157lt", "0", "-1")), []string{"k", "m", "n"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b157lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b157lx", "t")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b157lx", "u")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b157lx", "s")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b157lx", "0", "-1")), []string{"s", "t", "u"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b157zs", "5", "a", "15", "b", "25", "c", "35", "d", "45", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b157zs", "15", "35", "BYSCORE", "WITHSCORES", "LIMIT", "0", "2")),
		[]string{"b", "15", "c", "25"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b157zlex", "0", "e", "0", "f", "0", "g", "0", "h")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b157zrs", "b157zlex", "[f", "(h", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b157zrs", "0", "-1")), []string{"f", "g"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYLEX", "b157zlex", "(h", "[e", "LIMIT", "0", "2")), []string{"g", "f"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "b157zs")), []string{"a", "5"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b157zu", "1", "b157zs", "WEIGHTS", "2", "AGGREGATE", "MIN")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b157zu", "0", "-1", "WITHSCORES")),
		[]string{"b", "30", "c", "50", "d", "70", "e", "90"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b157zbs", "b157zs", "15", "35", "BYSCORE")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b157zbs", "0", "-1")), []string{"b", "c", "d"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b157zs", "35", "15", "WITHSCORES", "LIMIT", "0", "2")),
		[]string{"d", "35", "c", "25"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYSCORE", "b157zs", "36", "50")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINCRBY", "b157zs", "6", "b")), "21")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b157z1", "15", "a", "24", "b", "8", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b157z2", "20", "b", "6", "c", "35", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b157z1", "b157z2", "WEIGHTS", "1", "1", "AGGREGATE", "SUM", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "c") {
		t.Fatalf("ZINTER SUM: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b157zi", "2", "b157z1", "b157z2", "WEIGHTS", "2", "1", "AGGREGATE", "MAX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b157zi", "0", "-1", "WITHSCORES")),
		[]string{"c", "16", "b", "48"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b157z1", "b157z2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b157zm", "3", "a", "84", "y", "21", "c")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b157zm", "MAX", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "y") {
		t.Fatalf("ZMPOP MAX: %s", zmp.ToBytes())
	}
	bz := db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "b157zm", "0"))
	if protocol.IsErrorReply(bz) || !strings.Contains(string(bz.ToBytes()), "a") {
		t.Fatalf("BZPOPMIN: %s", bz.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b157zm", "NX", "14", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b157zm", "XX", "CH", "29", "c")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b157zm", "CH", "8", "d")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b157zm", "c")), "29")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b157b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b157b2", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b157b2", "5", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "AND", "b157ba", "b157b1", "b157b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b157ba")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b157bx", "b157b1", "b157b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b157bx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ANDOR", "b157bor", "b157b1", "b157b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b157bor")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b157bf", "OVERFLOW", "SAT", "INCRBY", "u16", "0", "80000"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "65535") {
		t.Fatalf("BITFIELD u16 SAT: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b157st", "v", "PX", "91000")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b157st", "EXAT", "2000000009")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b157st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b157st")), -1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b157h", "f1", "tt", "f2", "52", "f3", "uu")), 3)
	hxx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b157h", "55", "XX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hxx) || !strings.Contains(string(hxx.ToBytes()), "0") {
		t.Fatalf("HEXPIRE XX: %s", hxx.ToBytes())
	}
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b157h", "75", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b157h", "16", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "0") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	hpgt := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b157h", "5500", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hpgt) || !strings.Contains(string(hpgt.ToBytes()), "0") {
		t.Fatalf("HPEXPIRE GT: %s", hpgt.ToBytes())
	}
	he := db.Exec(nil, utils.ToCmdLine("HGETEX", "b157h", "EXAT", "2000000003", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(he) || !strings.Contains(string(he.ToBytes()), "uu") {
		t.Fatalf("HGETEX EXAT: %s", he.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b157h", "f2", "7")), 59)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b157h")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b157sa", "za", "zb", "zc")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b157sb", "zc", "zd")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SUNIONSTORE", "b157su", "b157sa", "b157sb")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b157ss", "b157sa", "b157sb")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUNION", "b157ss")), []string{"zc"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b157sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b157sp")), "only")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b157sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b157x", "27-0", "k", "v")), "27-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b157x", "37-0", "k", "w")), "37-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b157x", "47-0", "k", "x")), "47-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b157x", "27-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b157x", "MINID", "42-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b157x")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b157g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b157g", "FROMLONLAT", "15", "37.5", "BYRADIUS", "80", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH BYRADIUS: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b157s1", "starlight")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b157s2", "star")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b157s1", "b157s2")), "star")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b157s1", "b157s2", "LEN")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b157sort", "mango", "melon", "mint", "mulberry")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b157sort", "ALPHA", "LIMIT", "1", "2")), []string{"melon", "mint"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b157p1", "c")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b157p2", "d")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b157pm", "b157p1", "b157p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b157pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b157s1", "b157s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b157s2", "b157s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p157")), "p157")
}
