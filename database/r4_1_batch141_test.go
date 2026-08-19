package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 141 R4-1 extras: RPOP COUNT 2, LMOVE LEFT RIGHT, LINSERT AFTER,
// LPOS RANK 3 / MAXLEN / COUNT RANK / COUNT 0, LMPOP RIGHT COUNT 2, BLMOVE RIGHT LEFT,
// RPOPLPUSH, LSET/LREM/LTRIM/RPUSHX, ZRANGE BYSCORE LIMIT WS, ZRANGESTORE BYLEX,
// ZREVRANGEBYLEX, ZPOPMIN, ZUNIONSTORE SUM WEIGHTS, ZINTER MIN WS,
// ZINTERSTORE SUM, ZDIFF WS, ZMPOP MAX, BZPOPMIN, ZADD NX/XX/CH,
// ZRANGESTORE BYSCORE, ZREVRANGEBYSCORE, ZREMRANGEBYSCORE, ZINCRBY,
// BITOP AND/XOR/ONE/NOT, BITFIELD i8 WRAP, GETEX PX/PERSIST, SET XX GET, KEEPTTL,
// HEXPIRE XX/NX/GT, HPEXPIRE GT 短于现 TTL→0,
// HGETEX PX/PERSIST, SUNIONSTORE/SINTERSTORE, XDEL/XTRIM MINID,
// GEOSEARCH BYRADIUS, SORT ALPHA, LCS, PFMERGE.
func TestR41Batch141Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b141l", "oak", "elm", "ash", "fir", "pine", "yew", "maple")), 7)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b141l", "2")), []string{"maple", "yew"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b141l", "0", "-1")), []string{"oak", "elm", "ash", "fir", "pine"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LMOVE", "b141l", "b141l2", "LEFT", "RIGHT")), "oak")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b141l", "0", "-1")), []string{"elm", "ash", "fir", "pine"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b141l2", "0", "-1")), []string{"oak"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b141ins", "ink", "paper")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b141ins", "AFTER", "ink", "dye")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b141ins", "0", "-1")), []string{"ink", "dye", "paper"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b141lp", "a", "b", "a", "b", "a", "b")), 6)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b141lp", "a", "RANK", "3")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b141lp", "a", "MAXLEN", "2")), 0)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b141lp", "a", "COUNT", "2", "RANK", "2")), []string{"2", "4"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b141lp", "a", "COUNT", "0")), []string{"0", "2", "4"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b141lmp", "w", "x", "y", "z")), 4)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b141lmp", "RIGHT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "z") {
		t.Fatalf("LMPOP RIGHT COUNT 2: %s", lmp.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b141lmp")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b141bl", "top", "mid", "bot")), 3)
	bl := db.Exec(nil, utils.ToCmdLine("BLMOVE", "b141bl", "b141bld", "RIGHT", "LEFT", "0"))
	if protocol.IsErrorReply(bl) || !strings.Contains(string(bl.ToBytes()), "bot") {
		t.Fatalf("BLMOVE RIGHT LEFT: %s", bl.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOPLPUSH", "b141bl", "b141bld")), "mid")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b141bl", "0", "-1")), []string{"top"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b141bld", "0", "-1")), []string{"mid", "bot"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b141lt", "f", "g", "h", "i", "j")), 5)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b141lt", "0", "F")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b141lt", "1", "i")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LTRIM", "b141lt", "0", "2")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b141lt", "0", "-1")), []string{"F", "g", "h"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b141lx", "v")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b141lx", "q")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b141lx", "z")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b141lx", "w")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b141lx", "0", "-1")), []string{"w", "q", "z"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b141zs", "6", "a", "11", "b", "16", "c", "21", "d", "26", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b141zs", "11", "21", "BYSCORE", "WITHSCORES", "LIMIT", "1", "2")),
		[]string{"c", "16", "d", "21"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b141zlex", "0", "i", "0", "j", "0", "k", "0", "l")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b141zrs", "b141zlex", "[j", "(l", "BYLEX")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b141zrs", "0", "-1")), []string{"j", "k"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYLEX", "b141zlex", "(l", "[i", "LIMIT", "0", "2")), []string{"k", "j"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "b141zs")), []string{"a", "6"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b141zu", "1", "b141zs", "WEIGHTS", "2", "AGGREGATE", "SUM")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b141zu", "0", "-1", "WITHSCORES")),
		[]string{"b", "22", "c", "32", "d", "42", "e", "52"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b141zbs", "b141zs", "11", "21", "BYSCORE")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b141zbs", "0", "-1")), []string{"b", "c", "d"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b141zs", "21", "11", "WITHSCORES", "LIMIT", "0", "2")),
		[]string{"d", "21", "c", "16"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYSCORE", "b141zs", "22", "40")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINCRBY", "b141zs", "3", "b")), "14")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b141z1", "11", "a", "17", "b", "9", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b141z2", "8", "b", "13", "c", "10", "d")), 3)
	zi := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b141z1", "b141z2", "WEIGHTS", "1", "1", "AGGREGATE", "MIN", "WITHSCORES"))
	if protocol.IsErrorReply(zi) || !strings.Contains(string(zi.ToBytes()), "b") {
		t.Fatalf("ZINTER MIN: %s", zi.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b141zi", "2", "b141z1", "b141z2", "WEIGHTS", "2", "1", "AGGREGATE", "SUM")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b141zi", "0", "-1", "WITHSCORES")),
		[]string{"c", "31", "b", "42"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b141z1", "b141z2")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b141zm", "8", "a", "28", "h", "13", "c")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b141zm", "MAX", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "h") {
		t.Fatalf("ZMPOP MAX: %s", zmp.ToBytes())
	}
	bz := db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "b141zm", "0"))
	if protocol.IsErrorReply(bz) || !strings.Contains(string(bz.ToBytes()), "a") {
		t.Fatalf("BZPOPMIN: %s", bz.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b141zm", "NX", "15", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b141zm", "XX", "CH", "20", "c")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b141zm", "CH", "9", "d")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b141zm", "c")), "20")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b141b1", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b141b2", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b141b2", "5", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "AND", "b141ba", "b141b1", "b141b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b141ba")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b141bx", "b141b1", "b141b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b141bx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ONE", "b141bo", "b141b1", "b141b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b141bo")), 1)
	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b141bf", "OVERFLOW", "WRAP", "INCRBY", "i8", "0", "200"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "-56") {
		t.Fatalf("BITFIELD i8 WRAP: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b141st", "v", "PX", "80000")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b141st", "PX", "90000")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b141st", "PERSIST")), "v")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b141st")), -1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b141h", "f1", "p1", "f2", "22", "f3", "p3")), 3)
	hxx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b141h", "50", "XX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hxx) || !strings.Contains(string(hxx.ToBytes()), "0") {
		t.Fatalf("HEXPIRE XX: %s", hxx.ToBytes())
	}
	hnx := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b141h", "90", "NX", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hnx) || !strings.Contains(string(hnx.ToBytes()), "1") {
		t.Fatalf("HEXPIRE NX: %s", hnx.ToBytes())
	}
	hgt := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b141h", "20", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hgt) || !strings.Contains(string(hgt.ToBytes()), "0") {
		t.Fatalf("HEXPIRE GT: %s", hgt.ToBytes())
	}
	hpgt := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "b141h", "8000", "GT", "FIELDS", "1", "f1"))
	if protocol.IsErrorReply(hpgt) || !strings.Contains(string(hpgt.ToBytes()), "0") {
		t.Fatalf("HPEXPIRE GT: %s", hpgt.ToBytes())
	}
	he := db.Exec(nil, utils.ToCmdLine("HGETEX", "b141h", "PX", "40000", "FIELDS", "1", "f3"))
	if protocol.IsErrorReply(he) || !strings.Contains(string(he.ToBytes()), "p3") {
		t.Fatalf("HGETEX PX: %s", he.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b141h", "f2", "9")), 31)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b141h")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b141sa", "p", "q", "r")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b141sb", "r", "s")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SUNIONSTORE", "b141su", "b141sa", "b141sb")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b141ss", "b141sa", "b141sb")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUNION", "b141ss")), []string{"r"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b141sp", "only")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "b141sp")), "only")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "b141sp")), "only")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b141x", "11-0", "k", "v")), "11-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b141x", "21-0", "k", "w")), "21-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b141x", "31-0", "k", "x")), "31-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b141x", "11-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b141x", "MINID", "26-0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b141x")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b141g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	gs := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b141g", "FROMLONLAT", "15", "37.5", "BYRADIUS", "80", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(gs) || !strings.Contains(string(gs.ToBytes()), "Catania") {
		t.Fatalf("GEOSEARCH BYRADIUS: %s", gs.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b141s1", "shipping")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b141s2", "ship")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b141s1", "b141s2")), "ship")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b141s1", "b141s2", "LEN")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b141sort", "dog", "cat", "ant", "bee")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b141sort", "ALPHA", "LIMIT", "0", "2")), []string{"ant", "bee"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b141p1", "m")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b141p2", "n")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "b141pm", "b141p1", "b141p2")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b141pm")), 2)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b141s1", "b141s1r")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b141s2", "b141s1r")), 0)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "p141")), "p141")
}
