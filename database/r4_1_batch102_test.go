package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 102 R4-1 extras: LPOP/RPOP COUNT, ZRANGE REV/WITHSCORES, ZREVRANGEBYSCORE,
// ZMSCORE, HMGET, SET NX GET, GETEX EX, BITFIELD u16 SAT, XRANGE, ZUNION WEIGHTS,
// SDIFF/SINTERCARD, LMPOP RIGHT, ZMPOP MAX, LPOS COUNT, EXPIRE GT miss, GETDEL,
// GEORADIUS, SORT LIMIT.
func TestR41Batch102Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b102l", "a", "b", "c", "d", "e", "f")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b102l", "3")), []string{"a", "b", "c"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b102l", "2")), []string{"f", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b102l", "0", "-1")), []string{"d"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b102z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b102z", "0", "2", "WITHSCORES")),
		[]string{"a", "1", "b", "2", "c", "3"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b102z", "5", "1", "LIMIT", "1", "2")),
		[]string{"d", "c"})
	zm := db.Exec(nil, utils.ToCmdLine("ZMSCORE", "b102z", "a", "c", "missing"))
	if protocol.IsErrorReply(zm) || !strings.Contains(string(zm.ToBytes()), "1") {
		t.Fatalf("ZMSCORE: %s", zm.ToBytes())
	}
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b102z", "0", "-1", "REV")),
		[]string{"e", "d", "c", "b", "a"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b102h", "a", "10", "b", "20", "c", "30")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HDEL", "b102h", "b")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HMGET", "b102h", "a", "c", "miss")), []string{"10", "30", ""})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b102h", "a", "5")), 15)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b102st", "foobar")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b102st", "-3", "-1")), "bar")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b102st", "0", "FOO")), 6)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUBSTR", "b102st", "0", "2")), "FOO")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("APPEND", "b102st", "!")), 7)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETDEL", "b102st")), "FOObar!")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b102nx", "v", "NX")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b102nx", "w", "NX", "GET")), "v")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b102xx", "v")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b102xx", "w", "XX", "GET")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b102xx", "EX", "30")), "w")

	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b102bf", "SET", "u16", "0", "1000"))
	if protocol.IsErrorReply(bf) {
		t.Fatalf("BITFIELD SET: %s", bf.ToBytes())
	}
	bg := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b102bf", "GET", "u16", "0"))
	if protocol.IsErrorReply(bg) || !strings.Contains(string(bg.ToBytes()), "1000") {
		t.Fatalf("BITFIELD GET: %s", bg.ToBytes())
	}
	bs := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b102bf", "OVERFLOW", "SAT", "INCRBY", "u16", "0", "70000"))
	if protocol.IsErrorReply(bs) || !strings.Contains(string(bs.ToBytes()), "65535") {
		t.Fatalf("BITFIELD SAT: %s", bs.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b102bit", "7", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b102bit", "1")), 7)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b102x", "1-0", "f", "a")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b102x", "2-0", "f", "b")), "2-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b102x", "3-0", "f", "c")), "3-0")
	xr := db.Exec(nil, utils.ToCmdLine("XRANGE", "b102x", "-", "+", "COUNT", "2"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "1-0") {
		t.Fatalf("XRANGE: %s", xr.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b102x", "MAXLEN", "2")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b102z2", "1", "x", "5", "y")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b102z3", "2", "y", "4", "z")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZUNION", "2", "b102z2", "b102z3", "WEIGHTS", "2", "1", "WITHSCORES")),
		[]string{"x", "2", "z", "4", "y", "12"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b102z2", "b102z3", "WITHSCORES")),
		[]string{"y", "7"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFF", "2", "b102z2", "b102z3")), []string{"x"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b102sa", "p", "q", "r")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b102sb", "q", "r")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "2", "b102sa", "b102sb")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SDIFF", "b102sa", "b102sb")), []string{"p"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b102lm", "1", "2", "3", "4", "5")), 5)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b102lm", "RIGHT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "5") {
		t.Fatalf("LMPOP: %s", lmp.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b102zm", "1", "a", "2", "b", "3", "c", "4", "d")), 4)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b102zm", "MAX", "COUNT", "2"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "d") {
		t.Fatalf("ZMPOP: %s", zmp.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b102lp", "x", "a", "x", "b", "x")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b102lp", "x", "COUNT", "2")), []string{"0", "2"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b102lp", "x", "RANK", "1")), 0)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b102c4", "hello")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b102c4", "10")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b102c4", "40", "GT")), 1)
	asserts.AssertIntReplyGreaterThan(t, db.Exec(nil, utils.ToCmdLine("TTL", "b102c4")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b102sort", "9", "1", "5", "3")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b102sort", "LIMIT", "1", "2")), []string{"3", "5"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b102g", "13.361389", "38.115556", "Palermo")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEORADIUS", "b102g", "13", "38", "50", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Palermo") {
		t.Fatalf("GEORADIUS: %s", geo.ToBytes())
	}

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b102f", "2.25")), "2.25")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "b102")), "b102")
}
