package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 100 R4-1 extras: LPOP COUNT, ZRANGE/ZREVRANGEBYSCORE, ZMSCORE, HMGET,
// SET NX GET, GETEX PX, BITFIELD i8, XREVRANGE, ZUNION WITHSCORES, SDIFF/SINTER,
// LMPOP/ZMPOP, LPOS COUNT 0, EXPIRE NX/LT, SORT LIMIT, GEOSEARCH, TOUCH/UNLINK.
func TestR41Batch100Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b100l", "a", "b", "c", "d", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b100l", "2")), []string{"a", "b"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b100l")), "e")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b100l", "0", "-1")), []string{"c", "d"})
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b100l", "0", "A")), "OK")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b100z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b100z", "2", "4", "WITHSCORES")),
		[]string{"c", "3", "d", "4", "e", "5"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b100z", "4", "2", "LIMIT", "0", "2")),
		[]string{"d", "c"})
	zm := db.Exec(nil, utils.ToCmdLine("ZMSCORE", "b100z", "a", "c", "missing"))
	if protocol.IsErrorReply(zm) || !strings.Contains(string(zm.ToBytes()), "1") {
		t.Fatalf("ZMSCORE: %s", zm.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b100h", "a", "1", "b", "2", "c", "3")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HMGET", "b100h", "a", "c", "miss")), []string{"1", "3", ""})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b100h", "a", "4")), 5)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b100st", "foobar")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b100st", "0", "2")), "foo")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b100st", "3", "XXX")), 6)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b100nx", "v", "NX")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b100nx", "w", "NX", "GET")), "v")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b100xx", "v")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b100xx", "w", "XX", "GET")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b100xx", "PX", "4000")), "w")

	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b100bf", "SET", "i8", "0", "-10"))
	if protocol.IsErrorReply(bf) {
		t.Fatalf("BITFIELD SET: %s", bf.ToBytes())
	}
	bg := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b100bf", "GET", "i8", "0"))
	if protocol.IsErrorReply(bg) || !strings.Contains(string(bg.ToBytes()), "-10") {
		t.Fatalf("BITFIELD GET: %s", bg.ToBytes())
	}
	bi := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b100bf", "INCRBY", "i8", "0", "3"))
	if protocol.IsErrorReply(bi) || !strings.Contains(string(bi.ToBytes()), "-7") {
		t.Fatalf("BITFIELD INCRBY: %s", bi.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b100bit", "4", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b100bit", "1")), 4)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b100x", "10-0", "f", "a")), "10-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b100x", "20-0", "f", "b")), "20-0")
	xr := db.Exec(nil, utils.ToCmdLine("XREVRANGE", "b100x", "+", "-", "COUNT", "1"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "20-0") {
		t.Fatalf("XREVRANGE: %s", xr.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b100z2", "1", "x", "3", "y")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b100z3", "2", "y", "4", "z")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZUNION", "2", "b100z2", "b100z3", "WITHSCORES")),
		[]string{"x", "1", "z", "4", "y", "5"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b100z2", "b100z3")), []string{"y"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFF", "2", "b100z2", "b100z3")), []string{"x"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b100sa", "p", "q")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b100sb", "q", "r")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SDIFF", "b100sa", "b100sb")), []string{"p"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SINTER", "b100sa", "b100sb")), []string{"q"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b100lm", "1", "2", "3", "4")), 4)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b100lm", "LEFT", "COUNT", "3"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "1") {
		t.Fatalf("LMPOP: %s", lmp.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b100zm", "1", "a", "2", "b", "3", "c", "4", "d")), 4)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b100zm", "MIN", "COUNT", "2"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "a") {
		t.Fatalf("ZMPOP: %s", zmp.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b100lp", "x", "a", "x", "b", "x")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b100lp", "x", "COUNT", "0")), []string{"0", "2", "4"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b100lp", "x", "RANK", "2")), 2)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b100c4", "v")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b100c4", "50", "NX")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b100c4", "20", "LT")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b100sort", "c", "a", "b")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b100sort", "ALPHA", "LIMIT", "1", "2")), []string{"b", "c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b100g", "13.361389", "38.115556", "Palermo")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "b100g", "FROMLONLAT", "13", "38", "BYRADIUS", "50", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Palermo") {
		t.Fatalf("GEOSEARCH: %s", geo.ToBytes())
	}

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b100f", "1.5")), "1.5")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "b100")), "b100")
}
