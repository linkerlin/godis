package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 95 R4-1 extras: LRANGE neg, LPOP COUNT, ZRANGE/ZREV* WITHSCORES, ZMSCORE,
// ZUNION/ZINTER/ZDIFF, SET XX GET, GETEX PX, BITFIELD u4, BITCOUNT range, LPOS COUNT 0,
// LMPOP/ZMPOP multi, SINTERCARD, XREVRANGE, INCRBYFLOAT, GEORADIUS.
func TestR41Batch95Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b95l", "a", "b", "c", "d", "e", "f")), 6)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b95l", "-3", "-1")), []string{"d", "e", "f"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b95l", "-2")), "e")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b95l", "1", "c")), 1)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b95l", "2")), []string{"a", "b"})
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b95l")), "f")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b95z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b95z", "1", "3", "WITHSCORES")),
		[]string{"b", "2", "c", "3", "d", "4"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGE", "b95z", "0", "2")), []string{"e", "d", "c"})
	zm := db.Exec(nil, utils.ToCmdLine("ZMSCORE", "b95z", "a", "e", "missing"))
	if protocol.IsErrorReply(zm) || !strings.Contains(string(zm.ToBytes()), "5") {
		t.Fatalf("ZMSCORE: %s", zm.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b95z2", "1", "x", "2", "y")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b95z3", "1", "y", "3", "z")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZUNION", "2", "b95z2", "b95z3")), []string{"x", "y", "z"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b95z2", "b95z3")), []string{"y"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFF", "2", "b95z2", "b95z3")), []string{"x"})

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b95xx", "v")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b95xx", "w", "XX", "GET")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b95xx", "PX", "3000")), "w")
	asserts.AssertIntReplyGreaterThan(t, db.Exec(nil, utils.ToCmdLine("PTTL", "b95xx")), 0)

	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b95bf", "SET", "u4", "0", "7"))
	if protocol.IsErrorReply(bf) {
		t.Fatalf("BITFIELD SET: %s", bf.ToBytes())
	}
	bi := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b95bf", "INCRBY", "u4", "0", "1"))
	if protocol.IsErrorReply(bi) || !strings.Contains(string(bi.ToBytes()), "8") {
		t.Fatalf("BITFIELD INCRBY: %s", bi.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b95bit", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b95bit", "8", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b95bit", "0", "0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b95bit", "1", "1")), 8)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b95x", "10-0", "f", "a")), "10-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b95x", "20-0", "f", "b")), "20-0")
	xr := db.Exec(nil, utils.ToCmdLine("XREVRANGE", "b95x", "+", "-", "COUNT", "1"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "20-0") {
		t.Fatalf("XREVRANGE: %s", xr.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b95lm", "1", "2", "3")), 3)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b95lm", "LEFT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "1") {
		t.Fatalf("LMPOP: %s", lmp.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b95zm", "1", "a", "2", "b", "3", "c")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b95zm", "MIN", "COUNT", "2"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "a") {
		t.Fatalf("ZMPOP: %s", zmp.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b95lp", "x", "y", "x", "z", "x")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b95lp", "x", "RANK", "3")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b95lp", "x", "COUNT", "0")), []string{"0", "2", "4"})

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b95f", "1.25")), "1.25")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b95f", "0.75")), "2")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b95g", "13.361389", "38.115556", "Palermo")), 1)
	geo := db.Exec(nil, utils.ToCmdLine("GEORADIUS", "b95g", "15", "37", "200", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Palermo") {
		t.Fatalf("GEORADIUS: %s", geo.ToBytes())
	}

	nx := db.Exec(nil, utils.ToCmdLine("SET", "b95nx", "v", "NX"))
	asserts.AssertStatusReply(t, nx, "OK")
	nx2 := db.Exec(nil, utils.ToCmdLine("SET", "b95nx", "w", "NX"))
	if _, ok := nx2.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET NX dup: %T %s", nx2, nx2.ToBytes())
	}

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "b95")), "b95")
}
