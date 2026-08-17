package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 98 R4-1 extras: LRANGE/RPOP COUNT, ZRANGE/ZREV* WITHSCORES, ZMSCORE, HMGET,
// SETRANGE/SUBSTR, SET NX GET, GETEX EX, BITFIELD u16, XRANGE/XTRIM, ZUNION/ZINTER/ZDIFF
// WITHSCORES, LMPOP/ZMPOP, LPOS, RENAME, SORT LIMIT, GEORADIUS, INCRBYFLOAT.
func TestR41Batch98Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b98l", "1", "2", "3", "4", "5")), 5)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b98l", "0", "2")), []string{"1", "2", "3"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b98l", "-2", "-1")), []string{"4", "5"})
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b98l", "2", "99")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "b98l")), "1")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "b98l", "2")), []string{"5", "4"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b98z", "1", "a", "2", "b", "3", "c", "4", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b98z", "0", "-1", "WITHSCORES")),
		[]string{"a", "1", "b", "2", "c", "3", "d", "4"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGE", "b98z", "1", "2", "WITHSCORES")),
		[]string{"c", "3", "b", "2"})
	zm := db.Exec(nil, utils.ToCmdLine("ZMSCORE", "b98z", "b", "c", "missing"))
	if protocol.IsErrorReply(zm) || !strings.Contains(string(zm.ToBytes()), "2") {
		t.Fatalf("ZMSCORE: %s", zm.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b98h", "a", "1", "b", "2", "c", "3", "d", "4")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HDEL", "b98h", "b", "d")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("HMGET", "b98h", "a", "c", "missing")), []string{"1", "3", ""})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b98h", "a", "9")), 10)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b98st", "hello")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b98st", "0", "HELLO")), 5)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUBSTR", "b98st", "1", "3")), "ELL")

	nx := db.Exec(nil, utils.ToCmdLine("SET", "b98nx", "v", "NX", "GET"))
	if _, ok := nx.(*protocol.NullBulkReply); !ok {
		t.Fatalf("SET NX GET miss: %T %s", nx, nx.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b98xx", "v")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b98xx", "w", "XX", "GET")), "v")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b98xx", "EX", "30")), "w")

	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b98bf", "SET", "u16", "0", "1000"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "0") {
		t.Fatalf("BITFIELD SET: %s", bf.ToBytes())
	}
	bi := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b98bf", "INCRBY", "u16", "0", "24"))
	if protocol.IsErrorReply(bi) || !strings.Contains(string(bi.ToBytes()), "1024") {
		t.Fatalf("BITFIELD INCRBY: %s", bi.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b98bit", "15", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b98bit", "1")), 15)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b98x", "1-0", "k", "v")), "1-0")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b98x", "2-0", "k2", "v2")), "2-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "b98x", "MAXLEN", "1")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b98z2", "1", "p", "2", "q")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b98z3", "2", "q", "3", "r")), 2)
	zu := db.Exec(nil, utils.ToCmdLine("ZUNION", "2", "b98z2", "b98z3", "WITHSCORES"))
	asserts.AssertMultiBulkReply(t, zu, []string{"p", "1", "r", "3", "q", "4"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "b98z2", "b98z3", "WITHSCORES")), []string{"q", "4"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFF", "2", "b98z2", "b98z3", "WITHSCORES")), []string{"p", "1"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b98lm", "a", "b", "c", "d")), 4)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b98lm", "RIGHT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "d") {
		t.Fatalf("LMPOP: %s", lmp.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b98zm", "1", "a", "2", "b", "3", "c")), 3)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b98zm", "MAX", "COUNT", "2"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "c") {
		t.Fatalf("ZMPOP: %s", zmp.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b98lp", "a", "x", "a", "y", "a")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b98lp", "a", "RANK", "-1")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b98lp", "a", "COUNT", "2")), []string{"0", "2"})

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b98c1", "HELLO!")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b98c1", "b98c3")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b98c3", "b98c4")), 1)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b98f", "0.5")), "0.5")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b98f", "1.5")), "2")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b98sort", "b", "a", "c")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b98sort", "ALPHA", "LIMIT", "0", "2")), []string{"a", "b"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "b98g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")), 2)
	geo := db.Exec(nil, utils.ToCmdLine("GEORADIUS", "b98g", "14", "37", "200", "km", "ASC", "COUNT", "1"))
	if protocol.IsErrorReply(geo) || !strings.Contains(string(geo.ToBytes()), "Catania") {
		t.Fatalf("GEORADIUS: %s", geo.ToBytes())
	}

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "b98")), "b98")
}
