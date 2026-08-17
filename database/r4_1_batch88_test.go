package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 88 R4-1 extras: LINSERT/LSET, Z*STORE, BITOP XOR, MSETNX/RENAMENX/COPY,
// GETEX+TTL, HSET multi, ZLEXCOUNT, LREM/SREM, RPOPLPUSH, SDIFFSTORE, PFADD, SETBIT.
func TestR41Batch88Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b88l", "a", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b88l", "BEFORE", "c", "b")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b88l", "0", "-1")), []string{"a", "b", "c"})
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b88l", "1", "x")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b88l", "1")), "x")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b88z", "1", "a", "2", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b88zu", "1", "b88z")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "b88zi", "1", "b88z")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGE", "b88z", "0", "0")), []string{"b"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b88b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b88b2", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b88bx", "b88b1", "b88b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b88bx", "0")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b88m1", "a", "b88m2", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("MSETNX", "b88m1", "x", "b88m3", "y")), 0)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b88r1", "v")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b88r1", "b88r2")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b88r2")), "v")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b88c1", "hi")), "OK")
	// COPY is server-level (sidecar-only); skip DB.Exec assert here.

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b88ge", "v")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b88ge", "EX", "100")), "v")
	ttl := db.Exec(nil, utils.ToCmdLine("TTL", "b88ge"))
	if ir, ok := ttl.(*protocol.IntReply); !ok || ir.Code < 1 || ir.Code > 100 {
		t.Fatalf("TTL after GETEX: %s", ttl.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b88h", "a", "1", "b", "2", "c", "3")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b88h")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HEXISTS", "b88h", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HEXISTS", "b88h", "z")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b88zl", "0", "a", "0", "b", "0", "c", "0", "d")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b88zl", "[a", "[c")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b88lr", "a", "b", "a", "c", "a")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b88lr", "2", "a")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b88lr")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b88sr", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SREM", "b88sr", "b", "z")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b88sr")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b88rp", "a", "b", "c")), 3)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("RPOPLPUSH", "b88rp", "b88rq")), "c")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b88rp")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b88rq", "0")), "c")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b88s1", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b88s2", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b88sd", "b88s1", "b88s2")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b88sd")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b88pf", "a", "b", "c")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b88pf")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b88sb", "7", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b88sb")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b88sb", "1")), 7)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b88touch", "t")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TOUCH", "b88touch")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("UNLINK", "b88touch")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "b88touch")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PTTL", "b88pttlmiss")), -2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRETIME", "b88etmiss")), -2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("APPEND", "b88ap", "xyz")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("STRLEN", "b88ap")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCRBY", "b88ib", "10")), 10)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b88hi", "n", "4")), 4)
}