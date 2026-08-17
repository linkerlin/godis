package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 85 R4-1 extras: miss GETBIT/PFCOUNT/BITCOUNT/BITPOS/PTTL/PERSIST/EXPIRETIME,
// HSETNX exist, SMOVE/ZCOUNT/HSTRLEN/LREM/ZREM/HDEL miss, SETRANGE create,
// INCR/DECR/APPEND new, STRLEN empty, RPUSHX hit, SETNX exist, SINTERCARD/ZINTERCARD 0,
// UNLINK/TOUCH multi-miss, BITFIELD GET, ZINCRBY/HINCRBY new, SUBSTR, LCS LEN,
// GETEX, TYPE, ECHO, SISMEMBER miss, XADD/XLEN/XDEL, PFADD/PFCOUNT,
// SUNIONSTORE. (COPY/MOVE are server-level; sidecar-only.)
func TestR41Batch85Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b85bitmiss", "0")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b85pfmiss")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b85bitcmiss")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b85bitpmiss", "1")), -1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PTTL", "b85pttlmiss")), -2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PERSIST", "b85persistmiss")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRETIME", "b85etmiss")), -2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b85h", "f", "v")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSETNX", "b85h", "f", "x")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSTRLEN", "b85hmiss", "f")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HDEL", "b85hmiss", "f")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b85hi", "n", "3")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SMOVE", "b85smiss", "b85smiss2", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "1", "b85smiss")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b85sa", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SISMEMBER", "b85sa", "z")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SUNIONSTORE", "b85su", "b85sa")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCOUNT", "b85zmiss", "-inf", "+inf")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREM", "b85zmiss", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "1", "b85zmiss")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINCRBY", "b85zi", "1.5", "m")), "1.5")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LREM", "b85lmiss", "1", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b85rx", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b85rx", "b")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b85sr", "0", "ab")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCR", "b85incr")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECR", "b85decr")), -1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("APPEND", "b85ap", "x")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b85empty", "")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("STRLEN", "b85empty")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETNX", "b85empty", "z")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("UNLINK", "b85u1", "b85u2")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TOUCH", "b85t1", "b85t2")), 0)

	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b85bf", "GET", "u8", "0"))
	asserts.AssertNotError(t, bf)
	if !strings.Contains(string(bf.ToBytes()), "0") {
		t.Fatalf("BITFIELD GET miss: %s", bf.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b85sub", "hello")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("SUBSTR", "b85sub", "0", "1")), "he")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b85sub", "PERSIST")), "hello")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TYPE", "b85sub")), "string")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ECHO", "b85")), "b85")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b85l1", "abc")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b85l2", "abd")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b85l1", "b85l2", "LEN")), 2)

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b85x", "1-0", "f", "v")), "1-0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b85x")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDEL", "b85x", "1-0")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b85pf", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b85pf")), 1)
}
