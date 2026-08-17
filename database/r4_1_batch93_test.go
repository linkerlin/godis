package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 93 R4-1 extras: LINDEX/LSET/LINSERT, ZRANGE WITHSCORES, ZREVRANGEBYSCORE LIMIT,
// HMGET null, KEEPTTL, GETEX EX, BITFIELD u8, XREVRANGE, SORT ALPHA, LPOS COUNT,
// ZMPOP MAX, LMPOP RIGHT, RENAMENX ok, COPY, SMISMEMBER, SETRANGE.
func TestR41Batch93Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b93l", "a", "b", "c", "d")), 4)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b93l", "-1")), "d")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b93l", "1", "-2")), []string{"b", "c"})
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b93l", "1", "B")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b93l", "BEFORE", "B", "X")), 5)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b93z", "1", "a", "2", "b", "3", "c", "4", "d")), 4)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b93z", "0", "-1", "WITHSCORES")),
		[]string{"a", "1", "b", "2", "c", "3", "d", "4"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "b93z", "3", "1", "LIMIT", "0", "2")),
		[]string{"c", "b"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCOUNT", "b93z", "2", "3")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b93h", "a", "1", "b", "2", "c", "3")), 3)
	hm := db.Exec(nil, utils.ToCmdLine("HMGET", "b93h", "a", "missing", "b"))
	asserts.AssertMultiBulkReply(t, hm, []string{"1", "", "2"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b93s", "x", "y", "z")), 3)
	sm := db.Exec(nil, utils.ToCmdLine("SMISMEMBER", "b93s", "x", "q", "z"))
	if protocol.IsErrorReply(sm) || !strings.Contains(string(sm.ToBytes()), "1") {
		t.Fatalf("SMISMEMBER: %s", sm.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b93st", "hello")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("APPEND", "b93st", "!")), 6)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETRANGE", "b93st", "5", "WORLD")), 10)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b93st")), "helloWORLD")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b93ge", "v", "EX", "100")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b93ge", "EX", "50")), "v")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b93kt", "keep", "EX", "200")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b93kt", "keep2", "KEEPTTL")), "OK")
	ttl := db.Exec(nil, utils.ToCmdLine("TTL", "b93kt"))
	asserts.AssertIntReplyGreaterThan(t, ttl, 0)

	bf := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b93bf", "SET", "u8", "0", "42"))
	if protocol.IsErrorReply(bf) || !strings.Contains(string(bf.ToBytes()), "0") {
		t.Fatalf("BITFIELD SET: %s", bf.ToBytes())
	}
	bg := db.Exec(nil, utils.ToCmdLine("BITFIELD", "b93bf", "GET", "u8", "0"))
	if protocol.IsErrorReply(bg) || !strings.Contains(string(bg.ToBytes()), "42") {
		t.Fatalf("BITFIELD GET: %s", bg.ToBytes())
	}

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "b93x", "1-0", "f", "v")), "1-0")
	xr := db.Exec(nil, utils.ToCmdLine("XREVRANGE", "b93x", "+", "-", "COUNT", "1"))
	if protocol.IsErrorReply(xr) || !strings.Contains(string(xr.ToBytes()), "1-0") {
		t.Fatalf("XREVRANGE: %s", xr.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b93sort", "c", "a", "b")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("SORT", "b93sort", "ALPHA")), []string{"a", "b", "c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b93lp", "a", "b", "a", "c", "a")), 5)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b93lp", "a", "RANK", "2")), 2)
	lpos := db.Exec(nil, utils.ToCmdLine("LPOS", "b93lp", "a", "COUNT", "2"))
	asserts.AssertMultiBulkReply(t, lpos, []string{"0", "2"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b93zm", "1", "a", "2", "b")), 2)
	zmp := db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "b93zm", "MAX", "COUNT", "1"))
	if protocol.IsErrorReply(zmp) || !strings.Contains(string(zmp.ToBytes()), "b") {
		t.Fatalf("ZMPOP MAX: %s", zmp.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b93lm", "x", "y", "z")), 3)
	lmp := db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "b93lm", "RIGHT", "COUNT", "2"))
	if protocol.IsErrorReply(lmp) || !strings.Contains(string(lmp.ToBytes()), "z") {
		t.Fatalf("LMPOP RIGHT: %s", lmp.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b93r1", "a")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b93r1", "b93r2")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b93r2")), "a")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("PING", "hello93")), "hello93")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TYPE", "b93miss")), "none")
}
