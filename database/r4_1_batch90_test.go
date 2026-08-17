package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 90 R4-1 extras: LINSERT AFTER, ZRANGESTORE/ZDIFFSTORE, S*STORE, BITOP AND,
// SET EX/PSETEX, RENAME, LCS, ZCOUNT/ZINCRBY, GETSET, HSTRLEN, HEXPIRE/HTTL.
func TestR41Batch90Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b90l", "a", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b90l", "AFTER", "a", "b")), 3)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("LRANGE", "b90l", "0", "-1")), []string{"a", "b", "c"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPOS", "b90l", "b")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b90z", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGESTORE", "b90zs", "b90z", "0", "1")), 2)
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "b90zs", "0", "-1")), []string{"a", "b"})
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCOUNT", "b90z", "1", "2")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZINCRBY", "b90z", "1", "a")), "2")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b90z1", "1", "a", "2", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b90z2", "1", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFFSTORE", "b90zd", "2", "b90z1", "b90z2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCARD", "b90zd")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b90s1", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b90s2", "b", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SUNIONSTORE", "b90su", "b90s1", "b90s2")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b90si", "b90s1", "b90s2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b90si")), 1)
	sm := db.Exec(nil, utils.ToCmdLine("SMISMEMBER", "b90s1", "a"))
	if protocol.IsErrorReply(sm) {
		t.Fatalf("SMISMEMBER: %s", sm.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b90b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b90b1", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b90b2", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "AND", "b90ba", "b90b1", "b90b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b90ba", "0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b90ba", "1")), 0)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b90ex", "v", "EX", "50")), "OK")
	ttl := db.Exec(nil, utils.ToCmdLine("TTL", "b90ex"))
	if ir, ok := ttl.(*protocol.IntReply); !ok || ir.Code < 1 || ir.Code > 50 {
		t.Fatalf("TTL SET EX: %s", ttl.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PSETEX", "b90px", "5000", "v")), "OK")
	pttl := db.Exec(nil, utils.ToCmdLine("PTTL", "b90px"))
	if ir, ok := pttl.(*protocol.IntReply); !ok || ir.Code < 1 || ir.Code > 5000 {
		t.Fatalf("PTTL PSETEX: %s", pttl.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b90rn1", "v")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RENAME", "b90rn1", "b90rn2")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b90rn2")), "v")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b90l1", "abc")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b90l2", "abd")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "b90l1", "b90l2", "LEN")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "REFCOUNT", "b90l1")), 1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b90gs", "old")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b90gs", "new")), "old")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b90gs")), "new")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b90ib", "0.5")), "0.5")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b90h", "f", "hello")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSTRLEN", "b90h", "f")), 5)
	hexp := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "b90h", "100", "FIELDS", "1", "f"))
	if protocol.IsErrorReply(hexp) {
		t.Fatalf("HEXPIRE: %s", hexp.ToBytes())
	}
	httl := db.Exec(nil, utils.ToCmdLine("HTTL", "b90h", "FIELDS", "1", "f"))
	if protocol.IsErrorReply(httl) {
		t.Fatalf("HTTL: %s", httl.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECR", "b90decr")), -1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCR", "b90incr")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "b90e1", "b90e2")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("UNLINK", "b90u1", "b90u2")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TOUCH", "b90t1", "b90t2")), 0)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TYPE", "b90none")), "none")
}
