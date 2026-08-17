package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 80 R4-1 extras: GETSET, TYPE none, EXPIRE+TTL, HDEL/SREM multi,
// LRANGE neg, ZCOUNT, TOUCH, RENAMENX miss, PEXPIRE+PTTL, MSET.
func TestR41Batch80Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b80gs", "old")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETSET", "b80gs", "new")), "old")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b80gs")), "new")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TYPE", "b80missing")), "none")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b80ttl", "v")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b80ttl", "120")), 1)
	ttl := db.Exec(nil, utils.ToCmdLine("TTL", "b80ttl"))
	asserts.AssertIntReplyGreaterThan(t, ttl, 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b80h", "a", "1", "b", "2")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HDEL", "b80h", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "b80h")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b80s", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SREM", "b80s", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b80s")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b80l", "a", "b", "c")), 3)
	r := db.Exec(nil, utils.ToCmdLine("LRANGE", "b80l", "-2", "-1"))
	asserts.AssertMultiBulkReply(t, r, []string{"b", "c"})

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b80z", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCOUNT", "b80z", "1", "2")), 2)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b80touch", "v")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TOUCH", "b80touch")), 1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b80rn", "a")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b80rn2", "b")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RENAMENX", "b80rn", "b80rn2")), 0)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b80pt", "v")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PEXPIRE", "b80pt", "120000")), 1)
	asserts.AssertIntReplyGreaterThan(t, db.Exec(nil, utils.ToCmdLine("PTTL", "b80pt")), 0)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("MSET", "b80m1", "a", "b80m2", "b")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "b80m1")), "a")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("STRLEN", "b80m1")), 1)
}
