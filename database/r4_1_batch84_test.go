package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 84 R4-1 extras: LPUSHX, GETBIT, miss HLEN/HEXISTS/SCARD/LLEN/EXISTS/UNLINK,
// SDIFFSTORE/SUNIONSTORE/SINTERSTORE, ZUNIONSTORE/ZSCORE, GETRANGE, SETEX/PERSIST,
// DECR/INCRBY/HINCRBY, BITOP AND.
func TestR41Batch84Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b84lx", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSH", "b84lx", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSHX", "b84lx", "b")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b84bit", "3", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b84bit", "3")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b84bit", "0")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "b84hmiss")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HEXISTS", "b84hmiss", "f")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b84smiss")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b84lmiss")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "b84emiss")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("UNLINK", "b84umiss")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b84sa", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b84sb", "b", "c", "d")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SDIFFSTORE", "b84sd", "b84sa", "b84sb")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b84sd")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SUNIONSTORE", "b84su", "b84sa", "b84sb")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "b84si", "b84sa", "b84sb")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b84za", "1", "a", "2", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b84zb", "1", "b", "3", "c")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "b84zu", "2", "b84za", "b84zb")), 3)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b84za", "a")), "1")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b84str", "hello")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b84str", "1", "3")), "ell")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SETEX", "b84sx", "120", "v")), "OK")
	asserts.AssertIntReplyGreaterThan(t, db.Exec(nil, utils.ToCmdLine("TTL", "b84sx")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PERSIST", "b84sx")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TTL", "b84sx")), -1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b84n", "10")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DECR", "b84n")), 9)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("INCRBY", "b84n", "5")), 14)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b84h", "n", "1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBY", "b84h", "n", "5")), 6)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b84b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b84b2", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "AND", "b84and", "b84b1", "b84b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b84and")), 1)
}
