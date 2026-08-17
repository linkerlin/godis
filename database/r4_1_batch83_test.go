package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 83 R4-1 extras: BITOP OR/XOR, LINDEX/LSET/LINSERT, GETEX,
// STRLEN/XLEN miss, APPEND create, HSTRLEN, ZLEXCOUNT, ZINTERCARD,
// SINTERCARD, ZREMRANGEBYRANK, RPUSHX, BITPOS, PFCOUNT multi.
func TestR41Batch83Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b83b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b83b2", "1", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b83or", "b83b1", "b83b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b83or")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "XOR", "b83xor", "b83b1", "b83b2")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b83xor")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b83b1", "1")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b83l", "a", "b", "c")), 3)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "b83l", "1")), "b")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("LSET", "b83l", "1", "B")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "b83l", "BEFORE", "B", "X")), 4)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b83l")), 4)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b83lx", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSH", "b83lx", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("RPUSHX", "b83lx", "b")), 2)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b83ge", "abc")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETEX", "b83ge", "EX", "120")), "abc")
	asserts.AssertIntReplyGreaterThan(t, db.Exec(nil, utils.ToCmdLine("TTL", "b83ge")), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("STRLEN", "b83missing")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "b83xmiss")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("APPEND", "b83ap", "hi")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "b83h", "f", "hello")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSTRLEN", "b83h", "f")), 5)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b83sa", "a", "b", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b83sb", "b", "c", "d")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "2", "b83sa", "b83sb")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b83za", "1", "a", "2", "b", "3", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b83zb", "1", "b", "2", "c", "3", "d")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "b83za", "b83zb")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYRANK", "b83za", "0", "0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCARD", "b83za")), 2)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b83zl", "0", "a", "0", "b", "0", "c")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "b83zl", "[a", "[c")), 3)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b83pf1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b83pf2", "b")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b83pf1", "b83pf2")), 2)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b83n", "1")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "b83n", "0.5")), "1.5")
}
