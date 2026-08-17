package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 76 R4-1 extras: ZADD CH/GT/LT/INCR, SREM, GETRANGE positive.
func TestR41Batch76ZAddSRemGetRange(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b76z", "1", "a", "2", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b76z", "CH", "3", "a", "4", "c")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b76z", "a")), "3")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b76z", "GT", "5", "a")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b76z", "a")), "5")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b76z", "LT", "1", "a")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b76z", "a")), "1")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b76z", "INCR", "0.5", "b")), "2.5")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SADD", "b76s", "a", "b")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SREM", "b76s", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b76s")), 1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b76str", "hello")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "b76str", "1", "3")), "ell")
}
