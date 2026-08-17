package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 75 R4-1 extras: EXPIRE XX/GT/LT, ZADD NX/XX, BITCOUNT byte range.
func TestR41Batch75ExpireZAddBitCount(t *testing.T) {
	db := makeTestDB()

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b75e", "v")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b75e", "100", "XX")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b75e", "100")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b75e", "50", "XX")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b75e", "200", "GT")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "b75e", "10", "LT")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b75z", "NX", "1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b75z", "NX", "2", "a")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "b75z", "XX", "3", "a")), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "b75z", "a")), "3")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b75b", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b75b", "8", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b75b", "0", "0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITCOUNT", "b75b", "1", "1")), 1)
}
