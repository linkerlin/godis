package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Redis: SPOP/LPOP/RPOP/ZPOP* count 0 → empty (no mutation); negative → ERR.
// ZMPOP/LMPOP COUNT 0 → ERR count should be greater than 0.
func TestPopCountZeroEdges(t *testing.T) {
	db := makeTestDB()

	db.Exec(nil, utils.ToCmdLine("SADD", "s", "a", "b", "c"))
	asserts.AssertMultiBulkReplySize(t, db.Exec(nil, utils.ToCmdLine("SPOP", "s", "0")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "s")), 3)
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "s", "-1")),
		"ERR value is out of range, must be positive")

	db.Exec(nil, utils.ToCmdLine("LPUSH", "l", "a", "b"))
	asserts.AssertMultiBulkReplySize(t, db.Exec(nil, utils.ToCmdLine("LPOP", "l", "0")), 0)
	asserts.AssertMultiBulkReplySize(t, db.Exec(nil, utils.ToCmdLine("RPOP", "l", "0")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "l")), 2)
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "l", "-1")),
		"ERR value is out of range, must be positive")

	db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1", "a", "2", "b"))
	asserts.AssertMultiBulkReplySize(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "z", "0")), 0)
	asserts.AssertMultiBulkReplySize(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "z", "0")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCARD", "z")), 2)
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "z", "-1")),
		"ERR value is out of range, must be positive")

	// WRONGTYPE still wins for ZPOPMIN count 0 on a string (Redis).
	db.Exec(nil, utils.ToCmdLine("SET", "w", "v"))
	if r := db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "w", "0")); !protocol.IsErrorReply(r) {
		t.Fatalf("expected WRONGTYPE, got %s", r.ToBytes())
	}
}

func TestMPopCountMustBePositive(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1", "a"))
	db.Exec(nil, utils.ToCmdLine("LPUSH", "l", "a"))

	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZMPOP", "1", "z", "MIN", "COUNT", "0")),
		"ERR count should be greater than 0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCARD", "z")), 1)

	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("LMPOP", "1", "l", "LEFT", "COUNT", "0")),
		"ERR count should be greater than 0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "l")), 1)
}
