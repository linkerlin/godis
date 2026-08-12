package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps16TypeScanCountMustBePositive(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("HSET", "h", "f", "v"))
	db.Exec(nil, utils.ToCmdLine("SADD", "s", "a"))
	db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1", "a"))

	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HSCAN", "h", "0", "COUNT", "0")),
		"ERR syntax error")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HSCAN", "h", "0", "COUNT", "-1")),
		"ERR syntax error")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HSCAN", "h", "0", "COUNT", "abc")),
		"ERR value is not an integer or out of range")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SSCAN", "s", "0", "COUNT", "0")),
		"ERR syntax error")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZSCAN", "z", "0", "COUNT", "0")),
		"ERR syntax error")

	// Missing key: Redis returns empty scan without validating COUNT.
	r := db.Exec(nil, utils.ToCmdLine("HSCAN", "nosuch", "0", "COUNT", "0"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 2 {
		t.Fatalf("missing key HSCAN COUNT 0: %T %s", r, r.ToBytes())
	}
	asserts.AssertBulkReply(t, mr.Replies[0], "0")
	asserts.AssertMultiBulkReplySize(t, mr.Replies[1], 0)
}

func TestGaps16ZSetNumkeysMustBePositive(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFF", "0")),
		"ERR wrong number of arguments for 'zdiff' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFF", "0", "z")),
		"ERR at least 1 input key is needed for 'zdiff' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFF", "-1", "z")),
		"ERR at least 1 input key is needed for 'zdiff' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZINTER", "0", "z")),
		"ERR at least 1 input key is needed for 'zinter' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZUNION", "0", "z")),
		"ERR at least 1 input key is needed for 'zunion' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZDIFFSTORE", "o", "0", "z")),
		"ERR at least 1 input key is needed for 'zdiffstore' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "o", "0", "z")),
		"ERR at least 1 input key is needed for 'zunionstore' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "o", "0", "z")),
		"ERR at least 1 input key is needed for 'zinterstore' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZUNIONSTORE", "o", "0")),
		"ERR wrong number of arguments for 'zunionstore' command")
}
