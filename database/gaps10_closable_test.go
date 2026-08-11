package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps10PopCountMissingKeyNegative(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "nosuch", "-1")),
		"ERR value is out of range, must be positive")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "nosuch", "-1")),
		"ERR value is out of range, must be positive")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "nosuch", "-1")),
		"ERR value is out of range, must be positive")
	asserts.AssertMultiBulkReplySize(t, db.Exec(nil, utils.ToCmdLine("LPOP", "nosuch", "0")), 0)
}

func TestGaps10XAddXTrimMaxLenNegative(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XADD", "x", "MAXLEN", "=", "-1", "*", "a", "1")),
		"ERR The MAXLEN argument must be >= 0.")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "x", "MAXLEN", "=", "-1")),
		"ERR The MAXLEN argument must be >= 0.")
	db.Exec(nil, utils.ToCmdLine("XADD", "x", "*", "a", "1"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XTRIM", "x", "MAXLEN", "~", "-1")),
		"ERR The MAXLEN argument must be >= 0.")
}

func TestGaps10HExpireNumFields(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("HSET", "h", "f1", "v1"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HEXPIRE", "h", "10", "FIELDS", "0")),
		"ERR wrong number of arguments for 'hexpire' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HEXPIRE", "h", "10", "FIELDS", "0", "f1")),
		"ERR Parameter `numFields` should be greater than 0")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "h", "10", "FIELDS", "-1", "f1")),
		"ERR Parameter `numFields` should be greater than 0")
}
