package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps18RestoreTTLBeforePayload(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("RESTORE", "k", "-1", "xx")),
		"ERR Invalid TTL value, must be >= 0")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("RESTORE", "k", "abc", "xx")),
		"ERR value is not an integer or out of range")
}

func TestGaps18SRandMemberCountBeforeMiss(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "nosuch", "abc")),
		"ERR value is not an integer or out of range")
	r := db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "nosuch", "2"))
	asserts.AssertMultiBulkReplySize(t, r, 0)
}

func TestGaps18PopCountNonIntegerMustBePositive(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "nosuch", "abc")),
		"ERR value is out of range, must be positive")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("RPOP", "nosuch", "abc")),
		"ERR value is out of range, must be positive")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMIN", "nosuch", "abc")),
		"ERR value is out of range, must be positive")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "nosuch", "abc")),
		"ERR value is out of range, must be positive")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SPOP", "nosuch", "abc")),
		"ERR value is out of range, must be positive")
}

func TestGaps18LIndexMissSkipsIndexParse(t *testing.T) {
	db := makeTestDB()
	asserts.AssertNullBulk(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "nosuch", "abc")))
	db.Exec(nil, utils.ToCmdLine("LPUSH", "l", "a"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("LINDEX", "l", "abc")),
		"ERR value is not an integer or out of range")
}

func TestGaps18XGroupCreateMissingKeyWording(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "s", "g", "$")),
		"ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.")
}
