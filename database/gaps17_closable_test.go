package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps17BitPosBitArgumentERR(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SET", "k", "v"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "k", "2")),
		"ERR The bit argument must be 1 or 0.")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "k", "-1")),
		"ERR The bit argument must be 1 or 0.")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "k", "abc")),
		"ERR value is not an integer or out of range")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "k", "01")),
		"ERR value is not an integer or out of range")
}

func TestGaps17LInsertValidateDirBeforeMiss(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "miss", "FOO", "x", "y")),
		"ERR syntax error")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LINSERT", "miss", "BEFORE", "x", "y")), 0)
}

func TestGaps17ZLexValidateBeforeMiss(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "nosuch", "a", "b")),
		"ERR min or max not valid string range item")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYLEX", "nosuch", "a", "b")),
		"ERR min or max not valid string range item")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZREMRANGEBYLEX", "nosuch", "a", "b")),
		"ERR min or max not valid string range item")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYLEX", "nosuch", "a", "b")),
		"ERR min or max not valid string range item")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZLEXCOUNT", "nosuch", "-", "+")), 0)
}
