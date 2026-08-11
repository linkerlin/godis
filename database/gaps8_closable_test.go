package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps8GeoAddInvalidPairFormat(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("GEOADD", "kg", "200", "0", "bad"))
	asserts.AssertErrReply(t, r, "ERR invalid longitude,latitude pair 200.000000,0.000000")
}

func TestGaps8SInterCardNumkeys(t *testing.T) {
	db := makeTestDB()
	// Arity: SINTERCARD 0 alone → wrong number of arguments (registered arity -3).
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "0")),
		"ERR wrong number of arguments for 'sintercard' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "0", "LIMIT", "1")),
		"ERR numkeys should be greater than 0")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "-1", "a")),
		"ERR numkeys should be greater than 0")
}

func TestGaps8ZInterCardNumkeys(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "0")),
		"ERR wrong number of arguments for 'zintercard' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "0", "LIMIT", "1")),
		"ERR at least 1 input key is needed for 'zintercard' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "-1", "a")),
		"ERR at least 1 input key is needed for 'zintercard' command")
}

func TestGaps8MPopNumkeys(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZMPOP", "0", "k", "MIN")),
		"ERR numkeys should be greater than 0")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("LMPOP", "0", "k", "LEFT")),
		"ERR numkeys should be greater than 0")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BZMPOP", "0.01", "0", "k", "MIN")),
		"ERR numkeys should be greater than 0")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BLMPOP", "0.01", "0", "k", "LEFT")),
		"ERR numkeys should be greater than 0")
}
