package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Gaps batch 4 — verified against Redis 8.10.0 (docker :6389).

func TestGaps4BLMPOPNoCountArity(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSH", "k", "a")), 1)
	r := db.Exec(nil, utils.ToCmdLine("BLMPOP", "0", "1", "k", "LEFT"))
	asserts.AssertMultiBulkReplySize(t, r, 2)
}

func TestGaps4BlockingTimeoutErrors(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BLPOP", "k", "-1")), "ERR timeout is negative")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BRPOP", "k", "-0.1")), "ERR timeout is negative")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BLMPOP", "-1", "1", "k", "LEFT")), "ERR timeout is negative")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BZPOPMIN", "k", "-1")), "ERR timeout is negative")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BZMPOP", "-1", "1", "k", "MIN")), "ERR timeout is negative")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BLPOP", "k", "abc")), "ERR timeout is not a float or out of range")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BLPOP", "k", "nan")), "ERR timeout is not a float or out of range")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BLPOP", "k", "inf")), "ERR timeout is out of range")
}

func TestGaps4ZADDOptionIncompat(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "z", "NX", "GT", "1", "a")),
		"ERR GT, LT, and/or NX options at the same time are not compatible")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "z", "NX", "LT", "1", "a")),
		"ERR GT, LT, and/or NX options at the same time are not compatible")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "z", "GT", "LT", "1", "a")),
		"ERR GT, LT, and/or NX options at the same time are not compatible")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZADD", "z", "XX", "NX", "1", "a")),
		"ERR XX and NX options at the same time are not compatible")
}

func TestGaps4PFADDEmptyKey(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "pf")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "pf")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "pf")), 0)
}

func TestGaps4IncrByFloatNaN(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "x", "nan")),
		"ERR value is not a valid float")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "x", "inf")),
		"ERR increment would produce NaN or Infinity")
	db.Exec(nil, utils.ToCmdLine("SET", "s", "v"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "s", "inf")),
		"ERR value is not a valid float")
	db.Exec(nil, utils.ToCmdLine("SET", "x", "nan"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "x", "1")),
		"ERR value is not a valid float")
	db.Exec(nil, utils.ToCmdLine("SET", "y", "inf"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("INCRBYFLOAT", "y", "1")),
		"ERR increment would produce NaN or Infinity")

	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBYFLOAT", "h", "f", "nan")),
		"ERR value is not a valid float")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBYFLOAT", "h", "f", "inf")),
		"ERR value is NaN or Infinity")
	db.Exec(nil, utils.ToCmdLine("HSET", "h", "g", "nan"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HINCRBYFLOAT", "h", "g", "1")),
		"ERR hash value is not a float")
}
