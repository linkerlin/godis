package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Gaps batch 6 — verified against Redis 8.10.0 (docker :6389, requirepass ylf).

func TestGaps6XReadCountNonPositiveUnlimited(t *testing.T) {
	db := makeTestDB()
	id := db.Exec(nil, utils.ToCmdLine("XADD", "xr", "*", "a", "1"))
	asserts.AssertNotError(t, id)

	for _, count := range []string{"0", "-1"} {
		r := db.Exec(nil, utils.ToCmdLine("XREAD", "COUNT", count, "STREAMS", "xr", "0-0"))
		asserts.AssertNotError(t, r)
		if len(r.ToBytes()) < 10 {
			t.Fatalf("XREAD COUNT %s should return entries, got %s", count, r.ToBytes())
		}
	}
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XREAD", "COUNT", "abc", "STREAMS", "xr", "0-0")),
		"ERR value is not an integer or out of range")
}

func TestGaps6XReadBlockTimeoutMessages(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("XADD", "xb", "*", "a", "1"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XREAD", "BLOCK", "-1", "STREAMS", "xb", "$")),
		"ERR timeout is negative")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XREAD", "BLOCK", "abc", "STREAMS", "xb", "$")),
		"ERR timeout is not an integer or out of range")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine(
		"XREADGROUP", "GROUP", "g", "c", "BLOCK", "-1", "STREAMS", "xb", ">")),
		"ERR timeout is negative")
}

func TestGaps6XReadGroupCountNonPositive(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "xg", "g", "$", "MKSTREAM")), "OK")
	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XADD", "xg", "*", "b", "2")))
	r := db.Exec(nil, utils.ToCmdLine("XREADGROUP", "GROUP", "g", "c", "COUNT", "0", "STREAMS", "xg", ">"))
	asserts.AssertNotError(t, r)
	if len(r.ToBytes()) < 10 {
		t.Fatalf("XREADGROUP COUNT 0 should return entries, got %s", r.ToBytes())
	}
}

func TestGaps6SInterCardLimitZeroUnlimited(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SADD", "sa", "a", "b", "c"))
	db.Exec(nil, utils.ToCmdLine("SADD", "sb", "b", "c", "d"))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "2", "sa", "sb", "LIMIT", "0")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "2", "sa", "sb", "LIMIT", "1")), 1)
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("SINTERCARD", "2", "sa", "sb", "LIMIT", "-1")),
		"ERR LIMIT can't be negative")
}

func TestGaps6SelectSwapDBMessages(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, server.Exec(c, utils.ToCmdLine("SELECT", "abc")),
		"ERR value is not an integer or out of range")
	asserts.AssertErrReply(t, server.Exec(c, utils.ToCmdLine("SELECT", "99")),
		"ERR DB index is out of range")
	asserts.AssertErrReply(t, server.Exec(c, utils.ToCmdLine("SWAPDB", "abc", "0")),
		"ERR invalid first DB index")
	asserts.AssertErrReply(t, server.Exec(c, utils.ToCmdLine("SWAPDB", "0", "abc")),
		"ERR invalid second DB index")
	asserts.AssertErrReply(t, server.Exec(c, utils.ToCmdLine("SWAPDB", "0", "99")),
		"ERR DB index is out of range")
}

func TestGaps6FCallNotFoundMessage(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("FCALL", "missing", "0")),
		"ERR Function not found")
}

func TestGaps6RestoreFreqUpperBound(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine(
		"RESTORE", "rk", "0", "xx", "REPLACE", "FREQ", "-1")),
		"ERR Invalid FREQ value, must be >= 0 and <= 255")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine(
		"RESTORE", "rk", "0", "xx", "REPLACE", "FREQ", "256")),
		"ERR Invalid FREQ value, must be >= 0 and <= 255")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine(
		"RESTORE", "rk", "0", "xx", "REPLACE", "IDLETIME", "-1")),
		"ERR Invalid IDLETIME value, must be >= 0")
}

func TestGaps6XPendingIdleParse(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "xp", "g", "$", "MKSTREAM")), "OK")
	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XADD", "xp", "*", "a", "1")))
	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine(
		"XREADGROUP", "GROUP", "g", "c", "COUNT", "1", "STREAMS", "xp", ">")))
	// Negative IDLE accepted (no effective filter).
	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine(
		"XPENDING", "xp", "g", "IDLE", "-1", "-", "+", "10")))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine(
		"XPENDING", "xp", "g", "IDLE", "abc", "-", "+", "10")),
		"ERR value is not an integer or out of range")
}
