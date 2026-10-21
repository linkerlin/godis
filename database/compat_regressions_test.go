package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Regressions found by the real Redis 8.10 R4-1 sidecar run.

func TestCompatZPopMaxDescendingOrder(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1", "a", "2", "b", "3", "c"))
	r := db.Exec(nil, utils.ToCmdLine("ZPOPMAX", "z", "2"))
	b := string(r.ToBytes())
	if i, j := strings.Index(b, "c"), strings.Index(b, "b"); i < 0 || j < 0 || i > j {
		t.Fatalf("ZPOPMAX must return highest score first: %s", r.ToBytes())
	}
}

func TestCompatZRangeByScoreInfinityBorders(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e"))
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYSCORE", "z", "3", "+inf")),
		[]string{"c", "d", "e"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGEBYSCORE", "z", "+inf", "3")),
		[]string{"e", "d", "c"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGEBYSCORE", "z", "-inf", "2")),
		[]string{"a", "b"})
}

func TestCompatLCSTieBreakMatchesRedis(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SET", "s1", "algorithm"))
	db.Exec(nil, utils.ToCmdLine("SET", "s2", "logarithm"))
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "s1", "s2")), "lorithm")
}
