package database

import (
	"fmt"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTSearchWithoutCount verifies WITHOUTCOUNT reports returned-doc count
// (not full hit total) when LIMIT truncates — Redis 8.x Query Engine semantics.
func TestFTSearchWithoutCount(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "wcidx", "SCHEMA", "title", "TEXT",
	)), "OK")
	for i := 1; i <= 12; i++ {
		id := fmt.Sprintf("wc:%d", i)
		if r := db.Exec(nil, utils.ToCmdLine(
			"FT.ADD", "wcidx", id, "FIELDS", "title", "red item",
		)); protocol.IsErrorReply(r) {
			t.Fatalf("add %s: %s", id, r.ToBytes())
		}
	}

	full := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "wcidx", "red", "NOCONTENT", "LIMIT", "0", "10"))
	if !searchTotalIs(t, full, 12) {
		t.Fatalf("default total want 12, got %s", full.ToBytes())
	}

	wc := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "wcidx", "red", "WITHOUTCOUNT", "NOCONTENT", "LIMIT", "0", "10",
	))
	if !searchTotalIs(t, wc, 10) {
		t.Fatalf("WITHOUTCOUNT LIMIT 0 10 want leading 10, got %s", wc.ToBytes())
	}

	wc3 := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "wcidx", "red", "WITHOUTCOUNT", "NOCONTENT", "LIMIT", "0", "3",
	))
	if !searchTotalIs(t, wc3, 3) {
		t.Fatalf("WITHOUTCOUNT LIMIT 0 3 want leading 3, got %s", wc3.ToBytes())
	}
}
