package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// TestFTAggregateTimefmtPkL aligns APPLY timefmt %P/%k/%l with Redis 8.6 QE
// (lowercase am/pm; space-padded 0–23 / 1–12 hours). Unknown %Q still Null.
func TestFTAggregateTimefmtPkL(t *testing.T) {
	db := makeTestDB()
	create := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx76tf", "ON", "HASH", "PREFIX", "1", "d76tf:",
		"SCHEMA", "n", "NUMERIC", "SORTABLE",
	))
	if _, ok := create.(*protocol.OkReply); !ok {
		t.Fatalf("FT.CREATE: %s", create.ToBytes())
	}
	db.Exec(nil, utils.ToCmdLine("HSET", "d76tf:1", "n", "10"))

	noon := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "idx76tf", "*", "LOAD", "1", "@n",
		"APPLY", "timefmt(1704198896,'%P|%p|%k|%l')", "AS", "fmt", "FILTER", "@n==10",
	))
	noonBody := string(noon.ToBytes())
	if !strings.Contains(noonBody, "pm|PM|12|12") {
		t.Fatalf("noon %%P/%%p/%%k/%%l: %s", noonBody)
	}

	midnight := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "idx76tf", "*", "LOAD", "1", "@n",
		"APPLY", "timefmt(1704153600,'%k|%l|%P')", "AS", "fmt", "FILTER", "@n==10",
	))
	midBody := string(midnight.ToBytes())
	if !strings.Contains(midBody, " 0|12|am") {
		t.Fatalf("midnight %%k/%%l/%%P: %s", midBody)
	}

	unk := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "idx76tf", "*", "LOAD", "1", "@n",
		"APPLY", "timefmt(1704067200,'%Q')", "AS", "fmt", "FILTER", "@n==10",
	))
	unkBody := string(unk.ToBytes())
	if strings.Contains(unkBody, "%Q") || !strings.Contains(unkBody, "$-1") {
		t.Fatalf("unknown %%Q want $-1, got %q", unkBody)
	}
}
