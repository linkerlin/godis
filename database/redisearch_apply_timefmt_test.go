package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// TestFTAggregateTimefmtParsetimeNullWire aligns APPLY timefmt/parsetime with
// Redis 8.x: unknown strftime kept literally; parsetime fail → RESP null bulk.
func TestFTAggregateTimefmtParsetimeNullWire(t *testing.T) {
	db := makeTestDB()
	create := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx74tf", "ON", "HASH", "PREFIX", "1", "d74tf:",
		"SCHEMA", "n", "NUMERIC", "SORTABLE",
	))
	if _, ok := create.(*protocol.OkReply); !ok {
		t.Fatalf("FT.CREATE: %s", create.ToBytes())
	}
	db.Exec(nil, utils.ToCmdLine("HSET", "d74tf:1", "n", "10"))

	okFmt := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "idx74tf", "*", "LOAD", "1", "@n",
		"APPLY", "timefmt(1704067200,'%F')", "AS", "fmt", "FILTER", "@n==10",
	))
	body := string(okFmt.ToBytes())
	if !strings.Contains(body, "2024-01-01") {
		t.Fatalf("timefmt %%F: %s", body)
	}

	unk := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "idx74tf", "*", "LOAD", "1", "@n",
		"APPLY", "timefmt(1704067200,'%Q')", "AS", "fmt", "FILTER", "@n==10",
	))
	unkBody := string(unk.ToBytes())
	if !strings.Contains(unkBody, "%Q") {
		t.Fatalf("unknown directive should stay literal: %s", unkBody)
	}

	badParse := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "idx74tf", "*", "LOAD", "1", "@n",
		"APPLY", "parsetime('nope','%Y-%m-%d')", "AS", "p", "FILTER", "@n==10",
	))
	raw := string(badParse.ToBytes())
	if !strings.Contains(raw, "$-1") {
		t.Fatalf("parsetime fail want null bulk $-1, got %q", raw)
	}
}
