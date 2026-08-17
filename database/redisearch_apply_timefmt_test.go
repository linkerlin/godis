package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// TestFTAggregateTimefmtParsetimeNullWire aligns APPLY timefmt/parsetime with
// Redis 8.10: unsupported strftime → Null ($-1); parsetime fail → $-1.
func TestFTAggregateTimefmtParsetimeNullWire(t *testing.T) {
	db := makeTestDB()
	create := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx75tf", "ON", "HASH", "PREFIX", "1", "d75tf:",
		"SCHEMA", "n", "NUMERIC", "SORTABLE",
	))
	if _, ok := create.(*protocol.OkReply); !ok {
		t.Fatalf("FT.CREATE: %s", create.ToBytes())
	}
	db.Exec(nil, utils.ToCmdLine("HSET", "d75tf:1", "n", "10"))

	okFmt := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "idx75tf", "*", "LOAD", "1", "@n",
		"APPLY", "timefmt(1704067200,'%F')", "AS", "fmt", "FILTER", "@n==10",
	))
	body := string(okFmt.ToBytes())
	if !strings.Contains(body, "2024-01-01") {
		t.Fatalf("timefmt %%F: %s", body)
	}

	unk := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "idx75tf", "*", "LOAD", "1", "@n",
		"APPLY", "timefmt(1704067200,'%Q')", "AS", "fmt", "FILTER", "@n==10",
	))
	unkBody := string(unk.ToBytes())
	if strings.Contains(unkBody, "%Q") {
		t.Fatalf("unknown directive should be Null, not literal %%Q: %s", unkBody)
	}
	if !strings.Contains(unkBody, "$-1") {
		t.Fatalf("unknown directive want null bulk $-1, got %q", unkBody)
	}

	badParse := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "idx75tf", "*", "LOAD", "1", "@n",
		"APPLY", "parsetime('nope','%Y-%m-%d')", "AS", "p", "FILTER", "@n==10",
	))
	raw := string(badParse.ToBytes())
	if !strings.Contains(raw, "$-1") {
		t.Fatalf("parsetime fail want null bulk $-1, got %q", raw)
	}
}
