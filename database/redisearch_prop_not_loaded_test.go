package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// TestFTAggregatePropNotLoaded aligns APPLY/FILTER with Redis 8.x:
// non-SORTABLE fields require LOAD; GROUPBY/TOLIST may still read full docs.
func TestFTAggregatePropNotLoaded(t *testing.T) {
	db := makeTestDB()
	create := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "pnl", "ON", "HASH", "PREFIX", "1", "pnl:",
		"SCHEMA", "title", "TEXT", "price", "NUMERIC", "SORTABLE",
	))
	if protocol.IsErrorReply(create) {
		t.Fatalf("FT.CREATE: %s", create.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "pnl:1", "title", "hello", "price", "10"))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "pnl:2", "title", "world", "price", "20"))

	badApply := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "pnl", "*", "APPLY", "@title", "AS", "t",
	))
	if !protocol.IsErrorReply(badApply) {
		t.Fatalf("APPLY @title without LOAD want ERR, got %s", badApply.ToBytes())
	}
	if !strings.Contains(string(badApply.ToBytes()), "Property not loaded nor in pipeline") ||
		!strings.Contains(string(badApply.ToBytes()), "title") {
		t.Fatalf("want Property not loaded … title, got %s", badApply.ToBytes())
	}

	badFilter := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "pnl", "*", "FILTER", "@title=='hello'",
	))
	if !protocol.IsErrorReply(badFilter) ||
		!strings.Contains(string(badFilter.ToBytes()), "Property not loaded nor in pipeline") {
		t.Fatalf("FILTER @title without LOAD want not-loaded ERR, got %s", badFilter.ToBytes())
	}

	okSortable := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "pnl", "*", "APPLY", "@price*2", "AS", "d",
	))
	if protocol.IsErrorReply(okSortable) {
		t.Fatalf("APPLY SORTABLE price without LOAD: %s", okSortable.ToBytes())
	}
	okBody := string(okSortable.ToBytes())
	if !strings.Contains(okBody, "d") || !strings.Contains(okBody, "20") {
		t.Fatalf("want d=20 from price*2, got %s", okBody)
	}

	okLoad := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "pnl", "*",
		"LOAD", "1", "@title",
		"APPLY", "upper(@title)", "AS", "t",
	))
	if protocol.IsErrorReply(okLoad) {
		t.Fatalf("APPLY after LOAD: %s", okLoad.ToBytes())
	}
	loadBody := string(okLoad.ToBytes())
	if !strings.Contains(loadBody, "HELLO") && !strings.Contains(loadBody, "WORLD") {
		t.Fatalf("want upper(title), got %s", loadBody)
	}

	// LOAD AS: alias is in pipeline; original name is not.
	asOnly := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "pnl", "*",
		"LOAD", "3", "@title", "AS", "t",
		"APPLY", "upper(@title)", "AS", "u",
	))
	if !protocol.IsErrorReply(asOnly) ||
		!strings.Contains(string(asOnly.ToBytes()), "title") {
		t.Fatalf("APPLY @title after LOAD AS t want not-loaded, got %s", asOnly.ToBytes())
	}
	asOK := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "pnl", "*",
		"LOAD", "3", "@title", "AS", "t",
		"APPLY", "upper(@t)", "AS", "u",
	))
	if protocol.IsErrorReply(asOK) {
		t.Fatalf("APPLY @t after LOAD AS: %s", asOK.ToBytes())
	}

	// GROUPBY/TOLIST on non-SORTABLE without LOAD still works (document path).
	gb := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "pnl", "*",
		"GROUPBY", "1", "@title",
		"REDUCE", "COUNT", "0", "AS", "c",
	))
	if protocol.IsErrorReply(gb) {
		t.Fatalf("GROUPBY @title no LOAD: %s", gb.ToBytes())
	}
	gbBody := string(gb.ToBytes())
	if !strings.Contains(gbBody, "hello") || !strings.Contains(gbBody, "world") {
		t.Fatalf("GROUPBY title want hello/world, got %s", gbBody)
	}

	tl := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "pnl", "*",
		"GROUPBY", "0",
		"REDUCE", "TOLIST", "1", "@title", "AS", "t",
	))
	if protocol.IsErrorReply(tl) {
		t.Fatalf("TOLIST @title no LOAD: %s", tl.ToBytes())
	}
	tlBody := string(tl.ToBytes())
	if !strings.Contains(tlBody, "hello") || !strings.Contains(tlBody, "world") {
		t.Fatalf("TOLIST title want hello/world, got %s", tlBody)
	}

	// After GROUPBY, FILTER may use the group key without LOAD.
	gbFilter := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "pnl", "*",
		"GROUPBY", "1", "@title",
		"REDUCE", "COUNT", "0", "AS", "c",
		"FILTER", "@title=='hello'",
	))
	if protocol.IsErrorReply(gbFilter) {
		t.Fatalf("FILTER after GROUPBY: %s", gbFilter.ToBytes())
	}
	gbFilterBody := string(gbFilter.ToBytes())
	if !strings.Contains(gbFilterBody, "hello") {
		t.Fatalf("FILTER after GROUPBY want hello, got %s", gbFilterBody)
	}
	if strings.Contains(gbFilterBody, "world") {
		t.Fatalf("FILTER after GROUPBY should drop world: %s", gbFilterBody)
	}

	// __key requires explicit LOAD even with LOAD *.
	starKey := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "pnl", "*",
		"LOAD", "*",
		"APPLY", "@__key", "AS", "k",
	))
	if !protocol.IsErrorReply(starKey) ||
		!strings.Contains(string(starKey.ToBytes()), "__key") {
		t.Fatalf("APPLY @__key with LOAD * want not-loaded, got %s", starKey.ToBytes())
	}
}
