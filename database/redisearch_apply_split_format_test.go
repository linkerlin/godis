package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTAggregateApplySplitStrip aligns APPLY split charset sep/strip with Redis 8.x.
func TestFTAggregateApplySplitStrip(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "spl", "ON", "HASH", "PREFIX", "1", "spl:",
		"SCHEMA", "t", "TEXT", "SORTABLE",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "spl:1", "t", "a::b;c"))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "spl", "*",
		"LOAD", "1", "@t",
		"APPLY", `split(@t, ":;")`, "AS", "parts",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("split charset: %s", r.ToBytes())
	}
	body := string(r.ToBytes())
	if !strings.Contains(body, "a") || !strings.Contains(body, "b") || !strings.Contains(body, "c") {
		t.Fatalf("want split parts a/b/c: %s", body)
	}

	strip := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "spl", "*",
		"LOAD", "1", "@t",
		"APPLY", `split("xa,xb", ",", "x")`, "AS", "p2",
	))
	if protocol.IsErrorReply(strip) {
		t.Fatalf("split strip: %s", strip.ToBytes())
	}
	if !strings.Contains(string(strip.ToBytes()), "a") || !strings.Contains(string(strip.ToBytes()), "b") {
		t.Fatalf("want stripped a/b: %s", strip.ToBytes())
	}
}

// TestFTAggregateApplyFormatParseArgs aligns format() PARSE_ARGS errors with Redis 8.x.
func TestFTAggregateApplyFormatParseArgs(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "fmt", "ON", "HASH", "PREFIX", "1", "fmt:",
		"SCHEMA", "n", "NUMERIC", "SORTABLE",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "fmt:1", "n", "1"))

	ok := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "fmt", "*",
		"APPLY", `format("v=%s", @n)`, "AS", "s",
	))
	if protocol.IsErrorReply(ok) {
		t.Fatalf("format ok: %s", ok.ToBytes())
	}
	if !strings.Contains(string(ok.ToBytes()), "v=1") {
		t.Fatalf("want v=1: %s", ok.ToBytes())
	}

	bad := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "fmt", "*",
		"APPLY", `format("bad %")`, "AS", "x",
	))
	if !protocol.IsErrorReply(bad) ||
		!strings.Contains(string(bad.ToBytes()), "SEARCH_PARSE_ARGS Bad format string!") {
		t.Fatalf("trailing %%: %s", bad.ToBytes())
	}

	miss := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "fmt", "*",
		"APPLY", `format("%s")`, "AS", "x",
	))
	if !protocol.IsErrorReply(miss) ||
		!strings.Contains(string(miss.ToBytes()), "SEARCH_PARSE_ARGS Not enough arguments for format") {
		t.Fatalf("missing arg: %s", miss.ToBytes())
	}

	unk := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "fmt", "*",
		"APPLY", `format("%d", 1)`, "AS", "x",
	))
	if !protocol.IsErrorReply(unk) ||
		!strings.Contains(string(unk.ToBytes()), "SEARCH_PARSE_ARGS Unknown format specifier passed") {
		t.Fatalf("bad specifier: %s", unk.ToBytes())
	}
}
