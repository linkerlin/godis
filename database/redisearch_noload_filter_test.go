package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTAggregateNoLoadEmptyRows aligns absent LOAD with Redis 8.x: pipeline
// rows omit non-SORTABLE document fields (empty arrays). SORTABLE fields and
// ADDSCORES @__score remain available without LOAD.
func TestFTAggregateNoLoadEmptyRows(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "noload", "ON", "HASH", "PREFIX", "1", "nl:",
		"SCHEMA", "title", "TEXT", "price", "NUMERIC", "SORTABLE",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "nl:1", "title", "hello", "price", "10"))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "nl:2", "title", "world", "price", "20"))

	empty := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "noload", "*"))
	if protocol.IsErrorReply(empty) {
		t.Fatalf("no LOAD: %s", empty.ToBytes())
	}
	body := string(empty.ToBytes())
	if strings.Contains(body, "title") || strings.Contains(body, "hello") {
		t.Fatalf("absent LOAD must drop non-SORTABLE title: %s", body)
	}
	// price is SORTABLE → still in pipeline without LOAD.
	if !strings.Contains(body, "price") || !strings.Contains(body, "10") {
		t.Fatalf("SORTABLE price should survive absent LOAD: %s", body)
	}

	load := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "noload", "*", "LOAD", "1", "@title",
	))
	if protocol.IsErrorReply(load) {
		t.Fatalf("LOAD title: %s", load.ToBytes())
	}
	loadBody := string(load.ToBytes())
	if !strings.Contains(loadBody, "title") || !strings.Contains(loadBody, "hello") {
		t.Fatalf("LOAD @title want title=hello: %s", loadBody)
	}
	if strings.Contains(loadBody, "price") {
		t.Fatalf("LOAD projection must drop price: %s", loadBody)
	}

	star := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "noload", "*", "LOAD", "*",
	))
	if protocol.IsErrorReply(star) {
		t.Fatalf("LOAD *: %s", star.ToBytes())
	}
	starBody := string(star.ToBytes())
	if !strings.Contains(starBody, "title") || !strings.Contains(starBody, "price") {
		t.Fatalf("LOAD * must keep all fields: %s", starBody)
	}

	// No SORTABLE fields → fully empty rows (Redis *0).
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "noload0", "ON", "HASH", "PREFIX", "1", "nz:",
		"SCHEMA", "t", "TEXT",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "nz:1", "t", "x"))
	blank := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "noload0", "*"))
	if protocol.IsErrorReply(blank) {
		t.Fatalf("no-SORTABLE aggregate: %s", blank.ToBytes())
	}
	blankBody := string(blank.ToBytes())
	if strings.Contains(blankBody, "t") || strings.Contains(blankBody, "$1\r\nx\r\n") {
		t.Fatalf("no-SORTABLE absent LOAD must be empty rows: %s", blankBody)
	}

	add := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "noload", "hello", "ADDSCORES", "LIMIT", "0", "1",
	))
	if protocol.IsErrorReply(add) {
		t.Fatalf("ADDSCORES no LOAD: %s", add.ToBytes())
	}
	addBody := string(add.ToBytes())
	if !strings.Contains(addBody, "__score") {
		t.Fatalf("ADDSCORES must keep __score without LOAD: %s", addBody)
	}
}

// TestFTAggregateReduceToListWire verifies REDUCE TOLIST returns a nested
// array (Redis 8.x), not a Go fmt %v string.
func TestFTAggregateReduceToListWire(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "tolist", "ON", "HASH", "PREFIX", "1", "tl:",
		"SCHEMA", "cat", "TAG", "SORTABLE", "title", "TEXT", "SORTABLE",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "tl:1", "cat", "a", "title", "hello"))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "tl:2", "cat", "a", "title", "world"))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "tolist", "*",
		"GROUPBY", "1", "@cat",
		"REDUCE", "TOLIST", "1", "@title", "AS", "titles",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("TOLIST: %s", r.ToBytes())
	}
	body := string(r.ToBytes())
	// Nested array marker after titles (not "[hello world]" bulk).
	if strings.Contains(body, "[hello") || strings.Contains(body, "[world") {
		t.Fatalf("TOLIST must not use Go %%v list string: %s", body)
	}
	if !strings.Contains(body, "titles") || !strings.Contains(body, "hello") || !strings.Contains(body, "world") {
		t.Fatalf("TOLIST want nested hello/world: %s", body)
	}
	// RESP nested array: *2\r\n$5\r\nhello or similar after titles key.
	if !strings.Contains(body, "*2\r\n") {
		t.Fatalf("TOLIST want nested multi-bulk (*2): %s", body)
	}
}

// TestFTCreateFilterExpr skips documents that fail the FT.CREATE FILTER
// expression (aggregation language; @__key + schema fields).
func TestFTCreateFilterExpr(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "filt", "ON", "HASH", "PREFIX", "1", "af:",
		"FILTER", `@country=="usa"`,
		"SCHEMA", "business", "TEXT", "country", "TAG",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "af:1", "business", "foo", "country", "usa"))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "af:2", "business", "bar", "country", "israel"))

	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "filt", "*"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("FT.SEARCH: %s", r.ToBytes())
	}
	body := string(r.ToBytes())
	if !strings.Contains(body, "af:1") || !strings.Contains(body, "foo") {
		t.Fatalf("usa doc must be indexed: %s", body)
	}
	if strings.Contains(body, "af:2") || strings.Contains(body, "israel") {
		t.Fatalf("israel doc must be filtered out: %s", body)
	}

	// Key filter via @__key.
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "filtkey", "ON", "HASH", "PREFIX", "1", "ak:",
		"FILTER", `@__key!="ak:skip"`,
		"SCHEMA", "t", "TEXT",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "ak:ok", "t", "keep"))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "ak:skip", "t", "drop"))
	kr := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "filtkey", "*"))
	if protocol.IsErrorReply(kr) {
		t.Fatalf("key FILTER search: %s", kr.ToBytes())
	}
	kb := string(kr.ToBytes())
	if !strings.Contains(kb, "ak:ok") {
		t.Fatalf("want ak:ok indexed: %s", kb)
	}
	if strings.Contains(kb, "ak:skip") {
		t.Fatalf("ak:skip must be filtered: %s", kb)
	}

	// Update that fails FILTER removes the document (reindex deletes then skips).
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "af:1", "business", "foo", "country", "canada"))
	after := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "filt", "*"))
	if strings.Contains(string(after.ToBytes()), "af:1") {
		t.Fatalf("af:1 should leave index after FILTER fails: %s", after.ToBytes())
	}
}
