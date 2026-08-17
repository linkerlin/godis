package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTAggregateApplyValueNotFound aligns missing-field APPLY with Redis 8.x.
func TestFTAggregateApplyValueNotFound(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "vnf", "ON", "HASH", "PREFIX", "1", "vnf:",
		"SCHEMA", "t", "TEXT", "SORTABLE", "n", "NUMERIC", "SORTABLE",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "vnf:1", "t", "hello", "n", "3"))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "vnf:2", "n", "1"))

	bad := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "vnf", "*",
		"LOAD", "1", "@t",
		"APPLY", "upper(@t)", "AS", "u",
	))
	if !protocol.IsErrorReply(bad) ||
		!strings.Contains(string(bad.ToBytes()), "SEARCH_VALUE_NOT_FOUND") ||
		!strings.Contains(string(bad.ToBytes()), " for t") {
		t.Fatalf("upper missing field: %s", bad.ToBytes())
	}

	ok := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "vnf", "*",
		"LOAD", "1", "@t",
		"FILTER", "exists(@t)",
		"APPLY", "upper(@t)", "AS", "u",
	))
	if protocol.IsErrorReply(ok) {
		t.Fatalf("FILTER exists then upper: %s", ok.ToBytes())
	}
	if !strings.Contains(string(ok.ToBytes()), "HELLO") {
		t.Fatalf("want HELLO: %s", ok.ToBytes())
	}

	cased := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "vnf", "*",
		"LOAD", "1", "@t",
		"APPLY", `case(exists(@t), upper(@t), "missing")`, "AS", "u",
	))
	if protocol.IsErrorReply(cased) {
		t.Fatalf("case short-circuit: %s", cased.ToBytes())
	}
	body := string(cased.ToBytes())
	if !strings.Contains(body, "HELLO") || !strings.Contains(body, "missing") {
		t.Fatalf("want HELLO+missing: %s", body)
	}
}

// TestFTAggregateApplySubstrBytes aligns substr byte offsets with Redis 8.x.
func TestFTAggregateApplySubstrBytes(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "sub", "ON", "HASH", "PREFIX", "1", "sub:",
		"SCHEMA", "t", "TEXT", "SORTABLE",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "sub:1", "t", "你好abc"))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "sub", "*",
		"LOAD", "1", "@t",
		"APPLY", "substr(@t,0,3)", "AS", "s",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("substr: %s", r.ToBytes())
	}
	if !strings.Contains(string(r.ToBytes()), "你") {
		t.Fatalf("want byte substr 你: %s", r.ToBytes())
	}
}
