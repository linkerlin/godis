package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTAggregateFilterApplyInterleave aligns FILTER/APPLY command-order
// interleaving with Redis 8.6 QE (multiple FILTER; FILTER between APPLY).
func TestFTAggregateFilterApplyInterleave(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p77", "ON", "HASH", "PREFIX", "1", "p77:",
		"SCHEMA", "title", "TEXT", "SORTABLE", "n", "NUMERIC", "SORTABLE", "cat", "TAG", "SORTABLE",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "p77:a", "title", "Hello", "n", "10", "cat", "x"))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "p77:b", "title", "World", "n", "20", "cat", "y"))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "p77:c", "title", "Hello", "n", "5", "cat", "x"))

	// APPLY then FILTER on alias (@d).
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p77", "*", "LOAD", "1", "@n",
		"APPLY", "@n*2", "AS", "d",
		"FILTER", "@d>=20",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("apply-then-filter: %s", r.ToBytes())
	}
	s := string(r.ToBytes())
	if !strings.Contains(s, "20") || !strings.Contains(s, "40") {
		t.Fatalf("apply-then-filter want d=20 and d=40, got %s", s)
	}

	// FILTER → APPLY → FILTER (Redis keeps only cat=x, n=10, d=20).
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p77", "*", "LOAD", "2", "@n", "@cat",
		"FILTER", "@cat=='x'",
		"APPLY", "@n*2", "AS", "d",
		"FILTER", "@d>10",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("f-a-f: %s", r.ToBytes())
	}
	s = string(r.ToBytes())
	if strings.Contains(s, "Property not loaded") {
		t.Fatalf("f-a-f must not early-filter on @d: %s", s)
	}
	if !strings.Contains(s, "20") || strings.Contains(s, "$1\r\ny\r\n") {
		t.Fatalf("f-a-f want single cat=x n=10 d=20, got %s", s)
	}

	// APPLY → FILTER → APPLY
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p77", "*", "LOAD", "1", "@n",
		"APPLY", "@n*2", "AS", "d",
		"FILTER", "@d>=20",
		"APPLY", "@d + 1", "AS", "e",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("a-f-a: %s", r.ToBytes())
	}
	s = string(r.ToBytes())
	if !strings.Contains(s, "21") || !strings.Contains(s, "41") {
		t.Fatalf("a-f-a want e=21 and e=41, got %s", s)
	}

	// Two FILTERs (AND by succession): cat=x AND n>=10 → only a.
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p77", "*", "LOAD", "2", "@n", "@cat",
		"FILTER", "@cat=='x'",
		"FILTER", "@n>=10",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("f-f: %s", r.ToBytes())
	}
	s = string(r.ToBytes())
	if strings.Contains(s, "$1\r\ny\r\n") {
		t.Fatalf("f-f must drop cat=y, got %s", s)
	}
	if !strings.Contains(s, "10") || !strings.Contains(s, "x") {
		t.Fatalf("f-f want n=10 cat=x, got %s", s)
	}
}
