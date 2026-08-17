package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTCursorCountScorerTagVals aligns WITHCURSOR COUNT / SCORER / TAGVALS
// error paths with Redis 8.x QE (no silent SCORER fallback).
func TestFTCursorCountScorerTagVals(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p79", "ON", "HASH", "PREFIX", "1", "p79:",
		"SCHEMA", "t", "TEXT", "tags", "TAG",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "p79:1", "t", "hello", "tags", "a,b"))

	for _, tc := range []struct {
		name string
		cmd  []string
		want string
	}{
		{"agg-count-miss", []string{"FT.AGGREGATE", "p79", "*", "WITHCURSOR", "COUNT"}, "Bad arguments for COUNT: Expected an argument, but none provided"},
		{"agg-count-neg", []string{"FT.AGGREGATE", "p79", "*", "WITHCURSOR", "COUNT", "-1"}, "Bad arguments for COUNT: Value is outside acceptable bounds"},
		{"agg-count-bad", []string{"FT.AGGREGATE", "p79", "*", "WITHCURSOR", "COUNT", "abc"}, "Bad arguments for COUNT: Could not convert argument to expected type"},
		{"agg-count-zero", []string{"FT.AGGREGATE", "p79", "*", "WITHCURSOR", "COUNT", "0"}, "Bad arguments for COUNT: Value is outside acceptable bounds"},
		{"search-count-miss", []string{"FT.SEARCH", "p79", "hello", "WITHCURSOR", "COUNT"}, "Bad arguments for COUNT: Expected an argument, but none provided"},
		{"search-count-neg", []string{"FT.SEARCH", "p79", "hello", "WITHCURSOR", "COUNT", "-1"}, "Bad arguments for COUNT: Value is outside acceptable bounds"},
		{"search-count-bad", []string{"FT.SEARCH", "p79", "hello", "WITHCURSOR", "COUNT", "abc"}, "Bad arguments for COUNT: Could not convert argument to expected type"},
		{"scorer-miss", []string{"FT.SEARCH", "p79", "hello", "SCORER"}, "Bad arguments for SCORER: Expected an argument, but none provided"},
		{"scorer-unknown", []string{"FT.SEARCH", "p79", "hello", "SCORER", "NOSUCH"}, "No such scorer NOSUCH"},
		{"scorer-lower", []string{"FT.SEARCH", "p79", "hello", "SCORER", "bm25std"}, "No such scorer bm25std"},
		{"tagvals-miss", []string{"FT.TAGVALS", "p79", "nosuch"}, "No such field"},
		{"tagvals-text", []string{"FT.TAGVALS", "p79", "t"}, "Not a tag field"},
	} {
		r := db.Exec(nil, utils.ToCmdLine(tc.cmd...))
		if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), tc.want) {
			t.Fatalf("%s: want %q, got %s", tc.name, tc.want, r.ToBytes())
		}
		if strings.Contains(string(r.ToBytes()), "ERR Invalid COUNT") {
			t.Fatalf("%s: legacy Invalid COUNT wording: %s", tc.name, r.ToBytes())
		}
	}

	// Known SCORER / TAGVALS still succeed.
	ok := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p79", "hello", "SCORER", "BM25STD", "NOCONTENT"))
	if protocol.IsErrorReply(ok) {
		t.Fatalf("SCORER BM25STD: %s", ok.ToBytes())
	}
	tags := db.Exec(nil, utils.ToCmdLine("FT.TAGVALS", "p79", "tags"))
	if protocol.IsErrorReply(tags) {
		t.Fatalf("TAGVALS tags: %s", tags.ToBytes())
	}
}
