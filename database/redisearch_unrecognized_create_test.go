package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTUnrecognizedAndCreateOpts aligns FT.SEARCH/AGGREGATE unknown-option
// SEARCH_ARG_UNRECOGNIZED positions and FT.CREATE STOPWORDS/TEMPORARY parse
// errors with Redis 8.x (ylf-e2e-redis 8.10).
func TestFTUnrecognizedAndCreateOpts(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p82", "ON", "HASH", "PREFIX", "1", "p82:",
		"SCHEMA", "t", "TEXT", "n", "NUMERIC", "SORTABLE",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "p82:1", "t", "hello", "n", "5"))

	for _, tc := range []struct {
		name string
		cmd  []string
		want string
	}{
		{"search-foo", []string{"FT.SEARCH", "p82", "hello", "FOO"}, "SEARCH_ARG_UNRECOGNIZED Unknown argument `FOO` at position 1 for <main>"},
		{"search-nocontent-foo", []string{"FT.SEARCH", "p82", "hello", "NOCONTENT", "FOO"}, "SEARCH_ARG_UNRECOGNIZED Unknown argument `FOO` at position 2 for <main>"},
		{"search-limit-foo", []string{"FT.SEARCH", "p82", "hello", "LIMIT", "0", "10", "FOO"}, "SEARCH_ARG_UNRECOGNIZED Unknown argument `FOO` at position 4 for <main>"},
		{"search-dialect-foo", []string{"FT.SEARCH", "p82", "hello", "DIALECT", "2", "FOO"}, "SEARCH_ARG_UNRECOGNIZED Unknown argument `FOO` at position 3 for <main>"},
		{"agg-foo", []string{"FT.AGGREGATE", "p82", "*", "FOO"}, "SEARCH_ARG_UNRECOGNIZED Unknown argument `FOO` at position 1 for <main>"},
		{"agg-load-foo", []string{"FT.AGGREGATE", "p82", "*", "LOAD", "1", "@t", "FOO"}, "SEARCH_ARG_UNRECOGNIZED Unknown argument `FOO` at position 4 for <main>"},
		{"agg-reduce-bare", []string{"FT.AGGREGATE", "p82", "*", "REDUCE", "COUNT", "0"}, "SEARCH_ARG_UNRECOGNIZED Unknown argument `REDUCE` at position 1 for <main>"},
		{"stop-miss", []string{"FT.CREATE", "p82sw0", "ON", "HASH", "PREFIX", "1", "sw0:", "STOPWORDS"}, "SEARCH_PARSE_ARGS Bad arguments for STOPWORDS: Expected an argument, but none provided"},
		{"stop-schema", []string{"FT.CREATE", "p82sw1", "ON", "HASH", "PREFIX", "1", "sw1:", "STOPWORDS", "SCHEMA", "t", "TEXT"}, "SEARCH_PARSE_ARGS Bad arguments for STOPWORDS: Could not convert argument to expected type"},
		{"stop-neg", []string{"FT.CREATE", "p82sw2", "ON", "HASH", "PREFIX", "1", "sw2:", "STOPWORDS", "-1", "SCHEMA", "t", "TEXT"}, "SEARCH_PARSE_ARGS Bad arguments for STOPWORDS: Value is outside acceptable bounds"},
		{"temp-miss", []string{"FT.CREATE", "p82t0", "ON", "HASH", "PREFIX", "1", "t0:", "TEMPORARY"}, "SEARCH_PARSE_ARGS Bad arguments for TEMPORARY: Expected an argument, but none provided"},
		{"temp-schema", []string{"FT.CREATE", "p82t1", "ON", "HASH", "PREFIX", "1", "t1:", "TEMPORARY", "SCHEMA", "t", "TEXT"}, "SEARCH_PARSE_ARGS Bad arguments for TEMPORARY: Could not convert argument to expected type"},
		{"temp-abc", []string{"FT.CREATE", "p82t2", "ON", "HASH", "PREFIX", "1", "t2:", "TEMPORARY", "abc", "SCHEMA", "t", "TEXT"}, "SEARCH_PARSE_ARGS Bad arguments for TEMPORARY: Could not convert argument to expected type"},
	} {
		r := db.Exec(nil, utils.ToCmdLine(tc.cmd...))
		if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), tc.want) {
			t.Fatalf("%s: want %q, got %s", tc.name, tc.want, r.ToBytes())
		}
	}

	// Bare WITHCOUNT is accepted (accurate total is already default).
	okWC := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p82", "hello", "WITHCOUNT", "NOCONTENT"))
	if protocol.IsErrorReply(okWC) {
		t.Fatalf("WITHCOUNT: %s", okWC.ToBytes())
	}

	// STOPWORDS 0 / TEMPORARY -1 accepted; TEMPORARY 0 → OK but index absent.
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p82sw3", "ON", "HASH", "PREFIX", "1", "sw3:", "STOPWORDS", "0", "SCHEMA", "t", "TEXT",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p82tn", "ON", "HASH", "PREFIX", "1", "tn:", "TEMPORARY", "-1", "SCHEMA", "t", "TEXT",
	)), "OK")
	infoNeg := db.Exec(nil, utils.ToCmdLine("FT.INFO", "p82tn"))
	if protocol.IsErrorReply(infoNeg) {
		t.Fatalf("TEMPORARY -1 INFO: %s", infoNeg.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p82t0ok", "ON", "HASH", "PREFIX", "1", "t0ok:", "TEMPORARY", "0", "SCHEMA", "t", "TEXT",
	)), "OK")
	info0 := db.Exec(nil, utils.ToCmdLine("FT.INFO", "p82t0ok"))
	if !protocol.IsErrorReply(info0) || !strings.Contains(string(info0.ToBytes()), "Index not found") {
		t.Fatalf("TEMPORARY 0 INFO want not found, got %s", info0.ToBytes())
	}
}
