package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTLimitAndAggregatePropErrors aligns LIMIT / SORTBY / GROUPBY error
// paths with Redis 8.10 QE (and prevents LIMIT negative offset panic).
func TestFTLimitAndAggregatePropErrors(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p78", "ON", "HASH", "PREFIX", "1", "p78:",
		"SCHEMA", "t", "TEXT", "n", "NUMERIC", "SORTABLE",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "p78:1", "t", "hello", "n", "3.7"))

	// LIMIT: missing / non-numeric / negative → SEARCH_PARSE_ARGS (no panic).
	for _, tc := range []struct {
		name string
		cmd  []string
		want string
	}{
		{"search-one-arg", []string{"FT.SEARCH", "p78", "hello", "LIMIT", "1"}, "LIMIT requires two arguments"},
		{"search-neg", []string{"FT.SEARCH", "p78", "hello", "LIMIT", "-1", "10"}, "LIMIT needs two numeric arguments"},
		{"search-bad", []string{"FT.SEARCH", "p78", "hello", "LIMIT", "foo", "10"}, "LIMIT needs two numeric arguments"},
		{"search-neg-num", []string{"FT.SEARCH", "p78", "hello", "LIMIT", "0", "-1"}, "LIMIT needs two numeric arguments"},
		{"agg-neg", []string{"FT.AGGREGATE", "p78", "*", "LIMIT", "-1", "10"}, "LIMIT needs two numeric arguments"},
		{"agg-bad", []string{"FT.AGGREGATE", "p78", "*", "LIMIT", "0", "foo"}, "LIMIT needs two numeric arguments"},
		{"agg-one-arg", []string{"FT.AGGREGATE", "p78", "*", "LIMIT", "1"}, "LIMIT requires two arguments"},
	} {
		r := db.Exec(nil, utils.ToCmdLine(tc.cmd...))
		if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), tc.want) {
			t.Fatalf("%s: want %q, got %s", tc.name, tc.want, r.ToBytes())
		}
		if strings.Contains(string(r.ToBytes()), "ERR SEARCH_PARSE_ARGS") {
			t.Fatalf("%s: must not double ERR prefix: %s", tc.name, r.ToBytes())
		}
	}

	// SORTBY unknown → Property not loaded nor in schema
	r := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "p78", "*", "SORTBY", "1", "@nosuch"))
	if !protocol.IsErrorReply(r) ||
		!strings.Contains(string(r.ToBytes()), "Property `nosuch` not loaded nor in schema") {
		t.Fatalf("SORTBY missing: %s", r.ToBytes())
	}

	// GROUPBY unknown → No such property
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p78", "*", "GROUPBY", "1", "@nosuch", "REDUCE", "COUNT", "0", "AS", "c",
	))
	if !protocol.IsErrorReply(r) ||
		!strings.Contains(string(r.ToBytes()), "No such property `nosuch`") {
		t.Fatalf("GROUPBY missing: %s", r.ToBytes())
	}

	// Schema TEXT SORTBY / GROUPBY still OK without LOAD.
	r = db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "p78", "*", "SORTBY", "1", "@t"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("SORTBY schema text: %s", r.ToBytes())
	}
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p78", "*", "GROUPBY", "1", "@t", "REDUCE", "COUNT", "0", "AS", "c",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("GROUPBY schema text: %s", r.ToBytes())
	}

	// APPLY alias SORTBY / GROUPBY OK; REDUCE AS SORTBY OK.
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p78", "*", "LOAD", "1", "@t",
		"APPLY", "upper(@t)", "AS", "u", "SORTBY", "1", "@u",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("SORTBY apply alias: %s", r.ToBytes())
	}
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p78", "*", "LOAD", "1", "@t",
		"APPLY", "upper(@t)", "AS", "u",
		"GROUPBY", "1", "@u", "REDUCE", "COUNT", "0", "AS", "c",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("GROUPBY apply alias: %s", r.ToBytes())
	}
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p78", "*", "GROUPBY", "1", "@n",
		"REDUCE", "COUNT", "0", "AS", "c", "SORTBY", "1", "@c",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("SORTBY reduce AS: %s", r.ToBytes())
	}
}
