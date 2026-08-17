package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTCreateOptsBatch83 aligns FT.CREATE PREFIX/ON/LANGUAGE/SCORE/INDEXALL/FILTER
// and TIMEOUT SEARCH_PARSE_ARGS prefixes with Redis 8.10 (ylf-e2e-redis).
func TestFTCreateOptsBatch83(t *testing.T) {
	db := makeTestDB()

	for _, tc := range []struct {
		name string
		cmd  []string
		want string
	}{
		{"prefix-miss", []string{"FT.CREATE", "p83a", "ON", "HASH", "PREFIX"}, "SEARCH_PARSE_ARGS Bad arguments for PREFIX: Expected an argument, but none provided"},
		{"prefix-schema", []string{"FT.CREATE", "p83b", "ON", "HASH", "PREFIX", "SCHEMA", "t", "TEXT"}, "SEARCH_PARSE_ARGS Bad arguments for PREFIX: Could not convert argument to expected type"},
		{"prefix-neg", []string{"FT.CREATE", "p83c", "ON", "HASH", "PREFIX", "-1", "SCHEMA", "t", "TEXT"}, "SEARCH_PARSE_ARGS Bad arguments for PREFIX: Value is outside acceptable bounds"},
		{"on-list", []string{"FT.CREATE", "p83d", "ON", "LIST", "SCHEMA", "t", "TEXT"}, "SEARCH_ADD_ARGS Invalid rule type: LIST"},
		{"on-string", []string{"FT.CREATE", "p83e", "ON", "STRING", "SCHEMA", "t", "TEXT"}, "SEARCH_ADD_ARGS Invalid rule type: STRING"},
		{"lang-end", []string{"FT.CREATE", "p83f", "ON", "HASH", "PREFIX", "1", "f:", "LANGUAGE"}, "SEARCH_PARSE_ARGS Bad arguments for LANGUAGE: Expected an argument, but none provided"},
		{"lang-bad", []string{"FT.CREATE", "p83g", "ON", "HASH", "PREFIX", "1", "g:", "LANGUAGE", "klingon", "SCHEMA", "t", "TEXT"}, "SEARCH_ADD_ARGS Invalid language"},
		{"score-end", []string{"FT.CREATE", "p83h", "ON", "HASH", "PREFIX", "1", "h:", "SCORE"}, "SEARCH_PARSE_ARGS Bad arguments for SCORE: Expected an argument, but none provided"},
		{"score-high", []string{"FT.CREATE", "p83i", "ON", "HASH", "PREFIX", "1", "i:", "SCORE", "1.5", "SCHEMA", "t", "TEXT"}, "SEARCH_ADD_ARGS Invalid score"},
		{"score-neg", []string{"FT.CREATE", "p83j", "ON", "HASH", "PREFIX", "1", "j:", "SCORE", "-0.1", "SCHEMA", "t", "TEXT"}, "SEARCH_ADD_ARGS Invalid score"},
		{"score-abc", []string{"FT.CREATE", "p83k", "ON", "HASH", "PREFIX", "1", "k:", "SCORE", "abc", "SCHEMA", "t", "TEXT"}, "SEARCH_ADD_ARGS Invalid score"},
		{"score-inf", []string{"FT.CREATE", "p83l", "ON", "HASH", "PREFIX", "1", "l:", "SCORE", "inf", "SCHEMA", "t", "TEXT"}, "SEARCH_ADD_ARGS Invalid score"},
		{"indexall-end", []string{"FT.CREATE", "p83m", "ON", "HASH", "PREFIX", "1", "m:", "INDEXALL"}, "SEARCH_PARSE_ARGS Bad arguments for INDEXALL: Expected an argument, but none provided"},
		{"indexall-foo", []string{"FT.CREATE", "p83n", "ON", "HASH", "PREFIX", "1", "n:", "INDEXALL", "FOO", "SCHEMA", "t", "TEXT"}, "SEARCH_ADD_ARGS Invalid argument for `INDEXALL`, use ENABLE/DISABLE"},
		{"filter-end", []string{"FT.CREATE", "p83o", "ON", "HASH", "PREFIX", "1", "o:", "FILTER"}, "SEARCH_PARSE_ARGS Bad arguments for FILTER: Expected an argument, but none provided"},
		{"no-schema", []string{"FT.CREATE", "p83p", "ON", "HASH", "PREFIX", "1", "p:"}, "SEARCH_PARSE_ARGS No schema found"},
	} {
		r := db.Exec(nil, utils.ToCmdLine(tc.cmd...))
		if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), tc.want) {
			t.Fatalf("%s: want %q, got %s", tc.name, tc.want, r.ToBytes())
		}
	}

	// Accept SCORE 0/1/NaN and LANGUAGE english; PREFIX 0.
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p83ok0", "ON", "HASH", "PREFIX", "1", "ok0:", "SCORE", "0", "SCHEMA", "t", "TEXT",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p83ok1", "ON", "HASH", "PREFIX", "1", "ok1:", "SCORE", "1", "SCHEMA", "t", "TEXT",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p83okn", "ON", "HASH", "PREFIX", "1", "okn:", "SCORE", "nan", "SCHEMA", "t", "TEXT",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p83okl", "ON", "HASH", "PREFIX", "1", "okl:", "LANGUAGE", "english", "SCHEMA", "t", "TEXT",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p83pfx0", "ON", "HASH", "PREFIX", "0", "SCHEMA", "t", "TEXT",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p83ia", "ON", "HASH", "PREFIX", "1", "ia:", "INDEXALL", "DISABLE", "SCHEMA", "t", "TEXT",
	)), "OK")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p83to", "ON", "HASH", "PREFIX", "1", "to:", "SCHEMA", "t", "TEXT",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "to:1", "t", "hello"))

	for _, tc := range []struct {
		name string
		cmd  []string
		want string
	}{
		{"to-miss", []string{"FT.SEARCH", "p83to", "hello", "TIMEOUT"}, "SEARCH_PARSE_ARGS Need argument for TIMEOUT"},
		{"to-abc", []string{"FT.SEARCH", "p83to", "hello", "TIMEOUT", "abc"}, "SEARCH_PARSE_ARGS TIMEOUT requires a non negative integer"},
		{"to-neg", []string{"FT.SEARCH", "p83to", "hello", "TIMEOUT", "-1"}, "SEARCH_PARSE_ARGS TIMEOUT requires a non negative integer"},
		{"agg-to-miss", []string{"FT.AGGREGATE", "p83to", "*", "TIMEOUT"}, "SEARCH_PARSE_ARGS Need argument for TIMEOUT"},
		{"agg-to-abc", []string{"FT.AGGREGATE", "p83to", "*", "TIMEOUT", "abc"}, "SEARCH_PARSE_ARGS TIMEOUT requires a non negative integer"},
	} {
		r := db.Exec(nil, utils.ToCmdLine(tc.cmd...))
		if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), tc.want) {
			t.Fatalf("%s: want %q, got %s", tc.name, tc.want, r.ToBytes())
		}
	}
}
