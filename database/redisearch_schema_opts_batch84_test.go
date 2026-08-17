package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTSchemaOptsBatch84 aligns FT.CREATE LANGUAGE_FIELD/SCORE_FIELD/PAYLOAD_FIELD
// missing-arg texts and SCHEMA field/VECTOR parse errors with Redis 8.10 (ylf-e2e).
func TestFTSchemaOptsBatch84(t *testing.T) {
	db := makeTestDB()

	for _, tc := range []struct {
		name string
		cmd  []string
		want string
	}{
		{"lf-miss", []string{"FT.CREATE", "p84a", "ON", "HASH", "LANGUAGE_FIELD"}, "SEARCH_PARSE_ARGS Bad arguments for LANGUAGE_FIELD: Expected an argument, but none provided"},
		{"sf-miss", []string{"FT.CREATE", "p84b", "ON", "HASH", "SCORE_FIELD"}, "SEARCH_PARSE_ARGS Bad arguments for SCORE_FIELD: Expected an argument, but none provided"},
		{"pf-miss", []string{"FT.CREATE", "p84c", "ON", "HASH", "PAYLOAD_FIELD"}, "SEARCH_PARSE_ARGS Bad arguments for PAYLOAD_FIELD: Expected an argument, but none provided"},
		{"schema-empty", []string{"FT.CREATE", "p84d", "ON", "HASH", "SCHEMA"}, "SEARCH_PARSE_ARGS Fields arguments are missing"},
		{"no-type", []string{"FT.CREATE", "p84e", "ON", "HASH", "SCHEMA", "t"}, "SEARCH_PARSE_ARGS Field `t` does not have a type"},
		{"bad-type", []string{"FT.CREATE", "p84f", "ON", "HASH", "SCHEMA", "t", "FOO"}, "SEARCH_PARSE_ARGS Invalid field type for field `t`"},
		{"as-miss", []string{"FT.CREATE", "p84as", "ON", "HASH", "SCHEMA", "t", "AS"}, "SEARCH_PARSE_ARGS AS requires an argument"},
		{"as-alias-as-type", []string{"FT.CREATE", "p84as2", "ON", "HASH", "SCHEMA", "t", "AS", "TEXT"}, "SEARCH_PARSE_ARGS Field `TEXT` does not have a type"},
		{"weight-miss", []string{"FT.CREATE", "p84g", "ON", "HASH", "SCHEMA", "t", "TEXT", "WEIGHT"}, "SEARCH_PARSE_ARGS Bad arguments for weight: Could not convert argument to expected type"},
		{"weight-abc", []string{"FT.CREATE", "p84h", "ON", "HASH", "SCHEMA", "t", "TEXT", "WEIGHT", "abc"}, "SEARCH_PARSE_ARGS Bad arguments for weight: Could not convert argument to expected type"},
		{"weight-nan", []string{"FT.CREATE", "p84nan", "ON", "HASH", "SCHEMA", "t", "TEXT", "WEIGHT", "nan"}, "SEARCH_PARSE_ARGS Bad arguments for weight: Could not convert argument to expected type"},
		{"sep-miss", []string{"FT.CREATE", "p84j", "ON", "HASH", "SCHEMA", "t", "TAG", "SEPARATOR"}, "SEARCH_PARSE_ARGS SEPARATOR requires an argument"},
		{"sep-multi", []string{"FT.CREATE", "p84jm", "ON", "HASH", "SCHEMA", "t", "TAG", "SEPARATOR", "ab"}, "SEARCH_PARSE_ARGS Tag separator must be a single character. Got `%s`ab"},
		{"phon-miss", []string{"FT.CREATE", "p84k", "ON", "HASH", "SCHEMA", "t", "TEXT", "PHONETIC"}, "SEARCH_PARSE_ARGS PHONETIC requires an argument"},
		{"phon-bad", []string{"FT.CREATE", "p84l", "ON", "HASH", "SCHEMA", "t", "TEXT", "PHONETIC", "dm:xx"}, "SEARCH_QUERY_BAD Matcher Format: <2 chars algorithm>:<2 chars language>"},
		{"phon-case", []string{"FT.CREATE", "p84lc", "ON", "HASH", "SCHEMA", "t", "TEXT", "PHONETIC", "DM:EN"}, "SEARCH_QUERY_BAD Matcher Format: <2 chars algorithm>:<2 chars language>"},
		{"vec-algo", []string{"FT.CREATE", "p84m", "ON", "HASH", "SCHEMA", "t", "VECTOR"}, "SEARCH_PARSE_ARGS Bad arguments for vector similarity algorithm: Expected an argument, but none provided"},
		{"vec-count", []string{"FT.CREATE", "p84n", "ON", "HASH", "SCHEMA", "t", "VECTOR", "FLAT"}, "SEARCH_PARSE_ARGS Bad arguments for vector similarity number of parameters: Expected an argument, but none provided"},
		{"vec-bad-algo", []string{"FT.CREATE", "p84o", "ON", "HASH", "SCHEMA", "t", "VECTOR", "FOO", "6"}, "SEARCH_PARSE_ARGS Bad arguments for vector similarity algorithm: Unknown argument"},
		{"vec-count-abc", []string{"FT.CREATE", "p84p", "ON", "HASH", "SCHEMA", "t", "VECTOR", "FLAT", "abc"}, "SEARCH_PARSE_ARGS Bad arguments for vector similarity number of parameters: Could not convert argument to expected type"},
		{"vec-count-neg", []string{"FT.CREATE", "p84pn", "ON", "HASH", "SCHEMA", "t", "VECTOR", "FLAT", "-2"}, "SEARCH_PARSE_ARGS Bad arguments for vector similarity number of parameters: Value is outside acceptable bounds"},
		{"vec-odd", []string{"FT.CREATE", "p84q", "ON", "HASH", "SCHEMA", "t", "VECTOR", "FLAT", "5"}, "SEARCH_PARSE_ARGS Bad number of arguments for vector similarity index: got 5 but expected even number"},
	} {
		r := db.Exec(nil, utils.ToCmdLine(tc.cmd...))
		if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), tc.want) {
			t.Fatalf("%s: want %q, got %s", tc.name, tc.want, r.ToBytes())
		}
	}

	// Accept LANGUAGE_FIELD/SCORE_FIELD/PAYLOAD_FIELD + WEIGHT 0/-1/inf + PHONETIC dm:en.
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p84ok1", "ON", "HASH", "LANGUAGE_FIELD", "mylang",
		"SCORE_FIELD", "myscore", "PAYLOAD_FIELD", "mypay", "SCHEMA", "t", "TEXT",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p84okw0", "ON", "HASH", "SCHEMA", "t", "TEXT", "WEIGHT", "0",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p84okwn", "ON", "HASH", "SCHEMA", "t", "TEXT", "WEIGHT", "-1",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p84okwi", "ON", "HASH", "SCHEMA", "t", "TEXT", "WEIGHT", "inf",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p84oksep", "ON", "HASH", "SCHEMA", "t", "TAG", "SEPARATOR", ",",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p84okph", "ON", "HASH", "SCHEMA", "t", "TEXT", "PHONETIC", "dm:en",
	)), "OK")

	// No SCHEMA token still uses SEARCH_PARSE_ARGS No schema found (batch 83).
	r := db.Exec(nil, utils.ToCmdLine("FT.CREATE", "p84nosch", "ON", "HASH", "PREFIX", "1", "nosch:"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "SEARCH_PARSE_ARGS No schema found") {
		t.Fatalf("no-schema: got %s", r.ToBytes())
	}
}
