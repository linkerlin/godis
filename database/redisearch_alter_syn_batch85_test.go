package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTAlterDropSynBatch85 aligns FT.ALTER / DROPINDEX / SYN* texts with Redis 8.10.
func TestFTAlterDropSynBatch85(t *testing.T) {
	db := makeTestDB()

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p85idx", "ON", "HASH", "SCHEMA", "t", "TEXT", "n", "NUMERIC", "g", "TAG",
	)), "OK")

	for _, tc := range []struct {
		name string
		cmd  []string
		want string
	}{
		{"alter-arity", []string{"FT.ALTER", "p85idx", "SCHEMA", "ADD"}, "ERR wrong number of arguments for 'FT.ALTER' command"},
		{"alter-no-type", []string{"FT.ALTER", "p85idx", "SCHEMA", "ADD", "extra"}, "ERR wrong number of arguments for 'FT.ALTER' command"},
		{"alter-bad-type", []string{"FT.ALTER", "p85idx", "SCHEMA", "ADD", "extra", "FOO"}, "SEARCH_PARSE_ARGS Invalid field type for field `extra`"},
		{"alter-dup", []string{"FT.ALTER", "p85idx", "SCHEMA", "ADD", "t", "TEXT"}, "SEARCH_QUERY_BAD Duplicate field in schema - t"},
		{"alter-no-schema", []string{"FT.ALTER", "p85idx", "FOO", "ADD", "extra", "TEXT"}, "ALTER must be followed by SCHEMA"},
		{"alter-bad-action", []string{"FT.ALTER", "p85idx", "SCHEMA", "FOO", "extra", "TEXT"}, "Unknown action passed to ALTER SCHEMA"},
		{"alter-miss", []string{"FT.ALTER", "nosuch", "SCHEMA", "ADD", "t", "TEXT"}, "SEARCH_INDEX_NOT_FOUND Index not found: nosuch"},
		{"drop-miss", []string{"FT.DROPINDEX", "nosuch"}, "SEARCH_INDEX_NOT_FOUND Index not found: nosuch"},
		{"drop-miss-foo", []string{"FT.DROPINDEX", "nosuch", "FOO"}, "SEARCH_INDEX_NOT_FOUND Index not found: nosuch"},
		{"drop-foo", []string{"FT.DROPINDEX", "p85idx", "FOO"}, "SEARCH_ARG_UNRECOGNIZED Unknown argument"},
		{"info-miss", []string{"FT.INFO", "nosuch"}, "SEARCH_INDEX_NOT_FOUND Index not found: nosuch"},
		{"synadd", []string{"FT.SYNADD", "p85idx", "hello", "hi"}, "No longer supported, use FT.SYNUPDATE"},
		{"synadd-bare", []string{"FT.SYNADD"}, "No longer supported, use FT.SYNUPDATE"},
		{"synupdate-miss", []string{"FT.SYNUPDATE", "nosuch", "0", "a", "b"}, "SEARCH_INDEX_NOT_FOUND Index not found: nosuch"},
		{"syndump-miss", []string{"FT.SYNDUMP", "nosuch"}, "SEARCH_INDEX_NOT_FOUND Index not found: nosuch"},
		{"alter-skip-end", []string{"FT.ALTER", "p85idx", "SCHEMA", "ADD", "extra3", "TEXT", "SKIPINITIALSCAN"}, "SEARCH_PARSE_ARGS Field `SKIPINITIALSCAN` does not have a type"},
	} {
		r := db.Exec(nil, utils.ToCmdLine(tc.cmd...))
		if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), tc.want) {
			t.Fatalf("%s: want %q, got %s", tc.name, tc.want, r.ToBytes())
		}
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.ALTER", "p85idx", "SCHEMA", "ADD", "extra", "TEXT",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.ALTER", "p85idx", "SKIPINITIALSCAN", "SCHEMA", "ADD", "extra2", "TEXT",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.SYNUPDATE", "p85idx", "0", "a", "b",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.SYNUPDATE", "p85idx", "0", "SKIPINITIALSCAN",
	)), "OK")
	dump := db.Exec(nil, utils.ToCmdLine("FT.SYNDUMP", "p85idx"))
	if protocol.IsErrorReply(dump) {
		t.Fatalf("SYNDUMP: %s", dump.ToBytes())
	}
	raw := string(dump.ToBytes())
	if !strings.Contains(raw, "a") || !strings.Contains(raw, "b") {
		t.Fatalf("SYNDUMP after skip no-op should keep terms, got %s", dump.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.DROPINDEX", "p85idx")), "OK")
	r := db.Exec(nil, utils.ToCmdLine("FT.INFO", "p85idx"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "SEARCH_INDEX_NOT_FOUND") {
		t.Fatalf("INFO after drop: got %s", r.ToBytes())
	}
}
