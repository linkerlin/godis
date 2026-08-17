package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTLanguageReturnInkeysLoadParams aligns FT.SEARCH/AGGREGATE option
// error paths with Redis 8.10 QE (LANGUAGE + RETURN/INKEYS/LOAD/PARAMS).
func TestFTLanguageReturnInkeysLoadParams(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p80", "ON", "HASH", "PREFIX", "1", "p80:",
		"SCHEMA", "t", "TEXT", "n", "NUMERIC", "SORTABLE",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "p80:1", "t", "hello", "n", "10"))

	for _, tc := range []struct {
		name string
		cmd  []string
		want string
	}{
		{"lang-miss", []string{"FT.SEARCH", "p80", "hello", "LANGUAGE"}, "SEARCH_PARSE_ARGS Bad arguments for LANGUAGE: Expected an argument, but none provided"},
		{"lang-bad", []string{"FT.SEARCH", "p80", "hello", "LANGUAGE", "nosuch"}, "SEARCH_QUERY_BAD No such language"},
		{"lang-polish", []string{"FT.SEARCH", "p80", "hello", "LANGUAGE", "polish"}, "SEARCH_QUERY_BAD No such language"},
		{"create-lang-bad", []string{"FT.CREATE", "p80bad", "ON", "HASH", "PREFIX", "1", "p80bad:", "LANGUAGE", "nosuch", "SCHEMA", "t", "TEXT"}, "Invalid language"},
		{"ret-miss", []string{"FT.SEARCH", "p80", "hello", "RETURN"}, "SEARCH_PARSE_ARGS Bad arguments for RETURN: Expected an argument, but none provided"},
		{"ret-neg", []string{"FT.SEARCH", "p80", "hello", "RETURN", "-1"}, "SEARCH_PARSE_ARGS Bad arguments for RETURN: Value is outside acceptable bounds"},
		{"ret-bad", []string{"FT.SEARCH", "p80", "hello", "RETURN", "abc"}, "SEARCH_PARSE_ARGS Bad arguments for RETURN: Could not convert argument to expected type"},
		{"ret-short", []string{"FT.SEARCH", "p80", "hello", "RETURN", "1"}, "SEARCH_PARSE_ARGS Bad arguments for RETURN: Expected an argument, but none provided"},
		{"ink-miss", []string{"FT.SEARCH", "p80", "hello", "INKEYS"}, "SEARCH_PARSE_ARGS Bad arguments for INKEYS: Expected an argument, but none provided"},
		{"ink-neg", []string{"FT.SEARCH", "p80", "hello", "INKEYS", "-1"}, "SEARCH_PARSE_ARGS Bad arguments for INKEYS: Value is outside acceptable bounds"},
		{"ink-bad", []string{"FT.SEARCH", "p80", "hello", "INKEYS", "abc"}, "SEARCH_PARSE_ARGS Bad arguments for INKEYS: Could not convert argument to expected type"},
		{"ink-short", []string{"FT.SEARCH", "p80", "hello", "INKEYS", "2", "p80:1"}, "SEARCH_PARSE_ARGS Bad arguments for INKEYS: Expected an argument, but none provided"},
		{"load-miss", []string{"FT.AGGREGATE", "p80", "*", "LOAD"}, "SEARCH_PARSE_ARGS Bad arguments for LOAD: Expected an argument, but none provided"},
		{"load-neg", []string{"FT.AGGREGATE", "p80", "*", "LOAD", "-1"}, "SEARCH_PARSE_ARGS Bad arguments for LOAD: Value is outside acceptable bounds"},
		{"load-bad", []string{"FT.AGGREGATE", "p80", "*", "LOAD", "abc"}, "SEARCH_PARSE_ARGS Bad arguments for LOAD: Expected number of fields or `*`"},
		{"load-short", []string{"FT.AGGREGATE", "p80", "*", "LOAD", "1"}, "SEARCH_PARSE_ARGS Bad arguments for LOAD: Expected an argument, but none provided"},
		{"params-miss", []string{"FT.SEARCH", "p80", "hello", "PARAMS"}, "SEARCH_PARSE_ARGS Bad arguments for PARAMS: Expected an argument, but none provided"},
		{"params-neg", []string{"FT.SEARCH", "p80", "hello", "PARAMS", "-1"}, "SEARCH_PARSE_ARGS Bad arguments for PARAMS: Value is outside acceptable bounds"},
		{"params-bad", []string{"FT.SEARCH", "p80", "hello", "PARAMS", "abc"}, "SEARCH_PARSE_ARGS Bad arguments for PARAMS: Could not convert argument to expected type"},
		{"params-zero", []string{"FT.SEARCH", "p80", "hello", "PARAMS", "0"}, "SEARCH_ADD_ARGS Parameters must be specified in PARAM VALUE pairs"},
		{"params-odd", []string{"FT.SEARCH", "p80", "hello", "PARAMS", "1", "foo"}, "SEARCH_ADD_ARGS Parameters must be specified in PARAM VALUE pairs"},
		{"hl-fields", []string{"FT.SEARCH", "p80", "hello", "HIGHLIGHT", "FIELDS"}, "SEARCH_PARSE_ARGS Bad arguments for HIGHLIGHT"},
		{"hl-tags", []string{"FT.SEARCH", "p80", "hello", "HIGHLIGHT", "TAGS", "<b>"}, "SEARCH_PARSE_ARGS Bad arguments for HIGHLIGHT"},
		{"sum-frags", []string{"FT.SEARCH", "p80", "hello", "SUMMARIZE", "FRAGS"}, "SEARCH_PARSE_ARGS Bad arguments for SUMMARIZE"},
		{"dialect-miss", []string{"FT.SEARCH", "p80", "hello", "DIALECT"}, "Need an argument for DIALECT"},
		{"dialect-0", []string{"FT.SEARCH", "p80", "hello", "DIALECT", "0"}, "DIALECT requires a non negative integer >=1 and <= 4"},
		{"dialect-5", []string{"FT.SEARCH", "p80", "hello", "DIALECT", "5"}, "DIALECT requires a non negative integer >=1 and <= 4"},
		{"sortby-miss", []string{"FT.SEARCH", "p80", "hello", "SORTBY"}, "Bad SORTBY arguments"},
		{"sortby-unknown", []string{"FT.SEARCH", "p80", "hello", "SORTBY", "nosuch"}, "Property `nosuch` not loaded nor in schema"},
		{"filter-miss", []string{"FT.SEARCH", "p80", "hello", "FILTER"}, "FILTER requires 3 arguments"},
		{"geofilter-miss", []string{"FT.SEARCH", "p80", "hello", "GEOFILTER"}, "GEOFILTER requires 5 arguments"},
		{"slop-miss", []string{"FT.SEARCH", "p80", "hello", "SLOP"}, "SEARCH_PARSE_ARGS Bad arguments for SLOP: Expected an argument, but none provided"},
		{"payload-miss", []string{"FT.SEARCH", "p80", "hello", "PAYLOAD"}, "SEARCH_PARSE_ARGS Bad arguments for PAYLOAD: Expected an argument, but none provided"},
		{"infields-miss", []string{"FT.SEARCH", "p80", "hello", "INFIELDS"}, "SEARCH_PARSE_ARGS Bad arguments for INFIELDS: Expected an argument, but none provided"},
		{"cursor-read-miss", []string{"FT.CURSOR", "READ", "p80", "999999"}, "Cursor not found, id: 999999"},
		{"cursor-del-miss", []string{"FT.CURSOR", "DEL", "p80", "999999"}, "Cursor does not exist"},
	} {
		r := db.Exec(nil, utils.ToCmdLine(tc.cmd...))
		if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), tc.want) {
			t.Fatalf("%s: want %q, got %s", tc.name, tc.want, r.ToBytes())
		}
	}

	ok := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p80", "hello", "LANGUAGE", "chinese", "NOCONTENT"))
	if protocol.IsErrorReply(ok) {
		t.Fatalf("LANGUAGE chinese: %s", ok.ToBytes())
	}
	ok2 := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p80", "hello", "LANGUAGE", "Chinese", "NOCONTENT"))
	if protocol.IsErrorReply(ok2) {
		t.Fatalf("LANGUAGE Chinese: %s", ok2.ToBytes())
	}
	okHindi := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p80", "hello", "LANGUAGE", "hindi", "NOCONTENT"))
	if protocol.IsErrorReply(okHindi) {
		t.Fatalf("LANGUAGE hindi: %s", okHindi.ToBytes())
	}
	ok3 := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p80", "hello", "RETURN", "1", "t", "NOCONTENT"))
	if protocol.IsErrorReply(ok3) {
		t.Fatalf("RETURN 1 t: %s", ok3.ToBytes())
	}
	ok4 := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p80", "hello", "INKEYS", "1", "p80:1", "NOCONTENT"))
	if protocol.IsErrorReply(ok4) {
		t.Fatalf("INKEYS: %s", ok4.ToBytes())
	}
	// Redis: INKEYS 0 → empty result set.
	ink0 := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p80", "hello", "INKEYS", "0", "NOCONTENT"))
	mr0 := ftSearchMultiRaw(ink0)
	if mr0 == nil || len(mr0.Replies) < 1 {
		t.Fatalf("INKEYS 0: %T %s", ink0, ink0.ToBytes())
	}
	if ir, ok := mr0.Replies[0].(*protocol.IntReply); !ok || ir.Code != 0 {
		t.Fatalf("INKEYS 0 want total 0, got %s", ink0.ToBytes())
	}
	ok5 := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "p80", "*", "LOAD", "1", "@t"))
	if protocol.IsErrorReply(ok5) {
		t.Fatalf("LOAD: %s", ok5.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p80ok", "ON", "HASH", "PREFIX", "1", "p80ok:",
		"LANGUAGE", "english", "SCHEMA", "t", "TEXT",
	)), "OK")
}

// TestFTCursorReadCountBadValue aligns FT.CURSOR READ COUNT with Redis 8.10
// (Bad value for COUNT; negative = drain remaining).
func TestFTCursorReadCountBadValue(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p80c", "ON", "HASH", "PREFIX", "1", "p80c:",
		"SCHEMA", "t", "TEXT",
	)), "OK")
	for _, id := range []string{"1", "2", "3"} {
		_ = db.Exec(nil, utils.ToCmdLine("HSET", "p80c:"+id, "t", "v"+id))
	}

	page := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "p80c", "*", "LOAD", "1", "@t", "WITHCURSOR", "COUNT", "1"))
	cid := ftCursorIDFromReply(t, page)

	bad := db.Exec(nil, utils.ToCmdLine("FT.CURSOR", "READ", "p80c", cid, "COUNT", "xyz"))
	if !protocol.IsErrorReply(bad) || !strings.Contains(string(bad.ToBytes()), "Bad value for COUNT: `xyz`") {
		t.Fatalf("want Bad value for COUNT, got %s", bad.ToBytes())
	}

	page2 := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "p80c", "*", "LOAD", "1", "@t", "WITHCURSOR", "COUNT", "1"))
	cid2 := ftCursorIDFromReply(t, page2)
	drain := db.Exec(nil, utils.ToCmdLine("FT.CURSOR", "READ", "p80c", cid2, "COUNT", "-1"))
	if protocol.IsErrorReply(drain) {
		t.Fatalf("COUNT -1 drain: %s", drain.ToBytes())
	}
}

func ftCursorIDFromReply(t *testing.T, r redis.Reply) string {
	t.Helper()
	if protocol.IsErrorReply(r) {
		t.Fatalf("cursor page: %s", r.ToBytes())
	}
	if mr, ok := r.(*protocol.MultiRawReply); ok && len(mr.Replies) >= 2 {
		switch last := mr.Replies[len(mr.Replies)-1].(type) {
		case *protocol.IntReply:
			return strings.TrimSpace(string(last.ToBytes()))
		case *protocol.BulkReply:
			return string(last.Arg)
		}
	}
	raw := string(r.ToBytes())
	lines := strings.Split(strings.TrimSpace(raw), "\r\n")
	if len(lines) == 0 {
		t.Fatalf("no cursor id in %s", raw)
	}
	return lines[len(lines)-1]
}
