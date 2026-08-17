package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/datastruct/redisearch"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 86: FT.CONFIG / FT._LIST / FT.DICT* / alias ERR texts (Redis 8.10-style).
func TestFTConfigValidationBatch86(t *testing.T) {
	db := makeTestDB()

	cases := []struct {
		name string
		cmd  []string
		want string
	}{
		{"unknown-opt", []string{"FT.CONFIG", "SET", "NOPE", "1"}, "SEARCH_OPTION_INVALID Invalid option"},
		{"timeout-bounds", []string{"FT.CONFIG", "SET", "TIMEOUT", "-1"}, "SEARCH_PARSE_ARGS Value is outside acceptable bounds"},
		{"timeout-bad", []string{"FT.CONFIG", "SET", "TIMEOUT", "x"}, "SEARCH_PARSE_ARGS Could not convert argument to expected type"},
		{"dialect-high", []string{"FT.CONFIG", "SET", "DEFAULT_DIALECT", "5"}, "SEARCH_VALUE_BAD Default dialect version cannot be higher than 4"},
		{"on-timeout-bad", []string{"FT.CONFIG", "SET", "ON_TIMEOUT", "BOGUS"}, "SEARCH_VALUE_BAD Invalid ON_TIMEOUT value"},
		{"set-missing-val", []string{"FT.CONFIG", "SET", "TIMEOUT"}, "SEARCH_PARSE_ARGS Expected an argument, but none provided"},
		{"excessargs", []string{"FT.CONFIG", "SET", "TIMEOUT", "0", "extra"}, "EXCESSARGS"},
		{"help-arity", []string{"FT.CONFIG", "HELP"}, "ERR wrong number of arguments for 'FT.CONFIG|HELP' command"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := db.Exec(nil, utils.ToCmdLine(tc.cmd...))
			if !protocol.IsErrorReply(r) {
				t.Fatalf("want error: %s", r.ToBytes())
			}
			if !strings.Contains(string(r.ToBytes()), tc.want) {
				t.Fatalf("got %q want substring %q", r.ToBytes(), tc.want)
			}
		})
	}

	asserts.AssertMultiBulkReplySize(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "HELP", "GET")), 0)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "ON_TIMEOUT", "RETURN-STRICT")), "OK")
	r := ftTimeoutReply(redisearch.ErrTimeout)
	if protocol.IsErrorReply(r) {
		t.Fatalf("RETURN-STRICT should empty search: %s", r.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "ON_TIMEOUT", "FAIL"))

	// GET accepts trailing args (ignored).
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MINPREFIX", "3")), "OK")
	got := db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "GET", "MINPREFIX", "ignored"))
	asserts.AssertMultiBulkReply(t, got, []string{"MINPREFIX", "3"})
	_ = db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MINPREFIX", "2"))
}

func TestFTListSortedBatch86(t *testing.T) {
	db := makeTestDB()
	for _, idx := range []string{"b86z", "b86a", "b86m"} {
		asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
			"FT.CREATE", idx, "SCHEMA", "t", "TEXT",
		)), "OK")
	}
	list := db.Exec(nil, utils.ToCmdLine("FT._LIST"))
	multi, ok := list.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("FT._LIST type %T", list)
	}
	if len(multi.Args) != 3 {
		t.Fatalf("want 3 indexes, got %d", len(multi.Args))
	}
	names := []string{string(multi.Args[0]), string(multi.Args[1]), string(multi.Args[2])}
	if names[0] != "b86a" || names[1] != "b86m" || names[2] != "b86z" {
		t.Fatalf("want sorted b86a,b86m,b86z got %v", names)
	}
	bad := db.Exec(nil, utils.ToCmdLine("FT._LIST", "x"))
	asserts.AssertErrReply(t, bad, "ERR wrong number of arguments for 'FT._LIST' command")
}

func TestFTAliasAndDictArityBatch86(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "b86idx", "SCHEMA", "t", "TEXT",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.ALIASADD", "b86alias", "b86idx")), "OK")
	dup := db.Exec(nil, utils.ToCmdLine("FT.ALIASADD", "b86alias", "b86idx"))
	if !protocol.IsErrorReply(dup) || !strings.Contains(string(dup.ToBytes()), "SEARCH_INDEX_EXISTS Alias already exists") {
		t.Fatalf("duplicate alias: %s", dup.ToBytes())
	}

	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("FT.DICTADD", "d")),
		"ERR wrong number of arguments for 'FT.DICTADD' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("FT.DICTDEL", "d")),
		"ERR wrong number of arguments for 'FT.DICTDEL' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("FT.DICTDUMP")),
		"ERR wrong number of arguments for 'FT.DICTDUMP' command")
}
