package database

import (
	"strings"
	"testing"

	redis "github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// TestFTAggregateSortByMax keeps top-n after SORTBY; Total stays pre-MAX (Redis 8.x).
func TestFTAggregateSortByMax(t *testing.T) {
	db := makeTestDB()
	create := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "sbmax", "ON", "HASH", "PREFIX", "1", "sbm:",
		"SCHEMA", "n", "NUMERIC", "SORTABLE",
	))
	if protocol.IsErrorReply(create) {
		t.Fatalf("FT.CREATE: %s", create.ToBytes())
	}
	for _, kv := range []struct{ k, n string }{
		{"sbm:1", "1"}, {"sbm:2", "2"}, {"sbm:3", "3"}, {"sbm:4", "4"},
	} {
		if r := db.Exec(nil, utils.ToCmdLine("HSET", kv.k, "n", kv.n)); protocol.IsErrorReply(r) {
			t.Fatalf("HSET %s: %s", kv.k, r.ToBytes())
		}
	}

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "sbmax", "*",
		"LOAD", "1", "@n",
		"SORTBY", "2", "@n", "DESC", "MAX", "2",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("SORTBY MAX: %s", r.ToBytes())
	}
	mr, ok := unwrapFTAggregate(r)
	if !ok {
		t.Fatalf("want multi-bulk aggregate, got %T %s", r, r.ToBytes())
	}
	if len(mr.Args) < 1 || string(mr.Args[0]) != "4" {
		t.Fatalf("want Total=4, got %v", mr.Args)
	}
	if len(mr.Args) != 3 { // total + 2 rows
		t.Fatalf("want 2 rows after MAX, got %d elems: %s", len(mr.Args)-1, r.ToBytes())
	}

	// MAX 0 = no truncate (Redis accepts; returns all).
	all := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "sbmax", "*",
		"LOAD", "1", "@n",
		"SORTBY", "2", "@n", "ASC", "MAX", "0",
	))
	am, ok := unwrapFTAggregate(all)
	if !ok {
		t.Fatalf("MAX 0: %T %s", all, all.ToBytes())
	}
	if len(am.Args) != 5 {
		t.Fatalf("MAX 0 want 4 rows, got %d: %s", len(am.Args)-1, all.ToBytes())
	}

	bad := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "sbmax", "*",
		"LOAD", "1", "@n",
		"SORTBY", "2", "@n", "DESC", "MAX", "ab",
	))
	if !protocol.IsErrorReply(bad) || !strings.Contains(string(bad.ToBytes()), "Bad arguments for MAX") {
		t.Fatalf("bad MAX: %s", bad.ToBytes())
	}
	miss := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "sbmax", "*",
		"LOAD", "1", "@n",
		"SORTBY", "2", "@n", "DESC", "MAX",
	))
	if !protocol.IsErrorReply(miss) || !strings.Contains(string(miss.ToBytes()), "Bad arguments for MAX") {
		t.Fatalf("missing MAX value: %s", miss.ToBytes())
	}
}

func unwrapFTAggregate(r redis.Reply) (*protocol.MultiBulkReply, bool) {
	switch v := r.(type) {
	case *protocol.MultiBulkReply:
		return v, true
	case *FTAggregateReply:
		if mb, ok := v.resp2.(*protocol.MultiBulkReply); ok {
			return mb, true
		}
	}
	return nil, false
}

// TestFTTimeoutErrorWording aligns TIMEOUT parse errors with Redis 8.x
// SEARCH_PARSE_ARGS Need argument / TIMEOUT requires a non negative integer.
func TestFTTimeoutErrorWording(t *testing.T) {
	db := makeTestDB()
	_ = db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "tmo", "ON", "HASH", "PREFIX", "1", "tmo:",
		"SCHEMA", "t", "TEXT",
	))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "tmo:1", "t", "hello"))

	neg := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "tmo", "*", "TIMEOUT", "-1"))
	if !protocol.IsErrorReply(neg) || !strings.Contains(string(neg.ToBytes()), "SEARCH_PARSE_ARGS TIMEOUT requires a non negative integer") {
		t.Fatalf("TIMEOUT -1: %s", neg.ToBytes())
	}
	abc := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "tmo", "*", "TIMEOUT", "abc"))
	if !protocol.IsErrorReply(abc) || !strings.Contains(string(abc.ToBytes()), "SEARCH_PARSE_ARGS TIMEOUT requires a non negative integer") {
		t.Fatalf("TIMEOUT abc: %s", abc.ToBytes())
	}
	miss := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "tmo", "*", "TIMEOUT"))
	if !protocol.IsErrorReply(miss) || !strings.Contains(string(miss.ToBytes()), "SEARCH_PARSE_ARGS Need argument for TIMEOUT") {
		t.Fatalf("TIMEOUT missing: %s", miss.ToBytes())
	}
	agg := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "tmo", "*", "TIMEOUT", "-1"))
	if !protocol.IsErrorReply(agg) || !strings.Contains(string(agg.ToBytes()), "SEARCH_PARSE_ARGS TIMEOUT requires a non negative integer") {
		t.Fatalf("AGG TIMEOUT -1: %s", agg.ToBytes())
	}
}
