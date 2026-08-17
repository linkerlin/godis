package database

import (
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/datastruct/redisearch"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTAggregateApplyToStrToNumber aligns APPLY to_str/to_number with Redis 8.x.
func TestFTAggregateApplyToStrToNumber(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "tos", "ON", "HASH", "PREFIX", "1", "tos:",
		"SCHEMA", "n", "NUMERIC", "SORTABLE", "s", "TEXT", "SORTABLE",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "tos:1", "n", "42", "s", "7.5"))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "tos", "*",
		"APPLY", "to_str(@n)", "AS", "ns",
		"APPLY", "to_number(@s)", "AS", "sn",
		"APPLY", "to_number(@ns) + 1", "AS", "n1",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("to_str/to_number: %s", r.ToBytes())
	}
	body := string(r.ToBytes())
	if !strings.Contains(body, "42") || !strings.Contains(body, "7.5") || !strings.Contains(body, "43") {
		t.Fatalf("want to_str/to_number values in reply: %s", body)
	}

	bad := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "tos", "*",
		"APPLY", "to_number('abc')", "AS", "x",
	))
	if !protocol.IsErrorReply(bad) ||
		!strings.Contains(string(bad.ToBytes()), "SEARCH_PARSE_ARGS to_number: cannot convert string 'abc'") {
		t.Fatalf("to_number bad string: %s", bad.ToBytes())
	}

	unk := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "tos", "*",
		"APPLY", "nosuchfn(@n)", "AS", "x",
	))
	if !protocol.IsErrorReply(unk) ||
		!strings.Contains(string(unk.ToBytes()), "SEARCH_EXPR Unknown function name 'nosuchfn'") {
		t.Fatalf("unknown APPLY fn: %s", unk.ToBytes())
	}

	nan := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "tos", "*",
		"APPLY", "floor('hello')", "AS", "f",
	))
	if protocol.IsErrorReply(nan) {
		t.Fatalf("floor(non-numeric) should be nan: %s", nan.ToBytes())
	}
	if !strings.Contains(string(nan.ToBytes()), "nan") {
		t.Fatalf("want lowercase nan: %s", nan.ToBytes())
	}
}

// TestFTOnTimeoutSoftPartial verifies soft TIMEOUT + SEARCH_TIMEOUT FAIL wording.
func TestFTOnTimeoutSoftPartial(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "tmo_soft", "ON", "HASH", "PREFIX", "1", "ts:",
		"SCHEMA", "t", "TEXT",
	)), "OK")
	for i := 0; i < 40; i++ {
		id := "ts:" + strings.Repeat("x", i+1)
		_ = db.Exec(nil, utils.ToCmdLine("HSET", id, "t", "hello world"))
	}
	engine := searchEngines["tmo_soft"]
	if engine == nil {
		t.Fatal("missing engine")
	}

	res, err := engine.Search("hello", &redisearch.SearchOptions{TimeoutMs: 1})
	if err != redisearch.ErrTimeout {
		res, err = engine.Search("hello", &redisearch.SearchOptions{
			Deadline: time.Now().Add(-time.Millisecond),
		})
	}
	if err != redisearch.ErrTimeout {
		t.Fatalf("want ErrTimeout, got %v (res=%v)", err, res)
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "ON_TIMEOUT", "FAIL")), "OK")
	fail := ftTimeoutReply(redisearch.ErrTimeout)
	out := string(fail.ToBytes())
	if !protocol.IsErrorReply(fail) || !strings.Contains(out, "SEARCH_TIMEOUT Timeout limit was reached") {
		t.Fatalf("FAIL want SEARCH_TIMEOUT: %s", out)
	}
	if strings.Contains(out, "ERR SEARCH_TIMEOUT") {
		t.Fatalf("must not double-prefix ERR: %s", out)
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "ON_TIMEOUT", "RETURN")), "OK")
	defer func() {
		_ = db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "ON_TIMEOUT", "FAIL"))
	}()
	empty := ftTimeoutReply(redisearch.ErrTimeout)
	if protocol.IsErrorReply(empty) {
		t.Fatalf("RETURN empty: %s", empty.ToBytes())
	}

	_, aerr := engine.Aggregate(&redisearch.AggregationRequest{
		Query:    "*",
		Deadline: time.Now().Add(-time.Millisecond),
	})
	if aerr != redisearch.ErrTimeout {
		t.Fatalf("agg past deadline: %v", aerr)
	}
}
