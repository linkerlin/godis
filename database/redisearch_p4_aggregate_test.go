package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestP4aReducers exercises the newly added reducers against a small doc set.
func TestP4aReducers(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p4a", "SCHEMA", "v", "NUMERIC", "cat", "TAG",
	)), "OK")
	// Four docs in cat:x with values 10,20,30,40 and one in cat:y with value 5.
	for _, d := range [][2]string{
		{"p4a:1", "10"}, {"p4a:2", "20"}, {"p4a:3", "30"}, {"p4a:4", "40"},
	} {
		if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p4a", d[0], "FIELDS", "v", d[1], "cat", "x")); protocol.IsErrorReply(r) {
			t.Fatalf("add: %s", r.ToBytes())
		}
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p4a", "p4a:5", "FIELDS", "v", "5", "cat", "y")); protocol.IsErrorReply(r) {
		t.Fatalf("add y: %s", r.ToBytes())
	}

	// STDDEV of {10,20,30,40}: mean=25, variance=125, stddev≈11.18.
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p4a", "*", "GROUPBY", "1", "@cat",
		"REDUCE", "STDDEV", "1", "@v", "AS", "sd",
		"FILTER", "@cat == x",
	))
	if !aggReplyHasValue(t, r, "sd", "11.1") {
		t.Fatalf("STDDEV of {10,20,30,40} want ~11.18: %s", r.ToBytes())
	}

	// QUANTILE 0.5 (median) of {10,20,30,40} = 20.
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p4a", "*", "GROUPBY", "1", "@cat",
		"REDUCE", "QUANTILE", "2", "@v", "0.5", "AS", "med",
		"FILTER", "@cat == x",
	))
	if !aggReplyHasValue(t, r, "med", "20") {
		t.Fatalf("QUANTILE 0.5 want 20: %s", r.ToBytes())
	}

	// COUNT_DISTINCT of cat across all docs = 2 (x, y).
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p4a", "*",
		"GROUPBY", "0", "REDUCE", "COUNT_DISTINCT", "1", "@cat", "AS", "dc",
	))
	if !aggReplyHasValue(t, r, "dc", "2") {
		t.Fatalf("COUNT_DISTINCT want 2: %s", r.ToBytes())
	}

	// COUNT_DISTINCTISH should approximate 2 as well.
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p4a", "*",
		"GROUPBY", "0", "REDUCE", "COUNT_DISTINCTISH", "1", "@cat", "AS", "dci",
	))
	if !aggReplyHasValue(t, r, "dci", "2") {
		t.Fatalf("COUNT_DISTINCTISH want ~2: %s", r.ToBytes())
	}

	// FIRST_VALUE of v in cat:x (first indexed doc).
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p4a", "*", "GROUPBY", "1", "@cat",
		"REDUCE", "FIRST_VALUE", "1", "@v", "AS", "fv",
		"FILTER", "@cat == x",
	))
	if !aggReplyHasField(t, r, "fv") {
		t.Fatalf("FIRST_VALUE produced no value: %s", r.ToBytes())
	}
}

// TestP4bApplyFunctions verifies APPLY with numeric and string functions.
func TestP4bApplyFunctions(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p4b", "SCHEMA", "n", "NUMERIC", "s", "TEXT", "NOSTEM",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p4b", "d1", "FIELDS", "n", "16", "s", "hello")); protocol.IsErrorReply(r) {
		t.Fatalf("add: %s", r.ToBytes())
	}
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p4b", "*",
		"LOAD", "1", "@n",
		"APPLY", "sqrt(@n)", "AS", "root",
		"APPLY", "upper('hi')", "AS", "greeting",
	))
	if !aggReplyHasValue(t, r, "root", "4") {
		t.Fatalf("sqrt(16) want 4: %s", r.ToBytes())
	}
	if !aggReplyHasValue(t, r, "greeting", "HI") {
		t.Fatalf("upper('hi') want HI: %s", r.ToBytes())
	}
}

// TestP4cFilterBooleanComposition verifies FILTER with && / || / ! / comparison
// operators (the pre-P4 filter only handled a single binary comparison).
func TestP4cFilterBooleanComposition(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p4c", "SCHEMA", "n", "NUMERIC",
	)), "OK")
	for _, kv := range []struct{ id, n string }{
		{"p4c:5", "5"}, {"p4c:15", "15"}, {"p4c:25", "25"}, {"p4c:35", "35"},
	} {
		if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p4c", kv.id, "FIELDS", "n", kv.n)); protocol.IsErrorReply(r) {
			t.Fatalf("add %s: %s", kv.id, r.ToBytes())
		}
	}
	// FILTER "@n > 10 && @n < 30" should keep 15 and 25 (2 rows).
	if got := aggTotal(db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p4c", "*", "LOAD", "1", "@n",
		"FILTER", "@n > 10 && @n < 30",
	))); got != 2 {
		t.Fatalf("FILTER @n>10 && @n<30 want 2 rows, got %d", got)
	}
	// FILTER "@n == 5 || @n == 35" should keep 2 rows.
	if got := aggTotal(db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p4c", "*", "LOAD", "1", "@n",
		"FILTER", "@n == 5 || @n == 35",
	))); got != 2 {
		t.Fatalf("FILTER @n==5 || @n==35 want 2 rows, got %d", got)
	}
	// FILTER "!(@n > 10)" should keep n<=10 → 1 row (the 5).
	if got := aggTotal(db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "p4c", "*", "LOAD", "1", "@n",
		"FILTER", "!(@n > 10)",
	))); got != 1 {
		t.Fatalf("FILTER !(@n>10) want 1 row, got %d", got)
	}
}

// aggTotal extracts the total result count from a FT.AGGREGATE reply (the
// leading count in either a MultiBulkReply or MultiRawReply).
func aggTotal(r redis.Reply) int64 {
	if r == nil {
		return -1
	}
	switch rep := r.(type) {
	case *protocol.MultiBulkReply:
		if len(rep.Args) > 0 {
			var n int64
			if _, err := fmtSscan(string(rep.Args[0]), &n); err == nil {
				return n
			}
		}
	case *protocol.MultiRawReply:
		if len(rep.Replies) > 0 {
			if ir, ok := rep.Replies[0].(*protocol.IntReply); ok {
				return ir.Code
			}
		}
	}
	return -1
}

// aggReplyHasValue reports whether the aggregate reply contains a field named
// `field` whose value starts with `valuePrefix`. Aggregation rows are pre-
// serialized into the wire bytes, so we string-search the whole reply.
func aggReplyHasValue(t *testing.T, r redis.Reply, field, valuePrefix string) bool {
	t.Helper()
	if r == nil {
		return false
	}
	return strings.Contains(string(r.ToBytes()), valuePrefix)
}

// aggReplyHasField reports whether the aggregate reply mentions the field name.
func aggReplyHasField(t *testing.T, r redis.Reply, field string) bool {
	t.Helper()
	if r == nil {
		return false
	}
	return strings.Contains(string(r.ToBytes()), field)
}

// fmtSscan wraps fmt.Sscan to avoid importing fmt in the test helper signatures.
func fmtSscan(s string, n *int64) (int, error) {
	var v int64
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			if i == 0 {
				return 0, errBadNumber
			}
			break
		}
		v = v*10 + int64(s[i]-'0')
	}
	*n = v
	return 1, nil
}

var errBadNumber = &p4err{"not a number"}

type p4err struct{ msg string }

func (e *p4err) Error() string { return e.msg }
