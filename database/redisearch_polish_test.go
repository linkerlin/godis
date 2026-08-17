package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestAggregateMaxResultsCap verifies FT.AGGREGATE results are capped by
// search-max-aggregate-results (8.0 name), not MAXSEARCHRESULTS.
func TestAggregateMaxResultsCap(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "cap", "SCHEMA", "v", "NUMERIC",
	)), "OK")
	for i := 0; i < 5; i++ {
		if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "cap", "cap:"+intToStr2(int64(i)), "FIELDS", "v", intToStr2(int64(i)))); protocol.IsErrorReply(r) {
			t.Fatalf("add: %s", r.ToBytes())
		}
	}
	// Set a tiny aggregate cap; the default LIMIT 0 10 must be clamped to it.
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MAXAGGREGATERESULTS", "3")), "OK")
	r := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "cap", "*", "LOAD", "1", "@v"))
	t.Logf("aggregate reply = %q", r.ToBytes())
	// Redis reports total (LIMIT-before) as the first integer, and the page
	// rows are clamped by the cap. Assert the PAGE has 3 rows: v=0,1,2 must be
	// present and v=3,4 must not. (LOAD required: absent LOAD drops non-SORTABLE.)
	body := string(r.ToBytes())
	for _, want := range []string{"0", "1", "2"} {
		if !strings.Contains(body, "$1\r\n"+want+"\r\n") {
			t.Fatalf("aggregate cap page missing v=%s: %s", want, body)
		}
	}
	if strings.Contains(body, "$1\r\n3\r\n") || strings.Contains(body, "$1\r\n4\r\n") {
		t.Fatalf("aggregate cap page should not contain v=3 or v=4: %s", body)
	}
	// Reset.
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MAXAGGREGATERESULTS", "2147483648")), "OK")
}

// TestExplainRealAST verifies FT.EXPLAIN renders the ACTUAL parsed AST (a
// union containing terms), not the old hand-rolled approximation.
func TestExplainRealAST(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "expl", "SCHEMA", "t", "TEXT",
	)), "OK")

	// "hello world | foo" parses as UNION{INTERSECT{hello world}, foo} under
	// DIALECT 1. The plan must show the union of an intersect.
	r := db.Exec(nil, utils.ToCmdLine("FT.EXPLAIN", "expl", "hello world | foo", "DIALECT", "1"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("EXPLAIN shape: %T %s", r, r.ToBytes())
	}
	plan := string(bulk.Arg)
	if !strings.Contains(plan, "UNION") || !strings.Contains(plan, "INTERSECT") {
		t.Fatalf("EXPLAIN should show UNION of INTERSECT: %s", plan)
	}
	if !strings.Contains(plan, "TERM{hello}") || !strings.Contains(plan, "TERM{world}") || !strings.Contains(plan, "TERM{foo}") {
		t.Fatalf("EXPLAIN should list all three terms: %s", plan)
	}

	// A numeric range node renders as RANGE.
	r = db.Exec(nil, utils.ToCmdLine("FT.EXPLAIN", "expl", "@n:[10 20]", "DIALECT", "2"))
	bulk, _ = r.(*protocol.BulkReply)
	if !strings.Contains(string(bulk.Arg), "RANGE{n [10 20]}") {
		t.Fatalf("EXPLAIN numeric range should render RANGE{n [10 20]}: %s", bulk.Arg)
	}
}
