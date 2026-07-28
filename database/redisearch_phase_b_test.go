package database

import (
	"strconv"
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTAggregateWithCursorPages verifies FT.AGGREGATE WITHCURSOR COUNT n
// returns a page plus a non-zero cursor, FT.CURSOR READ pages through the
// remainder, and the cursor reaches 0 once exhausted.
func TestFTAggregateWithCursorPages(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx_cursor", "ON", "HASH", "PREFIX", "1", "c:", "SCHEMA", "n", "NUMERIC",
	)), "OK")
	for i := 0; i < 5; i++ {
		_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "idx_cursor", "c:"+strconv.Itoa(i), "FIELDS", "n", strconv.Itoa(i)))
	}

	r := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "idx_cursor", "*", "WITHCURSOR", "COUNT", "2"))
	multi, ok := r.(*protocol.MultiRawReply)
	if !ok || len(multi.Replies) != 2 {
		t.Fatalf("expected [results, cursor], got %s", r.ToBytes())
	}
	inner, ok := multi.Replies[0].(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("expected inner MultiBulkReply, got %T", multi.Replies[0])
	}
	if got := len(inner.Args) - 1; got != 2 {
		t.Fatalf("expected 2 rows on first page, got %d (%s)", got, r.ToBytes())
	}
	cursorReply, ok := multi.Replies[1].(*protocol.IntReply)
	if !ok || cursorReply.Code == 0 {
		t.Fatalf("expected non-zero cursor after first page, got %s", r.ToBytes())
	}
	cursorID := cursorReply.Code
	totalSeen := len(inner.Args) - 1

	for cursorID != 0 {
		r = db.Exec(nil, utils.ToCmdLine("FT.CURSOR", "READ", "idx_cursor", strconv.FormatInt(cursorID, 10), "COUNT", "2"))
		multi, ok = r.(*protocol.MultiRawReply)
		if !ok || len(multi.Replies) != 2 {
			t.Fatalf("CURSOR READ: %s", r.ToBytes())
		}
		inner, ok = multi.Replies[0].(*protocol.MultiBulkReply)
		if !ok {
			t.Fatalf("CURSOR READ inner: %T", multi.Replies[0])
		}
		totalSeen += len(inner.Args) - 1
		cid, ok := multi.Replies[1].(*protocol.IntReply)
		if !ok {
			t.Fatalf("CURSOR READ cursor: %T", multi.Replies[1])
		}
		cursorID = cid.Code
	}
	if totalSeen != 5 {
		t.Fatalf("expected to see 5 rows total across all pages, got %d", totalSeen)
	}

	// FT.CURSOR DEL removes a cursor before it's exhausted; a subsequent READ
	// (or a second DEL) must fail with "Cursor not found".
	r = db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "idx_cursor", "*", "WITHCURSOR", "COUNT", "1"))
	multi = r.(*protocol.MultiRawReply)
	delCursorID := multi.Replies[1].(*protocol.IntReply).Code
	if delCursorID == 0 {
		t.Fatalf("expected non-zero cursor for DEL test, got %s", r.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CURSOR", "DEL", "idx_cursor", strconv.FormatInt(delCursorID, 10),
	)), "OK")

	readAfterDel := db.Exec(nil, utils.ToCmdLine(
		"FT.CURSOR", "READ", "idx_cursor", strconv.FormatInt(delCursorID, 10),
	))
	if !protocol.IsErrorReply(readAfterDel) {
		t.Fatalf("expected error reading a deleted cursor, got %s", readAfterDel.ToBytes())
	}

	delAgain := db.Exec(nil, utils.ToCmdLine(
		"FT.CURSOR", "DEL", "idx_cursor", strconv.FormatInt(delCursorID, 10),
	))
	if !protocol.IsErrorReply(delAgain) {
		t.Fatalf("expected error deleting an already-deleted cursor, got %s", delAgain.ToBytes())
	}
}

// TestFTAggregateApplyExpression verifies a minimal APPLY expression
// (numeric field reference * literal) with no GROUPBY, so each document
// contributes its own row carrying the computed field.
func TestFTAggregateApplyExpression(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx_apply", "ON", "HASH", "PREFIX", "1", "p:", "SCHEMA", "price", "NUMERIC",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "idx_apply", "p:1", "FIELDS", "price", "10"))
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "idx_apply", "p:2", "FIELDS", "price", "20"))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "idx_apply", "*", "APPLY", "@price*2", "AS", "double",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("APPLY: %s", r.ToBytes())
	}
	s := string(r.ToBytes())
	if !strings.Contains(s, "double") || !strings.Contains(s, "20") || !strings.Contains(s, "40") {
		t.Fatalf("expected computed field 'double' with values 20/40, got %s", s)
	}
}

// TestFTAggregateApplyAfterGroupBy verifies an APPLY clause placed after
// GROUPBY/REDUCE can reference the reducer's output field.
func TestFTAggregateApplyAfterGroupBy(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx_apply_post", "ON", "HASH", "PREFIX", "1", "q:", "SCHEMA", "cat", "TAG", "price", "NUMERIC",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "idx_apply_post", "q:1", "FIELDS", "cat", "a", "price", "10"))
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "idx_apply_post", "q:2", "FIELDS", "cat", "a", "price", "20"))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "idx_apply_post", "*",
		"GROUPBY", "1", "@cat",
		"REDUCE", "SUM", "1", "@price", "AS", "sum",
		"APPLY", "@sum*2", "AS", "doubleSum",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("APPLY after GROUPBY: %s", r.ToBytes())
	}
	s := string(r.ToBytes())
	if !strings.Contains(s, "doubleSum") || !strings.Contains(s, "60") {
		t.Fatalf("expected doubleSum=60, got %s", s)
	}
}

// TestFTAggregateApplyBeforeGroupByFeedsReduce verifies an APPLY clause
// placed before GROUPBY computes a per-document field that a later REDUCE
// can then aggregate over.
func TestFTAggregateApplyBeforeGroupByFeedsReduce(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx_apply_pre", "ON", "HASH", "PREFIX", "1", "r:", "SCHEMA", "cat", "TAG", "price", "NUMERIC",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "idx_apply_pre", "r:1", "FIELDS", "cat", "a", "price", "10"))
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "idx_apply_pre", "r:2", "FIELDS", "cat", "a", "price", "20"))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "idx_apply_pre", "*",
		"APPLY", "@price*2", "AS", "doubled",
		"GROUPBY", "1", "@cat",
		"REDUCE", "SUM", "1", "@doubled", "AS", "sumDoubled",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("APPLY before GROUPBY: %s", r.ToBytes())
	}
	s := string(r.ToBytes())
	if !strings.Contains(s, "sumDoubled") || !strings.Contains(s, "60") {
		t.Fatalf("expected sumDoubled=60, got %s", s)
	}
}
