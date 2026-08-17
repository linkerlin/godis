package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestCollectReducer verifies REDUCE COLLECT gathers per-group arrays of
// projected documents with DISTINCT / SORTBY / LIMIT support.
func TestCollectReducer(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "cl", "SCHEMA", "cat", "TAG", "SORTABLE", "v", "NUMERIC", "SORTABLE",
	)), "OK")
	// 3 docs in cat:x with v=10,20,30; 1 doc in cat:y with v=5.
	for _, kv := range [][2]string{
		{"cl:1", "10"}, {"cl:2", "20"}, {"cl:3", "30"},
	} {
		if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "cl", kv[0], "FIELDS", "cat", "x", "v", kv[1])); protocol.IsErrorReply(r) {
			t.Fatalf("add %s: %s", kv[0], r.ToBytes())
		}
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "cl", "cl:y", "FIELDS", "cat", "y", "v", "5")); protocol.IsErrorReply(r) {
		t.Fatalf("add y: %s", r.ToBytes())
	}

	// COLLECT with FIELDS * for cat:x, SORTBY @v DESC, LIMIT 0 2 — top-2 by v.
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "cl", "*", "GROUPBY", "1", "@cat",
		"REDUCE", "COLLECT", "9", "FIELDS", "*", "SORTBY", "@v", "DESC", "LIMIT", "0", "2", "AS", "top",
		"FILTER", "@cat == x",
	))
	body := string(r.ToBytes())
	// The collected array should contain the two highest v values.
	if !strings.Contains(body, "30") || !strings.Contains(body, "20") {
		t.Fatalf("COLLECT top-2 should contain v=30 and v=20: %s", body)
	}
	// LIMIT 0 2 means exactly 2 entries: count the __key markers.
	if got := strings.Count(body, "__key"); got != 2 {
		t.Fatalf("COLLECT LIMIT 0 2 should yield 2 entries (got %d __key markers): %s", got, body)
	}
	// v=10 must be outside the top-2 (SORTBY DESC).
	if strings.Contains(body, "10") {
		t.Fatalf("COLLECT SORTBY DESC top-2 should exclude v=10: %s", body)
	}
}

// TestCollectDistinct verifies the DISTINCT modifier collapses duplicates.
func TestCollectDistinct(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "cld", "SCHEMA", "cat", "TAG", "SORTABLE", "v", "NUMERIC", "SORTABLE",
	)), "OK")
	// Two docs in cat:x with identical v=7.
	for _, id := range []string{"cld:1", "cld:2"} {
		if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "cld", id, "FIELDS", "cat", "x", "v", "7")); protocol.IsErrorReply(r) {
			t.Fatalf("add %s: %s", id, r.ToBytes())
		}
	}

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "cld", "*", "GROUPBY", "1", "@cat",
		"REDUCE", "COLLECT", "4", "FIELDS", "1", "@v", "DISTINCT", "AS", "docs",
		"FILTER", "@cat == x",
	))
	body := string(r.ToBytes())
	t.Logf("distinct reply = %q", body)
	// DISTINCT over @v (both 7) yields exactly 1 entry: the "docs" field value
	// must be a 1-element array. (Counting "7" chars is unreliable — the
	// wire-format length prefixes like $73 also contain '7'.)
	if !strings.Contains(body, "$4\r\ndocs\r\n$29\r\n*1\r\n") {
		t.Fatalf("COLLECT DISTINCT over identical @v should yield 1 entry: %s", body)
	}
}

// TestCollectPassthrough verifies COLLECT without GROUPBY works per document.
func TestCollectPassthrough(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "clp", "SCHEMA", "v", "NUMERIC",
	)), "OK")
	for i := 1; i <= 3; i++ {
		if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "clp", "clp:"+intToStr2(int64(i)), "FIELDS", "v", intToStr2(int64(i)))); protocol.IsErrorReply(r) {
			t.Fatalf("add: %s", r.ToBytes())
		}
	}
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "clp", "*",
		"GROUPBY", "0",
		"REDUCE", "COLLECT", "3", "FIELDS", "1", "@__key", "AS", "all",
	))
	body := string(r.ToBytes())
	t.Logf("passthrough reply = %s", body)
	for _, id := range []string{"clp:1", "clp:2", "clp:3"} {
		if !strings.Contains(body, id) {
			t.Fatalf("COLLECT passthrough should include %s: %s", id, body)
		}
	}
}

// intToStr2 is a tiny helper avoiding strconv imports in the test file.
func intToStr2(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
