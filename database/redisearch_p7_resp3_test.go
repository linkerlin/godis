package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestP7FTSearchDualForm verifies FT.SEARCH returns a reply whose RESP2 form is
// the unchanged positional array and whose RESP3 form is the Redis 8.x map.
func TestP7FTSearchDualForm(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p7s", "SCHEMA", "t", "TEXT",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p7s", "d1", "FIELDS", "t", "hello")); protocol.IsErrorReply(r) {
		t.Fatalf("add: %s", r.ToBytes())
	}

	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p7s", "hello", "WITHSCORES"))
	ft, ok := r.(*FTSearchReply)
	if !ok {
		t.Fatalf("expected *FTSearchReply, got %T (%s)", r, r.ToBytes())
	}

	// RESP2 form: positional array starting with the total count.
	resp2 := string(ft.ToBytes())
	if !strings.HasPrefix(resp2, "*") {
		t.Fatalf("RESP2 form should be an array: %q", resp2)
	}
	if !strings.Contains(resp2, "d1") {
		t.Fatalf("RESP2 form should contain doc id d1: %q", resp2)
	}

	// RESP3 form: top-level map with the 8.x keys.
	resp3 := string(ft.ToRESP3())
	if !strings.HasPrefix(resp3, "%5\r\n") {
		t.Fatalf("RESP3 form should be a 5-entry map: %q", resp3)
	}
	for _, key := range []string{"total_results", "results", "attributes", "format", "warning"} {
		if !strings.Contains(resp3, key) {
			t.Fatalf("RESP3 map missing key %q: %q", key, resp3)
		}
	}
	if !strings.Contains(resp3, "d1") {
		t.Fatalf("RESP3 results should contain doc id d1: %q", resp3)
	}
}

// TestP7FTInfoMapReply verifies FT.INFO returns a MapReply (RESP2 flat k/v,
// RESP3 proper map) with nested structure preserved.
func TestP7FTInfoMapReply(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p7i", "SCHEMA", "t", "TEXT",
	)), "OK")

	r := db.Exec(nil, utils.ToCmdLine("FT.INFO", "p7i"))
	mr, ok := r.(*protocol.MapReply)
	if !ok {
		t.Fatalf("expected *MapReply, got %T (%s)", r, r.ToBytes())
	}
	// RESP2: flat k/v array.
	resp2 := string(mr.ToBytes())
	if !strings.HasPrefix(resp2, "*") {
		t.Fatalf("RESP2 form should be an array: %q", resp2)
	}
	if !strings.Contains(resp2, "index_name") {
		t.Fatalf("RESP2 FT.INFO missing index_name: %q", resp2)
	}
	// RESP3: map.
	resp3 := string(mr.ToRESP3())
	if !strings.HasPrefix(resp3, "%") {
		t.Fatalf("RESP3 form should be a map: %q", resp3)
	}
	if !strings.Contains(resp3, "attributes") {
		t.Fatalf("RESP3 FT.INFO missing attributes: %q", resp3)
	}
}

// TestP7FTAggregateDualForm verifies FT.AGGREGATE returns a reply whose RESP3
// form is the 8.x map shape.
func TestP7FTAggregateDualForm(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p7a", "SCHEMA", "t", "TEXT",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p7a", "d1", "FIELDS", "t", "hello")); protocol.IsErrorReply(r) {
		t.Fatalf("add: %s", r.ToBytes())
	}

	r := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "p7a", "*"))
	ag, ok := r.(*FTAggregateReply)
	if !ok {
		t.Fatalf("expected *FTAggregateReply, got %T (%s)", r, r.ToBytes())
	}
	// RESP2: flat array.
	if !strings.HasPrefix(string(ag.ToBytes()), "*") {
		t.Fatalf("RESP2 form should be an array: %q", ag.ToBytes())
	}
	// RESP3: 8.x map.
	resp3 := string(ag.ToRESP3())
	if !strings.HasPrefix(resp3, "%5\r\n") {
		t.Fatalf("RESP3 form should be a 5-entry map: %q", resp3)
	}
	for _, key := range []string{"total_results", "results", "attributes", "format", "warning"} {
		if !strings.Contains(resp3, key) {
			t.Fatalf("RESP3 aggregate map missing key %q: %q", key, resp3)
		}
	}
}
