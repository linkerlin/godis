package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestFTAggregateMatchedTerms verifies APPLY matched_terms() returns query
// terms present in the document (Redis 8.10 subset; multi-value array on wire).
func TestFTAggregateMatchedTerms(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "mtidx", "SCHEMA", "title", "TEXT",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.ADD", "mtidx", "doc:1", "FIELDS", "title", "red blue shoes",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("add: %s", r.ToBytes())
	}
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "mtidx", "red blue",
		"LOAD", "1", "@title",
		"APPLY", "matched_terms()", "AS", "mt",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("aggregate: %s", r.ToBytes())
	}
	body := string(r.ToBytes())
	if !strings.Contains(body, "red") || !strings.Contains(body, "blue") {
		t.Fatalf("matched_terms missing terms in reply: %s", body)
	}
	if !strings.Contains(body, "mt") {
		t.Fatalf("matched_terms field alias missing: %s", body)
	}
}
