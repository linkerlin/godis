package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// TestFTAggregateAddScores exposes BM25STD score as @__score and sorts DESC.
// Subset of Redis 8.x ADDSCORES; scores are Godis-self-consistent (b=0.09), not
// byte-identical to RediSearch.
func TestFTAggregateAddScores(t *testing.T) {
	db := makeTestDB()
	create := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "addsc", "ON", "HASH", "PREFIX", "1", "as:",
		"SCHEMA", "t", "TEXT",
	))
	if protocol.IsErrorReply(create) {
		t.Fatalf("FT.CREATE: %s", create.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "as:a", "t", "hello"))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "as:b", "t", "hello hello hello"))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "as:c", "t", "world"))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "addsc", "hello",
		"ADDSCORES",
		"SORTBY", "2", "@__score", "DESC",
		"LIMIT", "0", "10",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("ADDSCORES: %s", r.ToBytes())
	}
	s := string(r.ToBytes())
	if !strings.Contains(s, "__score") {
		t.Fatalf("want __score in pipeline rows, got %s", s)
	}
	// Both hello docs match; "world" must not appear.
	if strings.Contains(s, "as:c") || strings.Contains(s, "world") {
		t.Fatalf("non-matching doc leaked: %s", s)
	}

	// Without ADDSCORES, @__score is absent from the row.
	plain := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "addsc", "hello",
		"LIMIT", "0", "10",
	))
	if protocol.IsErrorReply(plain) {
		t.Fatalf("plain AGGREGATE: %s", plain.ToBytes())
	}
	if strings.Contains(string(plain.ToBytes()), "__score") {
		t.Fatalf("plain AGGREGATE must not inject __score: %s", plain.ToBytes())
	}
}
