package database

import (
	"testing"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

func searchTotal(t *testing.T, reply redis.Reply) int64 {
	t.Helper()
	multi, ok := reply.(*protocol.MultiRawReply)
	if !ok || len(multi.Replies) < 1 {
		t.Fatalf("expected MultiRawReply, got %s", reply.ToBytes())
	}
	total, ok := multi.Replies[0].(*protocol.IntReply)
	if !ok {
		t.Fatalf("expected total as IntReply, got %s", reply.ToBytes())
	}
	return total.Code
}

// TestFTCreateInitialScanBackfill verifies that FT.CREATE indexes hash keys
// that already exist in the keyspace before the index was created.
func TestFTCreateInitialScanBackfill(t *testing.T) {
	db := makeTestDB()
	if reply := db.Exec(nil, utils.ToCmdLine("HSET", "doc:1", "title", "hello world")); protocol.IsErrorReply(reply) {
		t.Fatalf("hset: %s", reply.ToBytes())
	}
	if reply := db.Exec(nil, utils.ToCmdLine("HSET", "doc:2", "title", "goodbye world")); protocol.IsErrorReply(reply) {
		t.Fatalf("hset: %s", reply.ToBytes())
	}

	if reply := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx_backfill", "PREFIX", "1", "doc:", "SCHEMA", "title", "TEXT",
	)); protocol.IsErrorReply(reply) {
		t.Fatalf("create: %s", reply.ToBytes())
	}

	reply := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "idx_backfill", "hello"))
	if total := searchTotal(t, reply); total != 1 {
		t.Fatalf("expected 1 hit from initial scan, got %d (%s)", total, reply.ToBytes())
	}
}

// TestFTCreateSkipInitialScan verifies SKIPINITIALSCAN leaves pre-existing
// keys unindexed until they are next written.
func TestFTCreateSkipInitialScan(t *testing.T) {
	db := makeTestDB()
	if reply := db.Exec(nil, utils.ToCmdLine("HSET", "doc:1", "title", "hello world")); protocol.IsErrorReply(reply) {
		t.Fatalf("hset: %s", reply.ToBytes())
	}

	if reply := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx_skip", "PREFIX", "1", "doc:", "SKIPINITIALSCAN", "SCHEMA", "title", "TEXT",
	)); protocol.IsErrorReply(reply) {
		t.Fatalf("create: %s", reply.ToBytes())
	}

	reply := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "idx_skip", "hello"))
	if total := searchTotal(t, reply); total != 0 {
		t.Fatalf("expected 0 hits with SKIPINITIALSCAN, got %d (%s)", total, reply.ToBytes())
	}

	// Writing the key afterwards should still index it via the normal HSET hook.
	if reply := db.Exec(nil, utils.ToCmdLine("HSET", "doc:1", "title", "hello again")); protocol.IsErrorReply(reply) {
		t.Fatalf("hset: %s", reply.ToBytes())
	}
	reply = db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "idx_skip", "hello"))
	if total := searchTotal(t, reply); total != 1 {
		t.Fatalf("expected 1 hit after subsequent HSET, got %d (%s)", total, reply.ToBytes())
	}
}

// TestFTCreateStopWordsZero verifies STOPWORDS 0 disables the default English
// stopword list so that words like "the" become searchable.
func TestFTCreateStopWordsZero(t *testing.T) {
	db := makeTestDB()
	if reply := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx_stop0", "STOPWORDS", "0", "SCHEMA", "title", "TEXT",
	)); protocol.IsErrorReply(reply) {
		t.Fatalf("create: %s", reply.ToBytes())
	}
	if reply := db.Exec(nil, utils.ToCmdLine(
		"FT.ADD", "idx_stop0", "doc1", "FIELDS", "title", "the quick fox",
	)); protocol.IsErrorReply(reply) {
		t.Fatalf("add: %s", reply.ToBytes())
	}

	reply := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "idx_stop0", "the"))
	if total := searchTotal(t, reply); total != 1 {
		t.Fatalf("expected 1 hit searching stop word with STOPWORDS 0, got %d (%s)", total, reply.ToBytes())
	}
}

// TestFTCreateDefaultStopWordsFilterThe verifies the default (no STOPWORDS
// option) behavior still filters common English stop words, as a contrast to
// TestFTCreateStopWordsZero.
func TestFTCreateDefaultStopWordsFilterThe(t *testing.T) {
	db := makeTestDB()
	if reply := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx_stop_default", "SCHEMA", "title", "TEXT",
	)); protocol.IsErrorReply(reply) {
		t.Fatalf("create: %s", reply.ToBytes())
	}
	if reply := db.Exec(nil, utils.ToCmdLine(
		"FT.ADD", "idx_stop_default", "doc1", "FIELDS", "title", "the quick fox",
	)); protocol.IsErrorReply(reply) {
		t.Fatalf("add: %s", reply.ToBytes())
	}

	reply := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "idx_stop_default", "the"))
	if total := searchTotal(t, reply); total != 0 {
		t.Fatalf("expected 0 hits searching default stop word, got %d (%s)", total, reply.ToBytes())
	}
}

// TestFTSynonymExpansionInSearch verifies FT.SYNADD synonyms are expanded
// when searching, so a synonym term finds documents containing the other
// member of the group.
func TestFTSynonymExpansionInSearch(t *testing.T) {
	db := makeTestDB()
	if reply := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx_syn", "SCHEMA", "title", "TEXT",
	)); protocol.IsErrorReply(reply) {
		t.Fatalf("create: %s", reply.ToBytes())
	}
	if reply := db.Exec(nil, utils.ToCmdLine(
		"FT.ADD", "idx_syn", "doc1", "FIELDS", "title", "fast car",
	)); protocol.IsErrorReply(reply) {
		t.Fatalf("add: %s", reply.ToBytes())
	}
	if reply := db.Exec(nil, utils.ToCmdLine("FT.SYNADD", "idx_syn", "quick", "fast")); protocol.IsErrorReply(reply) {
		t.Fatalf("synadd: %s", reply.ToBytes())
	}

	reply := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "idx_syn", "quick"))
	if total := searchTotal(t, reply); total != 1 {
		t.Fatalf("expected 1 hit via synonym expansion, got %d (%s)", total, reply.ToBytes())
	}
}

// TestFTGeoInlineRangeQuery verifies the inline @field:[lon lat radius unit]
// GEO query syntax filters documents by distance.
func TestFTGeoInlineRangeQuery(t *testing.T) {
	db := makeTestDB()
	if reply := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx_geo", "SCHEMA", "loc", "GEO",
	)); protocol.IsErrorReply(reply) {
		t.Fatalf("create: %s", reply.ToBytes())
	}
	// Beijing
	if reply := db.Exec(nil, utils.ToCmdLine(
		"FT.ADD", "idx_geo", "doc1", "FIELDS", "loc", "116.397128,39.916527",
	)); protocol.IsErrorReply(reply) {
		t.Fatalf("add doc1: %s", reply.ToBytes())
	}
	// Shanghai, far outside a 10km radius of Beijing
	if reply := db.Exec(nil, utils.ToCmdLine(
		"FT.ADD", "idx_geo", "doc2", "FIELDS", "loc", "121.473701,31.230416",
	)); protocol.IsErrorReply(reply) {
		t.Fatalf("add doc2: %s", reply.ToBytes())
	}

	reply := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "idx_geo", "@loc:[116.397128 39.916527 10 km]"))
	multi, ok := reply.(*protocol.MultiRawReply)
	if !ok || len(multi.Replies) < 2 {
		t.Fatalf("search: %s", reply.ToBytes())
	}
	if total := searchTotal(t, reply); total != 1 {
		t.Fatalf("expected 1 hit within 10km, got %d (%s)", total, reply.ToBytes())
	}
	docID, ok := multi.Replies[1].(*protocol.BulkReply)
	if !ok || string(docID.Arg) != "doc1" {
		t.Fatalf("expected doc1 within radius, got %s", reply.ToBytes())
	}
}
