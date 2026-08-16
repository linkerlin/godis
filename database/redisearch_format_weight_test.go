package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// TestFTSearchFormatOptions aligns FORMAT with Redis 8.10 error paths.
func TestFTSearchFormatOptions(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("FT.CREATE", "fmtidx", "ON", "HASH", "PREFIX", "1", "fmt:",
		"SCHEMA", "title", "TEXT"))
	db.Exec(nil, utils.ToCmdLine("HSET", "fmt:1", "title", "dogs"))

	ok := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "fmtidx", "dogs", "FORMAT", "STRING", "NOCONTENT"))
	if protocol.IsErrorReply(ok) {
		t.Fatalf("FORMAT STRING: %s", ok.ToBytes())
	}

	missing := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "fmtidx", "dogs", "FORMAT"))
	if !protocol.IsErrorReply(missing) || !strings.Contains(string(missing.ToBytes()), "Need an argument for FORMAT") {
		t.Fatalf("FORMAT missing: %s", missing.ToBytes())
	}

	json := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "fmtidx", "dogs", "FORMAT", "JSON", "NOCONTENT"))
	if !protocol.IsErrorReply(json) || !strings.Contains(string(json.ToBytes()), "FORMAT JSON is not supported") {
		t.Fatalf("FORMAT JSON: %s", json.ToBytes())
	}

	bad := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "fmtidx", "dogs", "FORMAT", "FOO", "NOCONTENT"))
	if !protocol.IsErrorReply(bad) || !strings.Contains(string(bad.ToBytes()), "FORMAT FOO is not supported") {
		t.Fatalf("FORMAT FOO: %s", bad.ToBytes())
	}

	// nil conn → RESP2 path: EXPAND after DIALECT≥3 still needs RESP3.
	expD2 := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "fmtidx", "dogs", "FORMAT", "EXPAND", "NOCONTENT", "DIALECT", "2"))
	if !protocol.IsErrorReply(expD2) || !strings.Contains(string(expD2.ToBytes()), "dialect 3") {
		t.Fatalf("EXPAND DIALECT 2: %s", expD2.ToBytes())
	}
	expD3 := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "fmtidx", "dogs", "FORMAT", "EXPAND", "NOCONTENT", "DIALECT", "3"))
	if !protocol.IsErrorReply(expD3) || !strings.Contains(string(expD3.ToBytes()), "RESP3") {
		t.Fatalf("EXPAND DIALECT 3 RESP2: %s", expD3.ToBytes())
	}

	aggJSON := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "fmtidx", "*", "FORMAT", "JSON"))
	if !protocol.IsErrorReply(aggJSON) || !strings.Contains(string(aggJSON.ToBytes()), "FORMAT JSON is not supported") {
		t.Fatalf("AGGREGATE FORMAT JSON: %s", aggJSON.ToBytes())
	}
	aggOK := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "fmtidx", "*", "FORMAT", "STRING", "LIMIT", "0", "1"))
	if protocol.IsErrorReply(aggOK) {
		t.Fatalf("AGGREGATE FORMAT STRING: %s", aggOK.ToBytes())
	}
}

// TestFTSearchQueryWeightAttr verifies $weight parse/score multiply (subset).
// Uses SCORER DOCSCORE so the base score is the document score (1.0), avoiding
// BM25 edge cases on tiny corpora; weight multiplies that score.
func TestFTSearchQueryWeightAttr(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine("FT.CREATE", "widx", "ON", "HASH", "PREFIX", "1", "w:",
		"SCHEMA", "title", "TEXT")); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	db.Exec(nil, utils.ToCmdLine("HSET", "w:1", "title", "dogs"))

	base := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "widx", "(dogs) => { $weight: 1.0; }",
		"WITHSCORES", "NOCONTENT", "SCORER", "DOCSCORE", "DIALECT", "2",
	))
	if protocol.IsErrorReply(base) {
		t.Fatalf("weight 1 search err: %s", base.ToBytes())
	}
	boost := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "widx", "(dogs) => { $weight: 10.0; }",
		"WITHSCORES", "NOCONTENT", "SCORER", "DOCSCORE", "DIALECT", "2",
	))
	if protocol.IsErrorReply(boost) {
		t.Fatalf("weight 10 search err: %s", boost.ToBytes())
	}
	bScores := ftSearchScores(t, base)
	hScores := ftSearchScores(t, boost)
	if len(bScores) == 0 || len(hScores) == 0 {
		t.Fatalf("scores empty base=%v boost=%v raw=%s / %s", bScores, hScores, base.ToBytes(), boost.ToBytes())
	}
	if bScores[0] < 0.9 || bScores[0] > 1.1 {
		t.Fatalf("weight 1 DOCSCORE want ~1, got %v", bScores)
	}
	if hScores[0] < 9.0 {
		t.Fatalf("weight 10 should lift DOCSCORE to ~10, got base=%v boost=%v", bScores, hScores)
	}

	neg := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "widx", "(dogs) => { $weight: -1; }", "NOCONTENT", "DIALECT", "2",
	))
	if !protocol.IsErrorReply(neg) || !strings.Contains(string(neg.ToBytes()), "weight") {
		t.Fatalf("neg weight: %s", neg.ToBytes())
	}
	abc := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "widx", "(dogs) => { $weight: abc; }", "NOCONTENT", "DIALECT", "2",
	))
	if !protocol.IsErrorReply(abc) || !strings.Contains(string(abc.ToBytes()), "weight") {
		t.Fatalf("abc weight: %s", abc.ToBytes())
	}
	unk := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "widx", "(dogs) => { $bogus: 1; }", "NOCONTENT", "DIALECT", "2",
	))
	if !protocol.IsErrorReply(unk) || !strings.Contains(string(unk.ToBytes()), "Invalid attribute") {
		t.Fatalf("unknown attr: %s", unk.ToBytes())
	}
	phon := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "widx", "(dogs) => { $phonetic: true; }", "NOCONTENT", "DIALECT", "2",
	))
	if !protocol.IsErrorReply(phon) || !strings.Contains(string(phon.ToBytes()), "phonetic") {
		t.Fatalf("phonetic true: %s", phon.ToBytes())
	}
}
