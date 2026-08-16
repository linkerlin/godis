package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// EXPLAINSCORE subset (batch 58): wire shape + WITHSCORES gate; not byte-identical
// to RediSearch BM25 (Godis uses b=0.09).

func TestFTExplainScoreRequiresWithScores(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "expreq", "ON", "HASH", "PREFIX", "1", "er:", "SCHEMA", "t", "TEXT",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "er:1", "t", "hello"))
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "expreq", "hello", "EXPLAINSCORE", "NOCONTENT",
	))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("want ERR, got %s", r.ToBytes())
	}
	msg := string(r.ToBytes())
	if !strings.Contains(msg, "EXPLAINSCORE") || !strings.Contains(msg, "WITHSCORES") {
		t.Fatalf("want SEARCH_PARSE_ARGS EXPLAINSCORE…WITHSCORES, got %s", msg)
	}
}

func TestFTExplainScoreBM25SingleTerm(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "expbm", "ON", "HASH", "PREFIX", "1", "eb:", "SCHEMA", "t", "TEXT",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "eb:1", "t", "hello"))
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "expbm", "hello", "WITHSCORES", "EXPLAINSCORE", "NOCONTENT", "LIMIT", "0", "1",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("search: %s", r.ToBytes())
	}
	body := string(r.ToBytes())
	if !strings.Contains(body, "Final BM25") {
		t.Fatalf("want Final BM25 header, got %s", body)
	}
	if !strings.Contains(body, "hello:") {
		t.Fatalf("want per-term line, got %s", body)
	}
	// Nested score slot: MultiRaw under FTSearchReply.
	ft, ok := r.(*FTSearchReply)
	if !ok {
		t.Fatalf("want FTSearchReply, got %T", r)
	}
	mr, ok := ft.resp2.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) < 3 {
		t.Fatalf("want MultiRaw with score slot, got %T len=%d", ft.resp2, len(mr.Replies))
	}
	slot, ok := mr.Replies[2].(*protocol.MultiRawReply)
	if !ok || len(slot.Replies) != 2 {
		t.Fatalf("score slot want [score,explain], got %#v", mr.Replies[2])
	}
}

func TestFTExplainScoreDocscoreAndDismax(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "expds", "ON", "HASH", "PREFIX", "1", "ed:", "SCHEMA", "t", "TEXT",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "ed:1", "t", "hello world"))

	doc := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "expds", "hello", "WITHSCORES", "EXPLAINSCORE", "SCORER", "DOCSCORE", "NOCONTENT",
	))
	if protocol.IsErrorReply(doc) {
		t.Fatalf("DOCSCORE: %s", doc.ToBytes())
	}
	if !strings.Contains(string(doc.ToBytes()), "Document's score is") {
		t.Fatalf("want Document's score leaf, got %s", doc.ToBytes())
	}

	dm := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "expds", "hello", "WITHSCORES", "EXPLAINSCORE", "SCORER", "DISMAX", "NOCONTENT",
	))
	if protocol.IsErrorReply(dm) {
		t.Fatalf("DISMAX: %s", dm.ToBytes())
	}
	if !strings.Contains(string(dm.ToBytes()), "DISMAX") {
		t.Fatalf("want DISMAX leaf, got %s", dm.ToBytes())
	}
}

func TestFTExplainScoreMultiTermWeightLine(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "expmt", "ON", "HASH", "PREFIX", "1", "em:", "SCHEMA", "t", "TEXT",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "em:1", "t", "hello world"))
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "expmt", "hello world", "WITHSCORES", "EXPLAINSCORE", "NOCONTENT", "LIMIT", "0", "1",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("search: %s", r.ToBytes())
	}
	body := string(r.ToBytes())
	if !strings.Contains(body, "children BM25") {
		t.Fatalf("multi-term want Weight children line, got %s", body)
	}
	if !strings.Contains(body, "hello:") || !strings.Contains(body, "world:") {
		t.Fatalf("want both term lines, got %s", body)
	}
}

func TestFTSortByWithCountAccepted(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "expwc", "ON", "HASH", "PREFIX", "1", "ew:",
		"SCHEMA", "t", "TEXT", "n", "NUMERIC", "SORTABLE",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "ew:1", "t", "hello", "n", "2"))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "ew:2", "t", "hello", "n", "1"))
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "expwc", "hello", "SORTBY", "n", "ASC", "WITHCOUNT", "NOCONTENT",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("SORTBY WITHCOUNT: %s", r.ToBytes())
	}
	if !searchTotalIs(t, r, 2) {
		t.Fatalf("want total 2, got %s", r.ToBytes())
	}
}
