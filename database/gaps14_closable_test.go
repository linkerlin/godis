package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps14BitOpDiffSourceKeysERR(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "DIFF", "out", "a")),
		"ERR BITOP DIFF must be called with at least two source keys.")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "DIFF1", "out", "a")),
		"ERR BITOP DIFF1 must be called with at least two source keys.")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BITOP", "ANDOR", "out", "a")),
		"ERR BITOP ANDOR must be called with at least two source keys.")
}

func TestGaps14LCSLenIdxConflict(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SET", "a", "x"))
	db.Exec(nil, utils.ToCmdLine("SET", "b", "x"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("LCS", "a", "b", "LEN", "IDX")),
		"ERR If you want both the length and indexes, please just use IDX.")
}

func TestGaps14LCSIdxMatches(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SET", "a", "hello"))
	db.Exec(nil, utils.ToCmdLine("SET", "b", "hello"))
	r := db.Exec(nil, utils.ToCmdLine("LCS", "a", "b", "IDX", "WITHMATCHLEN"))
	m, ok := r.(*protocol.MapReply)
	if !ok {
		t.Fatalf("type %T %s", r, r.ToBytes())
	}
	matches, ok := m.Data["matches"].(*protocol.MultiRawReply)
	if !ok || len(matches.Replies) != 1 {
		t.Fatalf("identical: want 1 match, got %T %s", m.Data["matches"], r.ToBytes())
	}
	entry, ok := matches.Replies[0].(*protocol.MultiRawReply)
	if !ok || len(entry.Replies) != 3 {
		t.Fatalf("match entry: %T", matches.Replies[0])
	}
	asserts.AssertIntReply(t, entry.Replies[2], 5)
	asserts.AssertIntReply(t, m.Data["len"], 5)

	db.Exec(nil, utils.ToCmdLine("SET", "a", "ohmytext"))
	db.Exec(nil, utils.ToCmdLine("SET", "b", "mynewtext"))
	r = db.Exec(nil, utils.ToCmdLine("LCS", "a", "b", "IDX", "WITHMATCHLEN"))
	m, ok = r.(*protocol.MapReply)
	if !ok {
		t.Fatalf("ohmy type %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, m.Data["len"], 6)
	matches, ok = m.Data["matches"].(*protocol.MultiRawReply)
	if !ok || len(matches.Replies) != 2 {
		t.Fatalf("ohmy matches: want 2, got %T %s", m.Data["matches"], r.ToBytes())
	}
	// Redis order last→first: "text" then "my"
	first, ok := matches.Replies[0].(*protocol.MultiRawReply)
	if !ok || len(first.Replies) != 3 {
		t.Fatalf("first match: %T", matches.Replies[0])
	}
	asserts.AssertIntReply(t, first.Replies[2], 4)
	r1, ok := first.Replies[0].(*protocol.MultiRawReply)
	if !ok || len(r1.Replies) != 2 {
		t.Fatalf("range1: %T", first.Replies[0])
	}
	asserts.AssertIntReply(t, r1.Replies[0], 4)
	asserts.AssertIntReply(t, r1.Replies[1], 7)

	filtered := db.Exec(nil, utils.ToCmdLine("LCS", "a", "b", "IDX", "MINMATCHLEN", "3", "WITHMATCHLEN"))
	fm, ok := filtered.(*protocol.MapReply)
	if !ok {
		t.Fatalf("filter type %T", filtered)
	}
	fmMatches, ok := fm.Data["matches"].(*protocol.MultiRawReply)
	if !ok || len(fmMatches.Replies) != 1 {
		t.Fatalf("MINMATCHLEN 3: want 1 match, got %s", filtered.ToBytes())
	}
	asserts.AssertIntReply(t, fm.Data["len"], 6)
}
