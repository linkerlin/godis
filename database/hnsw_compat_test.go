package database

import (
	"strconv"
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestHNSWVInfoAndVLinks(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "vh", "VALUES", "2", "1", "0", "ELE", "a", "M", "16", "EF", "100",
	)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "vh", "VALUES", "2", "0.9", "0.1", "ELE", "b",
	)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "vh", "VALUES", "2", "0", "1", "ELE", "c",
	)), 1)

	info := db.Exec(nil, utils.ToCmdLine("VINFO", "vh"))
	body := string(info.ToBytes())
	if !strings.Contains(body, "hnsw-m") || !strings.Contains(body, "16") {
		t.Fatalf("VINFO should report hnsw-m=16: %s", body)
	}
	if !strings.Contains(body, "hnsw-ef-construction") || !strings.Contains(body, "100") {
		t.Fatalf("VINFO should report ef-construction=100: %s", body)
	}

	links := db.Exec(nil, utils.ToCmdLine("VLINKS", "vh", "a"))
	mr, ok := links.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) == 0 {
		t.Fatalf("VLINKS: %T %s", links, links.ToBytes())
	}
	layer0, ok := mr.Replies[0].(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("layer0 type %T", mr.Replies[0])
	}
	if len(layer0.Args) == 0 {
		t.Fatalf("expected non-empty HNSW neighbors for a: %s", links.ToBytes())
	}
}

func TestHNSWVSimEFAndTruth(t *testing.T) {
	db := makeTestDB()
	// Build a set large enough to exercise the HNSW (not brute) path.
	for i := 0; i < 80; i++ {
		x := strconv.FormatFloat(float64(i)/80.0, 'f', 6, 64)
		y := strconv.FormatFloat(1.0-float64(i)/80.0, 'f', 6, 64)
		r := db.Exec(nil, utils.ToCmdLine(
			"VADD", "vs", "VALUES", "2", x, y, "ELE", "e"+strconv.Itoa(i), "M", "16", "EF", "50",
		))
		if protocol.IsErrorReply(r) {
			t.Fatalf("VADD e%d: %s", i, r.ToBytes())
		}
	}
	approx := db.Exec(nil, utils.ToCmdLine(
		"VSIM", "vs", "VALUES", "2", "1", "0", "COUNT", "5", "EF", "80",
	))
	exact := db.Exec(nil, utils.ToCmdLine(
		"VSIM", "vs", "VALUES", "2", "1", "0", "COUNT", "5", "TRUTH",
	))
	am, ok := approx.(*protocol.MultiBulkReply)
	if !ok || len(am.Args) == 0 {
		t.Fatalf("approx VSIM: %T %s", approx, approx.ToBytes())
	}
	em, ok := exact.(*protocol.MultiBulkReply)
	if !ok || len(em.Args) == 0 {
		t.Fatalf("truth VSIM: %T %s", exact, exact.ToBytes())
	}
	// Top hit under cosine for query (1,0) should be the largest x component → e79-ish.
	if string(am.Args[0]) == "" || string(em.Args[0]) == "" {
		t.Fatalf("empty top hits approx=%s exact=%s", am.Args[0], em.Args[0])
	}
}

func TestHNSWVSimEpsilon(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "ve", "VALUES", "2", "1", "0", "ELE", "near",
	)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "ve", "VALUES", "2", "0", "1", "ELE", "far",
	)), 1)

	all := db.Exec(nil, utils.ToCmdLine(
		"VSIM", "ve", "VALUES", "2", "1", "0", "COUNT", "10", "TRUTH",
	))
	am, ok := all.(*protocol.MultiBulkReply)
	if !ok || len(am.Args) != 2 {
		t.Fatalf("without EPSILON want 2 hits: %T %s", all, all.ToBytes())
	}

	tight := db.Exec(nil, utils.ToCmdLine(
		"VSIM", "ve", "VALUES", "2", "1", "0", "COUNT", "10", "EPSILON", "0.5", "TRUTH",
	))
	tm, ok := tight.(*protocol.MultiBulkReply)
	if !ok || len(tm.Args) != 1 || string(tm.Args[0]) != "near" {
		t.Fatalf("EPSILON 0.5 should keep only near: %T %s", tight, tight.ToBytes())
	}

	bad := db.Exec(nil, utils.ToCmdLine(
		"VSIM", "ve", "VALUES", "2", "1", "0", "EPSILON", "1.5",
	))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("EPSILON>1 should error: %s", bad.ToBytes())
	}
}
