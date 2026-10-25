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
		"VADD", "vh", "VALUES", "2", "1", "0", "a", "M", "16", "EF", "100",
	)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "vh", "VALUES", "2", "0.9", "0.1", "b",
	)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "vh", "VALUES", "2", "0", "1", "c",
	)), 1)

	info := db.Exec(nil, utils.ToCmdLine("VINFO", "vh"))
	body := string(info.ToBytes())
	if !strings.Contains(body, "hnsw-m") || !strings.Contains(body, "16") {
		t.Fatalf("VINFO should report hnsw-m=16: %s", body)
	}
	// Real 8.10 field set (no hnsw-ef-construction field).
	for _, f := range []string{"quant-type", "vector-dim", "projection-input-dim", "size", "max-level", "attributes-count", "vset-uid", "hnsw-max-node-uid"} {
		if !strings.Contains(body, f) {
			t.Fatalf("VINFO missing %s: %s", f, body)
		}
	}

	// Real 8.10 replies with a FLAT neighbor list.
	links := db.Exec(nil, utils.ToCmdLine("VLINKS", "vh", "a"))
	mb, ok := links.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("VLINKS should be a flat array: %T %s", links, links.ToBytes())
	}
	if len(mb.Args) == 0 {
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
			"VADD", "vs", "VALUES", "2", x, y, "e"+strconv.Itoa(i), "M", "16", "EF", "50",
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
		"VADD", "ve", "VALUES", "2", "1", "0", "near",
	)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "ve", "VALUES", "2", "0", "1", "far",
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
