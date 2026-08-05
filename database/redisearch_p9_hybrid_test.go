package database

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// f32 encodes a FLOAT32 vector blob (little-endian).
func f32(xs ...float32) []byte {
	buf := make([]byte, 4*len(xs))
	for i, x := range xs {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(x))
	}
	return buf
}

// TestP9FTHybridRRF verifies FT.HYBRID runs a text + vector search and fuses
// them via RRF. A doc matching both the text query and being near the query
// vector should rank above docs matching only one side.
func TestP9FTHybridRRF(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p9h", "ON", "HASH", "PREFIX", "1", "p9h:", "SCHEMA",
		"t", "TEXT", "NOSTEM",
		"vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
	)), "OK")
	// both: matches text "golang" AND nearest vector.
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p9h:both", "t", "golang", "vec", string(f32(0.1, 0.1)))); protocol.IsErrorReply(r) {
		t.Fatalf("hset both: %s", r.ToBytes())
	}
	// text_only: matches text, far vector.
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p9h:to", "t", "golang", "vec", string(f32(9, 9)))); protocol.IsErrorReply(r) {
		t.Fatalf("hset to: %s", r.ToBytes())
	}
	// vec_only: no text match, near vector.
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p9h:vo", "t", "other", "vec", string(f32(0.2, 0.2)))); protocol.IsErrorReply(r) {
		t.Fatalf("hset vo: %s", r.ToBytes())
	}

	// HYBRID: text "golang" + vector near (0,0), RRF, top 3.
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.HYBRID", "p9h",
		"SEARCH", "golang",
		"VSIM", "@vec", string(f32(0, 0)), "KNN", "1", "K", "3",
		"COMBINE", "RRF", "3",
		"LIMIT", "0", "3",
	))
	mr := ftSearchMultiRaw(r)
	if mr == nil {
		t.Fatalf("hybrid reply shape: %T %s", r, r.ToBytes())
	}
	total, _ := mr.Replies[0].(*protocol.IntReply)
	if total == nil || total.Code < 2 {
		t.Fatalf("hybrid should fuse >=2 docs (both + text_only + vec_only), got total %s", r.ToBytes())
	}
	// First result should be "both" (in both lists).
	id, _ := mr.Replies[1].(*protocol.BulkReply)
	if id == nil || string(id.Arg) != "p9h:both" {
		t.Fatalf("RRF top should be p9h:both (matches text + near vec), got %s", r.ToBytes())
	}
}

// TestP9FTHybridLinear verifies the LINEAR combine policy runs without error
// and produces a fused result.
func TestP9FTHybridLinear(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p9l", "ON", "HASH", "PREFIX", "1", "p9l:", "SCHEMA",
		"t", "TEXT", "NOSTEM",
		"vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "1", "DISTANCE_METRIC", "L2",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p9l:1", "t", "hello", "vec", string(f32(1.0)))); protocol.IsErrorReply(r) {
		t.Fatalf("hset: %s", r.ToBytes())
	}
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.HYBRID", "p9l",
		"SEARCH", "hello",
		"VSIM", "@vec", string(f32(0.0)), "KNN", "1", "K", "1",
		"COMBINE", "LINEAR", "1", "ALPHA", "0.5", "BETA", "0.5",
	))
	mr := ftSearchMultiRaw(r)
	if mr == nil {
		t.Fatalf("hybrid linear reply shape: %T %s", r, r.ToBytes())
	}
	total, _ := mr.Replies[0].(*protocol.IntReply)
	if total == nil || total.Code < 1 {
		t.Fatalf("linear hybrid should return >=1 doc, got %s", r.ToBytes())
	}
}

// TestP9FTHybridVectorOnly verifies that a "*" SEARCH (text side empty) still
// returns the vector KNN results via the hybrid path.
func TestP9FTHybridVectorOnly(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p9vo", "ON", "HASH", "PREFIX", "1", "p9vo:", "SCHEMA",
		"vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "1", "DISTANCE_METRIC", "L2",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p9vo:1", "vec", string(f32(1.0)))); protocol.IsErrorReply(r) {
		t.Fatalf("hset: %s", r.ToBytes())
	}
	// SEARCH "*" -> text side empty; vector KNN drives the result.
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.HYBRID", "p9vo",
		"SEARCH", "*",
		"VSIM", "@vec", string(f32(0.0)), "KNN", "1", "K", "1",
		"COMBINE", "RRF", "1",
	))
	mr := ftSearchMultiRaw(r)
	if mr == nil {
		t.Fatalf("vector-only hybrid reply shape: %T %s", r, r.ToBytes())
	}
	total, _ := mr.Replies[0].(*protocol.IntReply)
	if total == nil || total.Code != 1 {
		t.Fatalf("vector-only hybrid should return 1 doc, got %s", r.ToBytes())
	}
}
