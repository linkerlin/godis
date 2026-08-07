package database

import (
	"encoding/binary"
	"strings"
	"math"
	"testing"

	"github.com/linkerlin/godis/datastruct/hll"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestHLLStoredAsString verifies PFADD stores the HLL as a readable string
// (Redis semantics): GET returns the dense bytes, TYPE is string, and the
// encoding has the HYLL header.
func TestHLLStoredAsString(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "h", "a", "b", "c")), 1)

	// TYPE is string.
	r := db.Exec(nil, utils.ToCmdLine("TYPE", "h"))
	if s, ok := r.(*protocol.StatusReply); !ok || s.Status != "string" {
		t.Fatalf("TYPE h should be string, got %s", r.ToBytes())
	}
	// GET reads the dense bytes with a HYLL header.
	r = db.Exec(nil, utils.ToCmdLine("GET", "h"))
	b, ok := r.(*protocol.BulkReply)
	if !ok || len(b.Arg) != hll.TotalSize {
		t.Fatalf("GET h should return %d HLL bytes, got %s", hll.TotalSize, r.ToBytes())
	}
	if string(b.Arg[:4]) != "HYLL" {
		t.Fatalf("HLL header should be HYLL, got %q", b.Arg[:4])
	}
	// OBJECT ENCODING reports "hyperloglog" (Redis reports the type name, not raw).
	r = db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "h"))
	if b, ok := r.(*protocol.BulkReply); !ok || string(b.Arg) != "hyperloglog" {
		t.Fatalf("OBJECT ENCODING h should be hyperloglog, got %s", r.ToBytes())
	}
}

// TestHLLCountAccuracy verifies PFCOUNT approximates the true cardinality with
// the expected error (< 2% for 10k elements, well within HLL guarantees).
func TestHLLCountAccuracy(t *testing.T) {
	db := makeTestDB()
	const n = 10000
	for i := 0; i < n; i++ {
		db.Exec(nil, utils.ToCmdLine("PFADD", "acc", utils.RandString(8)))
	}
	r := db.Exec(nil, utils.ToCmdLine("PFCOUNT", "acc"))
	ir, ok := r.(*protocol.IntReply)
	if !ok {
		t.Fatalf("PFCOUNT shape: %T %s", r, r.ToBytes())
	}
	got := float64(ir.Code)
	errPct := math.Abs(got-float64(n)) / float64(n)
	if errPct > 0.02 {
		t.Fatalf("PFCOUNT accuracy: got %.0f want ~%d (err %.4f%%)", got, n, errPct*100)
	}
}

// TestHLLWrongType verifies PFADD/PFCOUNT on a non-HLL string error WRONGTYPE.
func TestHLLWrongType(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "s", "notanHLL")), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("PFADD", "s", "x")); !protocol.IsErrorReply(r) || !isWrongType(r) {
		t.Fatalf("PFADD on plain string should WRONGTYPE: %s", r.ToBytes())
	}
	// A string that happens to start with HYLL but is too short is also invalid.
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "s2", "HYLLshort")), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("PFCOUNT", "s2")); !protocol.IsErrorReply(r) {
		t.Fatalf("PFCOUNT on short HYLL-ish string should error: %s", r.ToBytes())
	}
}

// TestHLLMerge verifies PFMERGE unions registers (PFCOUNT of merged >= each).
func TestHLLMerge(t *testing.T) {
	db := makeTestDB()
	for i := 0; i < 500; i++ {
		db.Exec(nil, utils.ToCmdLine("PFADD", "m1", utils.RandString(6)))
		db.Exec(nil, utils.ToCmdLine("PFADD", "m2", utils.RandString(6)))
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "md", "m1", "m2")), "OK")
	merged := db.Exec(nil, utils.ToCmdLine("PFCOUNT", "md"))
	union := db.Exec(nil, utils.ToCmdLine("PFCOUNT", "m1", "m2"))
	mi, _ := merged.(*protocol.IntReply)
	ui, _ := union.(*protocol.IntReply)
	if mi == nil || ui == nil {
		t.Fatalf("merge/count shapes: %s %s", merged.ToBytes(), union.ToBytes())
	}
	if mi.Code < ui.Code {
		t.Fatalf("PFMERGE result should be >= union count: merged=%d union=%d", mi.Code, ui.Code)
	}
}

// TestHLLEncodeDecodeRoundtrip verifies dense encode/decode round-trips the
// register values (byte-level Redis compatibility path).
func TestHLLEncodeDecodeRoundtrip(t *testing.T) {
	h := hll.New()
	for i := 0; i < 1000; i++ {
		h.Add([]byte("elem" + itoa(i)))
	}
	encoded := h.Encode()
	// Header + expected size.
	if len(encoded) != hll.TotalSize {
		t.Fatalf("encoded size = %d, want %d", len(encoded), hll.TotalSize)
	}
	decoded, err := hll.Decode(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Count() != h.Count() {
		t.Fatalf("roundtrip count mismatch: %d vs %d", decoded.Count(), h.Count())
	}
	// Cached cardinality field (bytes 6-13) is 0 (invalidated), like Redis.
	if got := binary.LittleEndian.Uint64(encoded[6:]); got != 0 {
		t.Fatalf("cached cardinality should be 0, got %d", got)
	}
}

func isWrongType(r redis.Reply) bool {
	return strings.HasPrefix(string(r.ToBytes()), "-WRONGTYPE")
}
