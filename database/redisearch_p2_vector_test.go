package database

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// f32le encodes a float32 slice as a little-endian FLOAT32 blob (the wire
// format Redis Vector Search expects for HSET of a VECTOR field).
func f32le(xs ...float32) []byte {
	buf := make([]byte, 4*len(xs))
	for i, x := range xs {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(x))
	}
	return buf
}

// TestP2aVectorSchemaParsing verifies FT.CREATE parses VECTOR FLAT/HNSW with
// all required attributes (TYPE, DIM, DISTANCE_METRIC) and rejects bad ones.
func TestP2aVectorSchemaParsing(t *testing.T) {
	db := makeTestDB()
	// Valid FLAT.
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p2flat", "SCHEMA", "vec", "VECTOR", "FLAT", "6",
		"TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "L2",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("FLAT create: %s", r.ToBytes())
	}
	// Valid HNSW with optional M / EF_* (6 pairs = 12 attribute tokens).
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p2hnsw", "SCHEMA", "vec", "VECTOR", "HNSW", "12",
		"TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "COSINE",
		"M", "8", "EF_CONSTRUCTION", "64", "EF_RUNTIME", "20",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("HNSW create: %s", r.ToBytes())
	}
	// Missing DIM -> error.
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p2bad", "SCHEMA", "vec", "VECTOR", "FLAT", "4",
		"TYPE", "FLOAT32", "DISTANCE_METRIC", "L2",
	)); !protocol.IsErrorReply(r) {
		t.Fatalf("missing DIM should error: %s", r.ToBytes())
	}
	// Bad metric.
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p2bad2", "SCHEMA", "vec", "VECTOR", "FLAT", "6",
		"TYPE", "FLOAT32", "DIM", "3", "DISTANCE_METRIC", "MANHATTAN",
	)); !protocol.IsErrorReply(r) {
		t.Fatalf("bad metric should error: %s", r.ToBytes())
	}

	// Verify the config was stored.
	searchEnginesMu.RLock()
	engine := searchEngines["p2flat"]
	searchEnginesMu.RUnlock()
	if engine == nil {
		t.Fatal("engine missing")
	}
	vi := engine.VectorIndex("vec")
	if vi == nil {
		t.Fatal("vector index missing for field 'vec'")
	}
	if vi.Config().Dim != 3 || vi.Config().DistanceMetric != "L2" || vi.Config().Algorithm != "FLAT" {
		t.Fatalf("vector config wrong: %+v", vi.Config())
	}
}

// TestP2bHSetAutoIndexesVector verifies HSET on an ON HASH index stores the
// vector blob into the field's vector index (auto-indexing path).
func TestP2bHSetAutoIndexesVector(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p2b", "ON", "HASH", "PREFIX", "1", "p2b:",
		"SCHEMA", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
	)), "OK")

	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p2b:1", "vec", string(f32le(1.0, 0.0)))); protocol.IsErrorReply(r) {
		t.Fatalf("hset: %s", r.ToBytes())
	}
	searchEnginesMu.RLock()
	engine := searchEngines["p2b"]
	searchEnginesMu.RUnlock()
	vi := engine.VectorIndex("vec")
	if vi.Len() != 1 {
		t.Fatalf("vector index should have 1 vector, got %d", vi.Len())
	}
}

// TestP2eKNNFlatSearch verifies *=>[KNN K @vec $blob] returns nearest K docs by
// L2 distance, ordered ascending, with the AS field carrying the distance.
func TestP2eKNNFlatSearch(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p2e", "ON", "HASH", "PREFIX", "1", "p2e:",
		"SCHEMA", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
	)), "OK")
	// Three docs at increasing L2 distance from the query (0,0).
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p2e:near", "vec", string(f32le(0.1, 0.0)))); protocol.IsErrorReply(r) {
		t.Fatalf("hset near: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p2e:mid", "vec", string(f32le(1.0, 0.0)))); protocol.IsErrorReply(r) {
		t.Fatalf("hset mid: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p2e:far", "vec", string(f32le(5.0, 5.0)))); protocol.IsErrorReply(r) {
		t.Fatalf("hset far: %s", r.ToBytes())
	}

	// KNN 2 nearest to (0,0): near (0.1) then mid (1.0).
	qblob := string(f32le(0.0, 0.0))
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "p2e", "*=>[KNN 2 @vec $blob AS dist]",
		"PARAMS", "2", "blob", qblob, "DIALECT", "2", "RETURN", "1", "dist",
	))
	mr := ftSearchMultiRaw(r)
	// First element is total count (2).
	total, ok := mr.Replies[0].(*protocol.IntReply)
	if !ok || total.Code != 2 {
		t.Fatalf("expected total=2, got %s", r.ToBytes())
	}
	// First hit id should be p2e:near.
	id, ok := mr.Replies[1].(*protocol.BulkReply)
	if !ok || string(id.Arg) != "p2e:near" {
		t.Fatalf("nearest should be p2e:near, got %s", r.ToBytes())
	}
}

// TestP2eKNNRequiresDialect2 verifies KNN without DIALECT 2 errors.
func TestP2eKNNRequiresDialect2(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p2ed", "ON", "HASH", "PREFIX", "1", "p2ed:",
		"SCHEMA", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p2ed:1", "vec", string(f32le(1.0, 1.0)))); protocol.IsErrorReply(r) {
		t.Fatalf("hset: %s", r.ToBytes())
	}
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "p2ed", "*=>[KNN 1 @vec $blob]",
		"PARAMS", "2", "blob", string(f32le(0.0, 0.0)),
	))
	if !protocol.IsErrorReply(r) || !contains(string(r.ToBytes()), "DIALECT 2") {
		t.Fatalf("KNN without DIALECT 2 should error: %s", r.ToBytes())
	}
}

// TestP2gHybridKNNPrefilter verifies "@filter=>[KNN ...]" restricts the KNN
// candidate set to docs matching the filter.
func TestP2gHybridKNNPrefilter(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p2g", "ON", "HASH", "PREFIX", "1", "p2g:",
		"SCHEMA",
		"price", "NUMERIC",
		"vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "1", "DISTANCE_METRIC", "L2",
	)), "OK")
	// Three docs; only price <= 10 should be candidates.
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p2g:cheap", "price", "5", "vec", string(f32le(1.0)))); protocol.IsErrorReply(r) {
		t.Fatalf("hset cheap: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p2g:mid", "price", "8", "vec", string(f32le(2.0)))); protocol.IsErrorReply(r) {
		t.Fatalf("hset mid: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p2g:pricey", "price", "100", "vec", string(f32le(0.5)))); protocol.IsErrorReply(r) {
		t.Fatalf("hset pricey: %s", r.ToBytes())
	}

	// Query vector 0.0; without filter the nearest would be pricey (0.5). With
	// the price filter [0 10], pricey is excluded and the nearest is cheap (1.0).
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "p2g", "@price:[0 10]=>[KNN 1 @vec $blob]",
		"PARAMS", "2", "blob", string(f32le(0.0)), "DIALECT", "2", "NOCONTENT",
	))
	mr := ftSearchMultiRaw(r)
	total, _ := mr.Replies[0].(*protocol.IntReply)
	if total == nil || total.Code != 1 {
		t.Fatalf("hybrid KNN should return 1 hit, got %s", r.ToBytes())
	}
	id, _ := mr.Replies[1].(*protocol.BulkReply)
	if string(id.Arg) != "p2g:cheap" {
		t.Fatalf("filtered KNN nearest should be p2g:cheap, got %s", r.ToBytes())
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// TestP2fHNSWKNN verifies an HNSW-declared VECTOR field returns the same KNN
// ordering as FLAT (the graph path must agree with brute force on top-K).
func TestP2fHNSWKNN(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p2f", "ON", "HASH", "PREFIX", "1", "p2f:",
		"SCHEMA", "vec", "VECTOR", "HNSW", "12",
		"TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
		"M", "8", "EF_CONSTRUCTION", "64", "EF_RUNTIME", "50",
	)), "OK")
	// Seed enough docs that HNSW (len>1) takes the graph path.
	pts := [][2]float32{{0.1, 0}, {1, 0}, {5, 5}, {0.2, 0.1}, {2, 2}, {-1, -1}}
	for i, p := range pts {
		if r := db.Exec(nil, utils.ToCmdLine("HSET", "p2f:d"+itoa(i), "vec", string(f32le(p[0], p[1])))); protocol.IsErrorReply(r) {
			t.Fatalf("hset d%d: %s", i, r.ToBytes())
		}
	}
	// KNN 2 nearest to (0,0): d0 (0.1) then d3 (~0.22).
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "p2f", "*=>[KNN 2 @vec $blob]",
		"PARAMS", "2", "blob", string(f32le(0, 0)), "DIALECT", "2", "NOCONTENT",
	))
	mr := ftSearchMultiRaw(r)
	total, _ := mr.Replies[0].(*protocol.IntReply)
	if total == nil || total.Code != 2 {
		t.Fatalf("HNSW KNN should return 2 hits, got %s", r.ToBytes())
	}
	id, _ := mr.Replies[1].(*protocol.BulkReply)
	if string(id.Arg) != "p2f:d0" {
		t.Fatalf("HNSW nearest should be p2f:d0, got %s", r.ToBytes())
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// TestP2fSVSVAMANA verifies the 8.2+ SVS-VAMANA algorithm parses (with an
// accepted-and-ignored COMPRESSION mode, matching OSS Redis's scalar fallback)
// and runs KNN on the graph backend.
func TestP2fSVSVAMANA(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p2v", "ON", "HASH", "PREFIX", "1", "p2v:",
		"SCHEMA", "vec", "VECTOR", "SVS-VAMANA", "8",
		"TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
		"COMPRESSION", "LVQ8",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p2v:1", "vec", string(f32le(1.0, 0.0)))); protocol.IsErrorReply(r) {
		t.Fatalf("hset: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p2v:2", "vec", string(f32le(5.0, 5.0)))); protocol.IsErrorReply(r) {
		t.Fatalf("hset 2: %s", r.ToBytes())
	}
	// KNN nearest to (0,0) is p2v:1 (distance 1) over p2v:2 (distance ~7.07).
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "p2v", "*=>[KNN 1 @vec $blob]", "NOCONTENT",
		"PARAMS", "2", "blob", string(f32le(0, 0)), "DIALECT", "2",
	))
	mr := ftSearchMultiRaw(r)
	if mr == nil {
		t.Fatalf("VAMANA KNN reply shape: %T %s", r, r.ToBytes())
	}
	id, _ := mr.Replies[1].(*protocol.BulkReply)
	if id == nil || string(id.Arg) != "p2v:1" {
		t.Fatalf("VAMANA KNN nearest should be p2v:1, got %s", r.ToBytes())
	}
}
