package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// KNN / DIALECT boundary tests: error paths only (not full KNN dialect / BM25).

func TestFTDialectInvalidValues(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "dialbd", "ON", "HASH", "PREFIX", "1", "db:", "SCHEMA", "t", "TEXT",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	for _, bad := range []string{"0", "5", "foo"} {
		r := db.Exec(nil, utils.ToCmdLine(
			"FT.SEARCH", "dialbd", "*", "DIALECT", bad,
		))
		if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "Invalid DIALECT") {
			t.Fatalf("DIALECT %s want Invalid DIALECT ERR, got %s", bad, r.ToBytes())
		}
	}
	// DIALECT 4 is in the accepted range (subset semantics only — not full dialect 4).
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "dialbd", "*", "DIALECT", "4", "NOCONTENT"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("DIALECT 4 should be accepted as value, got %s", r.ToBytes())
	}
}

func TestFTKNNErrorPaths(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "knnbd", "ON", "HASH", "PREFIX", "1", "kb:",
		"SCHEMA", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
		"title", "TEXT",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "kb:a", "vec", f32leKNN(0, 0), "title", "x"))

	// Missing PARAMS binding for $q.
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "knnbd", "*=>[KNN 1 @vec $q]",
		"PARAMS", "2", "other", f32leKNN(0, 0), "DIALECT", "2",
	))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "No such parameter") {
		t.Fatalf("missing param: %s", r.ToBytes())
	}

	// Non-vector field name.
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "knnbd", "*=>[KNN 1 @title $q]",
		"PARAMS", "2", "q", f32leKNN(0, 0), "DIALECT", "2",
	))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "Vector field") {
		t.Fatalf("non-vector field: %s", r.ToBytes())
	}

	// Dim mismatch (2-dim index, 1 float provided).
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "knnbd", "*=>[KNN 1 @vec $q]",
		"PARAMS", "2", "q", f32leKNN(0), "DIALECT", "2",
	))
	if !protocol.IsErrorReply(r) || !strings.Contains(strings.ToLower(string(r.ToBytes())), "expected") {
		t.Fatalf("dim mismatch: %s", r.ToBytes())
	}

	// Malformed KNN clause (syntax).
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "knnbd", "*=>[KNN 1 @vec $q",
		"PARAMS", "2", "q", f32leKNN(0, 0), "DIALECT", "2",
	))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "Invalid KNN") {
		t.Fatalf("malformed clause: %s", r.ToBytes())
	}

	// KNN under DIALECT 1 (PARAMS alone also fails dialect gate before/around KNN).
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "knnbd", "*=>[KNN 1 @vec $q]",
		"PARAMS", "2", "q", f32leKNN(0, 0), "DIALECT", "1",
	))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("DIALECT 1+KNN want ERR, got %s", r.ToBytes())
	}
	msg := string(r.ToBytes())
	if !strings.Contains(msg, "DIALECT") && !strings.Contains(msg, "PARAMS") {
		t.Fatalf("DIALECT 1+KNN want dialect/params ERR, got %s", msg)
	}

	// Invalid HYBRID_POLICY
	r = db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "knnbd", "*=>[KNN 1 @vec $q HYBRID_POLICY NOPE]",
		"PARAMS", "2", "q", f32leKNN(0, 0), "DIALECT", "2",
	))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "HYBRID_POLICY") {
		t.Fatalf("bad HYBRID_POLICY: %s", r.ToBytes())
	}
}

// TestFTKNNYieldDistanceAsAttr verifies =>{$YIELD_DISTANCE_AS: dist} surfaces
// the distance field equivalently to AS inside the KNN bracket.
func TestFTKNNYieldDistanceAsAttr(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "knnya", "ON", "HASH", "PREFIX", "1", "ya:",
		"SCHEMA", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "ya:a", "vec", f32leKNN(0.1, 0)))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "ya:b", "vec", f32leKNN(5, 5)))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "knnya", "*=>[KNN 1 @vec $q]=>{$YIELD_DISTANCE_AS: dist}",
		"PARAMS", "2", "q", f32leKNN(0, 0), "DIALECT", "2", "RETURN", "1", "dist",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("YIELD_DISTANCE_AS search: %s", r.ToBytes())
	}
	s := string(r.ToBytes())
	if !strings.Contains(s, "ya:a") || !strings.Contains(s, "dist") {
		t.Fatalf("want ya:a + dist field, got %s", s)
	}
}

// TestFTKNNEmptyPrefilter returns zero hits when the base filter matches nothing
// (even if unfiltered KNN would find neighbors).
func TestFTKNNEmptyPrefilter(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "knnef", "ON", "HASH", "PREFIX", "1", "ef:",
		"SCHEMA", "price", "NUMERIC",
		"vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "1", "DISTANCE_METRIC", "L2",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "ef:1", "price", "5", "vec", f32leKNN(0)))
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "knnef", "@price:[100 200]=>[KNN 1 @vec $q HYBRID_POLICY ADHOC_BF]",
		"PARAMS", "2", "q", f32leKNN(0), "DIALECT", "2", "NOCONTENT",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("empty prefilter: %s", r.ToBytes())
	}
	if !searchTotalIs(t, r, 0) {
		t.Fatalf("empty prefilter want total 0, got %s", r.ToBytes())
	}
}
