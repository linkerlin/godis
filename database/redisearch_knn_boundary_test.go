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
}
