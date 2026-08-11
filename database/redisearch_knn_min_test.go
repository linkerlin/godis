package database

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// f32le encodes float32 little-endian blob for FT VECTOR / KNN PARAMS.
func f32leKNN(vals ...float32) string {
	b := make([]byte, 4*len(vals))
	for i, v := range vals {
		binary.LittleEndian.PutUint32(b[i*4:], math.Float32bits(v))
	}
	return string(b)
}

// TestFTKNNMinimalPath is the acceptance contract for the FT.SEARCH KNN subset:
// VECTOR field + *=>[KNN K @field $param] + DIALECT 2 + PARAMS blob.
func TestFTKNNMinimalPath(t *testing.T) {
	db := makeTestDB()
	create := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "knnmin", "ON", "HASH", "PREFIX", "1", "km:",
		"SCHEMA", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
	))
	if protocol.IsErrorReply(create) {
		t.Fatalf("FT.CREATE: %s", create.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "km:a", "vec", f32leKNN(0.1, 0)))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "km:b", "vec", f32leKNN(5, 5)))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "km:c", "vec", f32leKNN(0.2, 0.1)))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "knnmin", "*=>[KNN 2 @vec $q AS dist]",
		"PARAMS", "2", "q", f32leKNN(0, 0),
		"DIALECT", "2", "NOCONTENT",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("KNN search: %s", r.ToBytes())
	}
	s := string(r.ToBytes())
	// Nearest two should be km:a then km:c (not km:b).
	if !strings.Contains(s, "km:a") || !strings.Contains(s, "km:c") {
		t.Fatalf("want km:a and km:c in top-2, got %s", s)
	}
	if strings.Index(s, "km:a") > strings.Index(s, "km:c") {
		t.Fatalf("want km:a before km:c, got %s", s)
	}
	if strings.Contains(s, "km:b") {
		t.Fatalf("far doc km:b should not be in K=2, got %s", s)
	}
}

func TestFTKNNMinimalHybridPrefilter(t *testing.T) {
	db := makeTestDB()
	create := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "knnhy", "ON", "HASH", "PREFIX", "1", "kh:",
		"SCHEMA", "price", "NUMERIC", "vec", "VECTOR", "FLAT", "6",
		"TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
	))
	if protocol.IsErrorReply(create) {
		t.Fatalf("FT.CREATE: %s", create.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "kh:cheap", "price", "5", "vec", f32leKNN(0.1, 0)))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "kh:farcheap", "price", "5", "vec", f32leKNN(9, 9)))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "kh:neardear", "price", "100", "vec", f32leKNN(0, 0)))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "knnhy", "@price:[0 10]=>[KNN 1 @vec $q]",
		"PARAMS", "2", "q", f32leKNN(0, 0),
		"DIALECT", "2", "NOCONTENT",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("hybrid KNN: %s", r.ToBytes())
	}
	s := string(r.ToBytes())
	if !strings.Contains(s, "kh:cheap") {
		t.Fatalf("prefilter should pick kh:cheap, got %s", s)
	}
	if strings.Contains(s, "kh:neardear") {
		t.Fatalf("price filter should exclude kh:neardear, got %s", s)
	}
}
