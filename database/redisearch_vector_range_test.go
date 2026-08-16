package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// TestFTVectorRangeMinimalPath: @vec:[VECTOR_RANGE r $q] + DIALECT 2 + PARAMS.
func TestFTVectorRangeMinimalPath(t *testing.T) {
	db := makeTestDB()
	create := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "vridx", "ON", "HASH", "PREFIX", "1", "vr:",
		"SCHEMA", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
	))
	if protocol.IsErrorReply(create) {
		t.Fatalf("FT.CREATE: %s", create.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "vr:near", "vec", f32leKNN(0.1, 0)))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "vr:mid", "vec", f32leKNN(0.5, 0)))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "vr:far", "vec", f32leKNN(5, 5)))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "vridx", "@vec:[VECTOR_RANGE 1 $q]",
		"PARAMS", "2", "q", f32leKNN(0, 0),
		"DIALECT", "2", "NOCONTENT",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("VECTOR_RANGE: %s", r.ToBytes())
	}
	s := string(r.ToBytes())
	if !strings.Contains(s, "vr:near") || !strings.Contains(s, "vr:mid") {
		t.Fatalf("want near+mid within radius 1, got %s", s)
	}
	if strings.Contains(s, "vr:far") {
		t.Fatalf("far doc should be outside radius, got %s", s)
	}
}

func TestFTVectorRangeYieldAndDialectGate(t *testing.T) {
	db := makeTestDB()
	create := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "vry", "ON", "HASH", "PREFIX", "1", "vy:",
		"SCHEMA", "vec", "VECTOR", "FLAT", "6", "TYPE", "FLOAT32", "DIM", "2", "DISTANCE_METRIC", "L2",
	))
	if protocol.IsErrorReply(create) {
		t.Fatalf("FT.CREATE: %s", create.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "vy:a", "vec", f32leKNN(0, 0)))

	d1 := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "vry", "@vec:[VECTOR_RANGE 1 $q]",
		"PARAMS", "2", "q", f32leKNN(0, 0),
		"DIALECT", "1",
	))
	if !protocol.IsErrorReply(d1) || !strings.Contains(string(d1.ToBytes()), "DIALECT 2") {
		t.Fatalf("want DIALECT 2 gate, got %s", d1.ToBytes())
	}

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "vry", "@vec:[VECTOR_RANGE 1 $q]=>{$YIELD_DISTANCE_AS: dist}",
		"PARAMS", "2", "q", f32leKNN(0, 0),
		"DIALECT", "2", "RETURN", "1", "dist",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("YIELD VECTOR_RANGE: %s", r.ToBytes())
	}
	s := string(r.ToBytes())
	if !strings.Contains(s, "dist") || !strings.Contains(s, "vy:a") {
		t.Fatalf("want dist field, got %s", s)
	}

	bad := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "vry", "@vec:[VECTOR_RANGE 1 $q]=>{$EPSILON:}",
		"PARAMS", "2", "q", f32leKNN(0, 0),
		"DIALECT", "2",
	))
	if !protocol.IsErrorReply(bad) || !strings.Contains(string(bad.ToBytes()), "EPSILON") {
		t.Fatalf("want EPSILON ERR, got %s", bad.ToBytes())
	}
}
