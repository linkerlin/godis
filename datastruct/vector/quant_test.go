package vector

import (
	"math"
	"testing"
)

func TestQuantizeQ8RoundTrip(t *testing.T) {
	in := []float32{1.262185, 1.958231}
	codes, r := QuantizeQ8(in)
	if r <= 0 {
		t.Fatalf("range=%v", r)
	}
	got := DequantizeQ8(codes, r)
	for i := range in {
		if math.Abs(float64(got[i]-in[i])) > 0.02 {
			t.Fatalf("comp %d: got %v want ~%v (codes=%v range=%v)", i, got[i], in[i], codes, r)
		}
	}
}

func TestVectorSetQ8Storage(t *testing.T) {
	vs := NewVectorSet()
	if !vs.SetQuantMode(QuantQ8) {
		t.Fatal("SetQuantMode Q8")
	}
	if !vs.Add("a", NewVector([]float32{0.5, -0.5, 1.0}), nil) {
		t.Fatal("Add")
	}
	item, ok := vs.Get("a")
	if !ok || len(item.Q8) != 3 {
		t.Fatalf("want Q8 codes, got %#v", item)
	}
	if vs.QuantMode().QuantTypeName() != "int8" {
		t.Fatalf("quant=%s", vs.QuantMode().QuantTypeName())
	}
	// Format lock: NOQUANT rejected after Q8 insert.
	if vs.SetQuantMode(QuantF32) {
		t.Fatal("expected quant mismatch")
	}
}

func TestQuantizeBINSignBits(t *testing.T) {
	// [1.2, 0.3, -0.5, -1.0] → bits 1,1,0,0
	bits := QuantizeBIN([]float32{1.2, 0.3, -0.5, -1.0})
	if len(bits) != 1 || bits[0]&0x0F != 0x03 {
		t.Fatalf("want low nibble 0011, got %#x", bits)
	}
	got := DequantizeBIN(bits, 4)
	want := []float32{1, 1, -1, -1}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dim %d: got %v want %v", i, got, want)
		}
	}
}

func TestHammingDistanceBits(t *testing.T) {
	// a: ++--  (bits 1,1,0,0)  b: +-+- (bits 1,0,1,0) → differ dim1+dim2 → h=2
	a := QuantizeBIN([]float32{1, 1, -1, -1})
	b := QuantizeBIN([]float32{1, -1, 1, -1})
	h := HammingDistanceBits(a, b, 4)
	if h != 2 {
		t.Fatalf("hamming=%d want 2", h)
	}
	cos := BINCosineFromHamming(h, 4)
	if math.Abs(float64(cos-0)) > 1e-6 {
		t.Fatalf("cos=%v want 0", cos)
	}
	// Partial dim: only first 3 bits (a vs b: dims 0 same, 1+2 differ → h=2)
	if got := HammingDistanceBits(a, b, 3); got != 2 {
		t.Fatalf("dim3 hamming=%d want 2", got)
	}
	if HammingDistanceBits(a, a, 4) != 0 {
		t.Fatal("identical bits")
	}
}

func TestQ8CosineFromCodesMatchesDequant(t *testing.T) {
	a := []float32{0.5, -0.2, 1.0, 0.1}
	b := []float32{0.4, -0.3, 0.9, 0.0}
	ca, ra := QuantizeQ8(a)
	cb, rb := QuantizeQ8(b)
	got := Q8CosineFromCodes(ca, cb)
	fa, fb := DequantizeQ8(ca, ra), DequantizeQ8(cb, rb)
	want := NewVector(fa).CosineSimilarity(NewVector(fb))
	if math.Abs(float64(got-want)) > 1e-5 {
		t.Fatalf("q8 cos=%v dequant cos=%v", got, want)
	}
	// L2 / dot also track dequant within tight tol.
	l2 := Q8L2FromCodes(ca, ra, cb, rb)
	wantL2 := NewVector(fa).EuclideanDistance(NewVector(fb))
	if math.Abs(float64(l2-wantL2)) > 1e-5 {
		t.Fatalf("q8 L2=%v dequant L2=%v", l2, wantL2)
	}
	dot := Q8DotFromCodes(ca, ra, cb, rb)
	wantDot := NewVector(fa).DotProduct(NewVector(fb))
	if math.Abs(float64(dot-wantDot)) > 1e-4 {
		t.Fatalf("q8 dot=%v dequant dot=%v", dot, wantDot)
	}
}

func TestVectorSetQ8Int8Search(t *testing.T) {
	vs := NewVectorSet()
	if !vs.SetQuantMode(QuantQ8) {
		t.Fatal("SetQuantMode Q8")
	}
	_ = vs.Add("near", NewVector([]float32{1, 0.9, 0.8, 0.7}), nil)
	_ = vs.Add("far", NewVector([]float32{-1, -0.9, -0.8, -0.7}), nil)
	_ = vs.Add("mid", NewVector([]float32{1, -1, 1, -1}), nil)

	q := NewVector([]float32{0.95, 0.85, 0.75, 0.65})
	res := vs.SearchWithMetricEF(q, 2, CosineSimilarity, 0, true)
	if len(res) < 2 || res[0].ID != "near" {
		t.Fatalf("brute Q8 top=%v", idsOf(res))
	}
	// Wipe dequantized f32 so any search using Vector.Data would mis-rank.
	vs.mu.Lock()
	for _, it := range vs.vectors {
		it.Vector = NewVector([]float32{0, 0, 0, 0})
	}
	vs.mu.Unlock()
	res2 := vs.SearchWithMetricEF(q, 2, CosineSimilarity, 0, true)
	if len(res2) < 2 || res2[0].ID != "near" {
		t.Fatalf("Q8 after f32 wipe top=%v", idsOf(res2))
	}
	// Graph insert/search distance must be on int8 codes (not f32 Vector.Data).
	vs.mu.RLock()
	df := vs.distFnLocked(CosineSimilarity)
	dSelf := df("near", "near")
	dNF := df("near", "far")
	qd := vs.queryDistFnLocked(q, CosineSimilarity)
	dQN := qd(hnswQueryKey, "near")
	dQF := qd(hnswQueryKey, "far")
	vs.mu.RUnlock()
	if dSelf > 1e-5 {
		t.Fatalf("Q8 self-distance=%v want ~0", dSelf)
	}
	if !(dNF > dSelf) {
		t.Fatalf("near-far dist %v should exceed self %v", dNF, dSelf)
	}
	if !(dQN < dQF) {
		t.Fatalf("query should be closer to near (%v) than far (%v)", dQN, dQF)
	}
}

func idsOf(res []*SearchResult) []string {
	out := make([]string, len(res))
	for i, r := range res {
		out[i] = r.ID
	}
	return out
}

func TestVectorSetBINHammingSearch(t *testing.T) {
	vs := NewVectorSet()
	if !vs.SetQuantMode(QuantBIN) {
		t.Fatal("SetQuantMode BIN")
	}
	// Force graph path (len > 64 skipped); keep tiny and use TRUTH-equivalent brute
	// plus an explicit distFn check via Search ranking.
	_ = vs.Add("near", NewVector([]float32{1, 1, 1, 1}), nil)
	_ = vs.Add("far", NewVector([]float32{-1, -1, -1, -1}), nil)
	_ = vs.Add("mid", NewVector([]float32{1, 1, -1, -1}), nil)

	q := NewVector([]float32{0.9, 0.8, 0.7, 0.6}) // all positive → same BIN as near
	res := vs.SearchWithMetricEF(q, 2, CosineSimilarity, 0, true)
	if len(res) < 2 {
		t.Fatalf("want 2 hits, got %d", len(res))
	}
	if res[0].ID != "near" {
		t.Fatalf("nearest=%s want near (scores=%v %v)", res[0].ID, res[0].Score, res[1].Score)
	}
	// Hamming path: near h=0 → cos=1; mid h=2 → cos=0
	if math.Abs(float64(res[0].Score-1)) > 1e-5 {
		t.Fatalf("near score=%v want 1", res[0].Score)
	}
	// Graph insert uses Hamming: Search without exact should agree on top-1.
	approx := vs.SearchWithMetricEF(q, 1, CosineSimilarity, 50, false)
	if len(approx) != 1 || approx[0].ID != "near" {
		t.Fatalf("graph Hamming top=%v", approx)
	}
}

func TestVectorSetBINStorage(t *testing.T) {
	vs := NewVectorSet()
	if !vs.SetQuantMode(QuantBIN) {
		t.Fatal("SetQuantMode BIN")
	}
	if !vs.Add("a", NewVector([]float32{1.2, -0.5, 0, -3}), nil) {
		t.Fatal("Add")
	}
	item, ok := vs.Get("a")
	if !ok || len(item.Bin) == 0 {
		t.Fatalf("want BIN bits, got %#v", item)
	}
	if vs.QuantMode().QuantTypeName() != "bin" {
		t.Fatalf("quant=%s", vs.QuantMode().QuantTypeName())
	}
	if vs.SetQuantMode(QuantQ8) {
		t.Fatal("expected quant mismatch")
	}
}
