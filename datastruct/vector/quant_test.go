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
