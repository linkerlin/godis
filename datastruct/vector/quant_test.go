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
