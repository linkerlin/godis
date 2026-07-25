package aof

import (
	"testing"

	"github.com/linkerlin/godis/datastruct/probabilistic"
	"github.com/linkerlin/godis/interface/database"
)

func TestOpaqueBloomRoundTrip(t *testing.T) {
	bf := probabilistic.NewBloomFilter(100, 0.01)
	bf.Add([]byte("x"))
	p, ok := EncodeOpaque(&database.DataEntity{Data: bf})
	if !ok {
		t.Fatal("encode failed")
	}
	e, ok := DecodeOpaque(p)
	if !ok {
		t.Fatal("decode failed")
	}
	out, ok := e.Data.(*probabilistic.BloomFilter)
	if !ok || !out.Exists([]byte("x")) {
		t.Fatal("missing x after decode")
	}
}
