package database

import (
	"testing"

	"github.com/linkerlin/godis/datastruct/probabilistic"
	"github.com/linkerlin/godis/interface/database"
)

func TestM2alEncodeDecodeBloomPayload(t *testing.T) {
	bf := probabilistic.NewBloomFilter(100, 0.01)
	bf.Add([]byte("x"))
	payload, err := encodeDumpPayload(&database.DataEntity{Data: bf})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	entity, err := decodeDumpPayload(payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	out, ok := entity.Data.(*probabilistic.BloomFilter)
	if !ok {
		t.Fatalf("type %T", entity.Data)
	}
	if !out.Exists([]byte("x")) {
		t.Fatal("missing")
	}
}
