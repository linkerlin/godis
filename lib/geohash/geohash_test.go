package geohash

import (
	"fmt"
	"math"
	"testing"
)

func TestToRange(t *testing.T) {
	neighbor := []byte{0x00, 0x00, 0x00, 0x00, 0xE0, 0x00, 0x00, 0x00}
	geoRange := toRange(neighbor, 36)
	expectedLower := ToInt([]byte{0x00, 0x00, 0x00, 0x00, 0xE0, 0x00, 0x00, 0x00})
	expectedUpper := ToInt([]byte{0x00, 0x00, 0x00, 0x00, 0xF0, 0x00, 0x00, 0x00})
	if expectedLower != geoRange[0] {
		t.Error("incorrect lower")
	}
	if expectedUpper != geoRange[1] {
		t.Error("incorrect upper")
	}
}

func TestEncode(t *testing.T) {
	lat0 := 48.669
	lng0 := -4.32913
	hash := Encode(lat0, lng0)
	str := ToString(FromInt(hash))
	if str != "gbsuv7zt7zh0" {
		t.Error("encode error")
	}
	lat, lng := Decode(hash)
	if math.Abs(lat-lat0) > 1e-6 || math.Abs(lng-lng0) > 1e-6 {
		t.Error("decode error")
	}
}

func TestEncodeFloat64Exact(t *testing.T) {
	h := Encode(48.669, -4.32913)
	if float64(h) != float64(uint64(float64(h))) {
		t.Fatalf("52-bit geohash must round-trip via float64: %d", h)
	}
	if h>>52 != 0 {
		t.Fatalf("hash should fit in 52 bits: %d", h)
	}
}

func TestGetNeighbours(t *testing.T) {
	ranges := GetNeighbours(90, 180, 630*1000)
	fmt.Printf("%#v", ranges)
}
