package hll

import (
	"bytes"
	"testing"
)

// TestDecodeRejectsSparse verifies sparse-encoded HLL blobs (from other Redis
// sources) are rejected instead of silently mis-parsed: godis always produces
// dense encoding, and Redis itself refuses to read corrupt HLL strings.
func TestDecodeRejectsSparse(t *testing.T) {
	sparse := make([]byte, TotalSize)
	copy(sparse[:4], "HYLL")
	sparse[4] = 1 // HLL_SPARSE
	if _, err := Decode(sparse); err == nil {
		t.Fatal("sparse HLL blob should be rejected")
	} else if err != ErrSparseEncoding {
		t.Fatalf("want ErrSparseEncoding, got %v", err)
	}
	if !IsSparseHLLString(sparse) {
		t.Fatal("IsSparseHLLString should detect sparse header")
	}
	// Short / bad-header blobs are also rejected.
	if _, err := Decode([]byte("HYLL")); err == nil {
		t.Fatal("short blob should be rejected")
	}
	bad := make([]byte, TotalSize)
	copy(bad[:4], "XXXX")
	bad[4] = 0
	if _, err := Decode(bad); err == nil {
		t.Fatal("bad header should be rejected")
	}
}

// TestDecodeDenseRoundTrip verifies a dense blob round-trips through
// Encode/Decode with the same register contents.
func TestDecodeDenseRoundTrip(t *testing.T) {
	h := New()
	for _, v := range []string{"a", "b", "c", "a", "d"} {
		h.Add([]byte(v))
	}
	blob := h.Encode()
	got, err := Decode(blob)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(got.Registers(), h.Registers()) {
		t.Fatal("dense round-trip should preserve registers")
	}
}
