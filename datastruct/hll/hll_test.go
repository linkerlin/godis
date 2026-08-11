package hll

import (
	"bytes"
	"testing"
)

// TestDecodeSparseEmpty promotes Redis empty sparse (XZERO:16384) to dense zero registers.
func TestDecodeSparseEmpty(t *testing.T) {
	blob := EncodeSparseEmpty()
	if !IsSparseHLLString(blob) {
		t.Fatal("expected sparse header")
	}
	h, err := Decode(blob)
	if err != nil {
		t.Fatalf("decode sparse empty: %v", err)
	}
	for i, v := range h.Registers() {
		if v != 0 {
			t.Fatalf("register %d = %d, want 0", i, v)
		}
	}
	if h.Count() != 0 {
		t.Fatalf("count=%d want 0", h.Count())
	}
	// Promote write path: Encode always dense.
	dense := h.Encode()
	if dense[4] != 0 || len(dense) != TotalSize {
		t.Fatalf("Encode should emit dense, enc=%d len=%d", dense[4], len(dense))
	}
}

// TestDecodeSparseRoundTrip builds a sparse blob from dense regs and decodes back.
func TestDecodeSparseRoundTrip(t *testing.T) {
	h := New()
	for _, v := range []string{"a", "b", "c", "a", "d", "e"} {
		h.Add([]byte(v))
	}
	// Cap register values at 32 so they fit sparse VAL opcodes (copy).
	regs := make([]uint8, Registers)
	copy(regs, h.Registers())
	for i, v := range regs {
		if v > 32 {
			regs[i] = 32
		}
	}
	sparse, err := EncodeSparseFromRegisters(regs)
	if err != nil {
		t.Fatalf("encode sparse: %v", err)
	}
	got, err := Decode(sparse)
	if err != nil {
		t.Fatalf("decode sparse: %v", err)
	}
	if !bytes.Equal(got.Registers(), regs) {
		// Find first mismatch for debugging.
		for i := range regs {
			if got.Registers()[i] != regs[i] {
				t.Fatalf("reg[%d] got %d want %d", i, got.Registers()[i], regs[i])
			}
		}
	}
}

// TestDecodeRejectsCorruptSparse verifies incomplete opcode streams fail cleanly.
func TestDecodeRejectsCorruptSparse(t *testing.T) {
	blob := EncodeSparseEmpty()
	// Truncate away the XZERO payload — leave only header.
	short := blob[:HeaderSize]
	if _, err := Decode(short); err != ErrCorruptHLL {
		t.Fatalf("want ErrCorruptHLL, got %v", err)
	}
	// Unknown encoding byte.
	bad := make([]byte, TotalSize)
	copy(bad[:4], "HYLL")
	bad[4] = 2
	if _, err := Decode(bad); err != ErrSparseEncoding {
		t.Fatalf("want ErrSparseEncoding, got %v", err)
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
