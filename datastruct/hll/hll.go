// Package hll implements a Redis-compatible HyperLogLog: the same 16384×6-bit
// dense register encoding ("HYLL" header + bit-packed registers) and the same
// xxHash64-based patlen, so a godis HLL stored as a string is byte-identical
// to a Redis HLL and can be migrated via RDB/DUMP-RESTORE.
package hll

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/cespare/xxhash/v2"
)

const (
	Registers     = 16384 // 2^14
	bitsPerReg    = 6
	regMax        = (1 << bitsPerReg) - 1 // 63
	denseSize     = (Registers * bitsPerReg) / 8 // 12288
	HeaderSize    = 16
	DenseDataSize = denseSize
	TotalSize     = HeaderSize + denseSize // 12304
)

var header = [4]byte{'H', 'Y', 'L', 'L'}

// HLL is the in-memory register array (0 = dense encoding, matching Redis).
type HLL struct {
	registers [Registers]uint8
}

// New creates an empty HLL.
func New() *HLL {
	return &HLL{}
}

// Add inserts an element; returns true when a register was raised.
func (h *HLL) Add(elem []byte) bool {
	hash := xxhash.Sum64(elem)
	index := hash & (Registers - 1) // low 14 bits
	hash >>= 14
	hash |= uint64(1) << 50 // guarantee loop termination
	bit := uint64(1)
	count := 1
	for hash&bit == 0 {
		count++
		bit <<= 1
	}
	if count > int(h.registers[index]) {
		h.registers[index] = uint8(count)
		return true
	}
	return false
}

// Count returns the estimated cardinality with the standard, small-range and
// large-range corrections (identical math to Redis hllCount).
func (h *HLL) Count() uint64 {
	var sum float64
	ez := 0
	for _, v := range h.registers {
		sum += math.Pow(2.0, -float64(v))
		if v == 0 {
			ez++
		}
	}
	m := float64(Registers)
	alpha := 0.7213 / (1.0 + 1.079/m)
	E := alpha * m * m / sum
	if E < m*2.5 && ez != 0 {
		E = m * math.Log(m/float64(ez))
	}
	// Large-range correction (2^64 domain).
	if E > math.Exp2(63) {
		E = -math.Exp2(63) * math.Log(1.0-E/math.Exp2(63))
	}
	return uint64(E + 0.5)
}

// Merge merges other into h (register-wise max).
func (h *HLL) Merge(other *HLL) {
	for i := range h.registers {
		if other.registers[i] > h.registers[i] {
			h.registers[i] = other.registers[i]
		}
	}
}

// Registers exposes the register array (for PFDEBUG GETREG).
func (h *HLL) Registers() []uint8 { return h.registers[:] }

// Encode serializes to the Redis dense wire format:
//
//	16-byte header ("HYLL" + encoding byte + cached cardinality)
//	12288 bytes of 6-bit packed registers.
func (h *HLL) Encode() []byte {
	buf := make([]byte, TotalSize)
	copy(buf, header[:])
	buf[4] = 0 // dense encoding
	binary.LittleEndian.PutUint64(buf[6:], 0)
	for i, v := range h.registers {
		setRegister(buf[HeaderSize:], i, v)
	}
	return buf
}

// Decode parses a Redis HLL string (dense). Sparse encoding is rejected with
// an error (godis always produces dense; a sparse blob from another source is
// reported, mirroring Redis's refusal to operate on corrupt HLLs).
func Decode(data []byte) (*HLL, error) {
	if len(data) < TotalSize {
		return nil, errors.New("invalid HLL string: too short")
	}
	if data[0] != 'H' || data[1] != 'Y' || data[2] != 'L' || data[3] != 'L' {
		return nil, errors.New("invalid HLL string: bad header")
	}
	if data[4] != 0 {
		return nil, errors.New("sparse HLL encoding not supported")
	}
	h := &HLL{}
	for i := 0; i < Registers; i++ {
		h.registers[i] = getRegister(data[HeaderSize:], i)
	}
	return h, nil
}

// IsHLLString reports whether data looks like a dense godis/Redis HLL string.
func IsHLLString(data []byte) bool {
	return len(data) >= TotalSize &&
		data[0] == 'H' && data[1] == 'Y' && data[2] == 'L' && data[3] == 'L' &&
		data[4] == 0
}

// getRegister reads the 6-bit register at index from the packed buffer.
func getRegister(buf []byte, idx int) uint8 {
	bytePos := idx * bitsPerReg / 8
	bitPos := idx * bitsPerReg & 7
	bitCount := 8 - bitPos
	low := buf[bytePos] >> bitPos
	var high uint8
	if bitCount < bitsPerReg {
		high = buf[bytePos+1] << bitCount
	}
	return (low | high) & regMax
}

// setRegister writes the 6-bit register at index into the packed buffer.
func setRegister(buf []byte, idx int, val uint8) {
	bytePos := idx * bitsPerReg / 8
	bitPos := idx * bitsPerReg & 7
	bitCount := 8 - bitPos
	buf[bytePos] &^= regMax << bitPos
	buf[bytePos] |= val << bitPos
	if bitCount < bitsPerReg {
		buf[bytePos+1] &^= regMax >> bitCount
		buf[bytePos+1] |= val >> bitCount
	}
}
