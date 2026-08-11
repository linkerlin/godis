// Package hll implements a Redis-compatible HyperLogLog: the same 16384×6-bit
// dense register encoding ("HYLL" header + bit-packed registers), sparse RLE
// decode→dense promote, and the same xxHash64-based patlen, so a godis HLL
// stored as a string is byte-identical to a Redis dense HLL and can be migrated
// via RDB/DUMP-RESTORE. Sparse blobs from Redis are read-safe and promoted to
// dense on write (PFADD/PFMERGE); godis itself always emits dense.
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

	encodingDense  = 0
	encodingSparse = 1
)

var header = [4]byte{'H', 'Y', 'L', 'L'}

// ErrSparseEncoding is returned for HLL blobs whose encoding byte is neither
// dense nor sparse (corrupt / future encodings).
var ErrSparseEncoding = errors.New("unsupported HLL encoding")

// ErrCorruptHLL is returned when a sparse (or dense) blob fails structural checks.
var ErrCorruptHLL = errors.New("corrupt HLL string")

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
	buf[4] = encodingDense
	binary.LittleEndian.PutUint64(buf[6:], 0)
	for i, v := range h.registers {
		setRegister(buf[HeaderSize:], i, v)
	}
	return buf
}

// Decode parses a Redis HLL string. Dense blobs are read directly; sparse
// (encoding=1) is promoted to an in-memory dense HLL (same as Redis
// hllSparseToDense). Godis always re-encodes as dense on write.
func Decode(data []byte) (*HLL, error) {
	if len(data) < HeaderSize {
		return nil, errors.New("invalid HLL string: too short")
	}
	if data[0] != 'H' || data[1] != 'Y' || data[2] != 'L' || data[3] != 'L' {
		return nil, errors.New("invalid HLL string: bad header")
	}
	switch data[4] {
	case encodingDense:
		if len(data) < TotalSize {
			return nil, errors.New("invalid HLL string: dense too short")
		}
		h := &HLL{}
		for i := 0; i < Registers; i++ {
			h.registers[i] = getRegister(data[HeaderSize:], i)
		}
		return h, nil
	case encodingSparse:
		return decodeSparse(data)
	default:
		return nil, ErrSparseEncoding
	}
}

// decodeSparse implements Redis hllSparseToDense opcode walk.
// ZERO  00xxxxxx       → 1..64 zero registers
// XZERO 01xxxxxx yyyy → 1..16384 zero registers (14-bit + 1)
// VAL   1vvvvvxx       → value 1..32, run 1..4
func decodeSparse(data []byte) (*HLL, error) {
	h := &HLL{}
	p := HeaderSize
	end := len(data)
	idx := 0
	for p < end {
		b := data[p]
		switch {
		case b&0xc0 == 0: // ZERO
			runlen := int(b&0x3f) + 1
			if idx+runlen > Registers {
				return nil, ErrCorruptHLL
			}
			idx += runlen
			p++
		case b&0xc0 == 0x40: // XZERO
			if p+1 >= end {
				return nil, ErrCorruptHLL
			}
			runlen := (int(b&0x3f)<<8 | int(data[p+1])) + 1
			if idx+runlen > Registers {
				return nil, ErrCorruptHLL
			}
			idx += runlen
			p += 2
		default: // VAL (MSB set)
			runlen := int(b&0x3) + 1
			regval := uint8(((b >> 2) & 0x1f) + 1)
			if idx+runlen > Registers {
				return nil, ErrCorruptHLL
			}
			for i := 0; i < runlen; i++ {
				h.registers[idx] = regval
				idx++
			}
			p++
		}
	}
	if idx != Registers {
		return nil, ErrCorruptHLL
	}
	return h, nil
}

// IsHLLString reports whether data looks like a dense or sparse Redis HLL.
func IsHLLString(data []byte) bool {
	if len(data) < HeaderSize ||
		data[0] != 'H' || data[1] != 'Y' || data[2] != 'L' || data[3] != 'L' {
		return false
	}
	switch data[4] {
	case encodingDense:
		return len(data) >= TotalSize
	case encodingSparse:
		return true
	default:
		return false
	}
}

// IsSparseHLLString reports a Redis-style HLL header with sparse encoding.
func IsSparseHLLString(data []byte) bool {
	return len(data) >= HeaderSize &&
		data[0] == 'H' && data[1] == 'Y' && data[2] == 'L' && data[3] == 'L' &&
		data[4] == encodingSparse
}

// EncodeSparseEmpty returns a Redis sparse empty HLL (XZERO:16384) for tests.
func EncodeSparseEmpty() []byte {
	buf := make([]byte, HeaderSize+2)
	copy(buf, header[:])
	buf[4] = encodingSparse
	// XZERO len 16384 → encoded (16383) as 0x7f 0xff
	buf[HeaderSize] = 0x40 | 0x3f
	buf[HeaderSize+1] = 0xff
	return buf
}

// EncodeSparseFromRegisters builds a minimal sparse blob from dense registers
// for tests. Values >32 cannot be encoded sparsely and return an error.
func EncodeSparseFromRegisters(regs []uint8) ([]byte, error) {
	if len(regs) != Registers {
		return nil, errors.New("need 16384 registers")
	}
	out := make([]byte, 0, HeaderSize+64)
	out = append(out, header[0], header[1], header[2], header[3], encodingSparse, 0, 0, 0)
	out = append(out, 0, 0, 0, 0, 0, 0, 0, 0) // card
	i := 0
	for i < Registers {
		v := regs[i]
		if v == 0 {
			j := i + 1
			for j < Registers && regs[j] == 0 {
				j++
			}
			n := j - i
			for n > 0 {
				chunk := n
				if chunk > 16384 {
					chunk = 16384
				}
				if chunk > 64 {
					enc := chunk - 1
					out = append(out, byte(0x40|((enc>>8)&0x3f)), byte(enc&0xff))
				} else {
					out = append(out, byte(chunk-1))
				}
				n -= chunk
			}
			i = j
			continue
		}
		if v > 32 {
			return nil, errors.New("register >32 not sparsable")
		}
		j := i + 1
		for j < Registers && regs[j] == v && j-i < 4 {
			j++
		}
		run := j - i
		out = append(out, byte(0x80|((int(v)-1)<<2)|(run-1)))
		i = j
	}
	return out, nil
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
