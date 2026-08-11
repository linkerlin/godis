package vector

import (
	"math"
	"math/bits"
)

// QuantMode is the per-set quantization format (locked after first insert).
type QuantMode int

const (
	// QuantF32 stores full-precision float32 (Godis default / NOQUANT).
	QuantF32 QuantMode = iota
	// QuantQ8 stores signed int8 per dim with a per-vector range scale.
	QuantQ8
	// QuantBIN stores 1 bit per dim (sign): packed LSB-first.
	// HNSW / VSIM use Hamming on packed bits (rank-equivalent to ±1 cosine).
	QuantBIN
)

// QuantTypeName returns the VINFO quant-type string.
func (m QuantMode) QuantTypeName() string {
	switch m {
	case QuantQ8:
		return "int8"
	case QuantBIN:
		return "bin"
	default:
		return "f32"
	}
}

// QuantizeQ8 maps floats to int8 in [-127,127] with range = max(|v_i|).
// Reconstruct: v_i ≈ q_i/127 * range. Empty/zero vectors use range=0 and zeros.
func QuantizeQ8(data []float32) (codes []int8, qrange float32) {
	if len(data) == 0 {
		return nil, 0
	}
	var maxAbs float32
	for _, v := range data {
		a := float32(math.Abs(float64(v)))
		if a > maxAbs {
			maxAbs = a
		}
	}
	codes = make([]int8, len(data))
	if maxAbs == 0 {
		return codes, 0
	}
	scale := float32(127) / maxAbs
	for i, v := range data {
		q := int(math.Round(float64(v * scale)))
		if q > 127 {
			q = 127
		} else if q < -127 {
			q = -127
		}
		codes[i] = int8(q)
	}
	return codes, maxAbs
}

// DequantizeQ8 reconstructs approximate float32 values from Q8 codes.
func DequantizeQ8(codes []int8, qrange float32) []float32 {
	out := make([]float32, len(codes))
	if qrange == 0 || len(codes) == 0 {
		return out
	}
	scale := qrange / float32(127)
	for i, q := range codes {
		out[i] = float32(q) * scale
	}
	return out
}

// QuantizeBIN packs one sign bit per dimension (1 if v>=0, else 0), LSB-first
// within each byte. Matches Redis Vector Set BIN convention for dense float input.
func QuantizeBIN(data []float32) []byte {
	if len(data) == 0 {
		return nil
	}
	out := make([]byte, (len(data)+7)/8)
	for i, v := range data {
		if v >= 0 {
			out[i/8] |= 1 << (uint(i) % 8)
		}
	}
	return out
}

// DequantizeBIN expands packed BIN bits to ±1.0 float32 (length = dim).
// Kept for VEMB / opaque display; graph search uses HammingDistanceBits.
func DequantizeBIN(packed []byte, dim int) []float32 {
	out := make([]float32, dim)
	for i := 0; i < dim; i++ {
		if i/8 < len(packed) && packed[i/8]&(1<<(uint(i)%8)) != 0 {
			out[i] = 1
		} else {
			out[i] = -1
		}
	}
	return out
}

// HammingDistanceBits counts differing sign bits among the first dim dimensions
// of two packed BIN codes (LSB-first within each byte). Unused bits in the last
// byte are masked out.
func HammingDistanceBits(a, b []byte, dim int) int {
	if dim <= 0 {
		return 0
	}
	nbytes := (dim + 7) / 8
	dist := 0
	for i := 0; i < nbytes; i++ {
		var ba, bb byte
		if i < len(a) {
			ba = a[i]
		}
		if i < len(b) {
			bb = b[i]
		}
		x := ba ^ bb
		if i == nbytes-1 {
			if rem := dim % 8; rem != 0 {
				x &= byte((1 << uint(rem)) - 1)
			}
		}
		dist += bits.OnesCount8(x)
	}
	return dist
}

// BINCosineFromHamming maps Hamming count to cosine of ±1 vectors:
// cos = (dim - 2*h) / dim. Rank-equivalent to using Hamming as distance.
func BINCosineFromHamming(h, dim int) float32 {
	if dim <= 0 {
		return 0
	}
	return float32(dim-2*h) / float32(dim)
}

// Q8DotInt returns Σ a_i·b_i over int8 codes (overflow-safe via int64).
func Q8DotInt(a, b []int8) int64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	var sum int64
	for i := 0; i < n; i++ {
		sum += int64(a[i]) * int64(b[i])
	}
	return sum
}

// Q8Norm2Int returns Σ a_i².
func Q8Norm2Int(a []int8) int64 {
	var sum int64
	for _, v := range a {
		sum += int64(v) * int64(v)
	}
	return sum
}

// Q8CosineFromCodes is cosine of the dequantized vectors without allocating
// float32 buffers. Per-vector ranges cancel: cos = (q·p) / (||q||·||p||).
func Q8CosineFromCodes(a, b []int8) float32 {
	na2, nb2 := Q8Norm2Int(a), Q8Norm2Int(b)
	if na2 == 0 || nb2 == 0 {
		return 0
	}
	dot := float64(Q8DotInt(a, b))
	return float32(dot / (math.Sqrt(float64(na2)) * math.Sqrt(float64(nb2))))
}

// Q8L2FromCodes is Euclidean distance of dequantized vectors:
// || (ra/127)·a − (rb/127)·b ||, computed without materializing f32 arrays.
func Q8L2FromCodes(a []int8, ra float32, b []int8, rb float32) float32 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	sa := float64(ra) / 127
	sb := float64(rb) / 127
	var sum float64
	for i := 0; i < n; i++ {
		d := sa*float64(a[i]) - sb*float64(b[i])
		sum += d * d
	}
	return float32(math.Sqrt(sum))
}

// Q8DotFromCodes is the dequantized dot product without f32 buffers.
func Q8DotFromCodes(a []int8, ra float32, b []int8, rb float32) float32 {
	return float32(float64(ra) * float64(rb) / (127 * 127) * float64(Q8DotInt(a, b)))
}
