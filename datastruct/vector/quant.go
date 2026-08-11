package vector

import "math"

// QuantMode is the per-set quantization format (locked after first insert).
type QuantMode int

const (
	// QuantF32 stores full-precision float32 (Godis default / NOQUANT).
	QuantF32 QuantMode = iota
	// QuantQ8 stores signed int8 per dim with a per-vector range scale.
	QuantQ8
)

// QuantTypeName returns the VINFO quant-type string.
func (m QuantMode) QuantTypeName() string {
	switch m {
	case QuantQ8:
		return "int8"
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
