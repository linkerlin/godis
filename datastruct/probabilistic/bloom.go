package probabilistic

import (
	"fmt"
	"hash/fnv"
	"math"
)

// BloomFilter is a Bloom filter implementation
type BloomFilter struct {
	bits       []bool
	size       uint
	hashNum    uint // Number of hash functions
	count      uint
	expansion  uint
	nonScaling bool
}

// NewBloomFilter creates a new Bloom filter
// capacity: expected number of elements
// errorRate: desired false positive rate
func NewBloomFilter(capacity uint, errorRate float64) *BloomFilter {
	// Calculate optimal size and number of hash functions
	// m = -n*ln(p) / (ln(2)^2)
	// k = m/n * ln(2)
	size := uint(math.Ceil(-float64(capacity) * math.Log(errorRate) / (math.Ln2 * math.Ln2)))
	hashNum := uint(math.Ceil(float64(size) / float64(capacity) * math.Ln2))

	if size < 64 {
		size = 64
	}
	if hashNum < 1 {
		hashNum = 1
	}

	return &BloomFilter{
		bits:    make([]bool, size),
		size:    size,
		hashNum: hashNum,
	}
}

// SetExpansion stores RedisBloom EXPANSION factor (scaling not fully implemented).
func (bf *BloomFilter) SetExpansion(n uint) {
	if n == 0 {
		n = 2
	}
	bf.expansion = n
}

// SetNonScaling marks the filter as non-scaling.
func (bf *BloomFilter) SetNonScaling(v bool) {
	bf.nonScaling = v
}

// Add adds an element to the filter.
// Returns true if the item was not already present (newly added).
func (bf *BloomFilter) Add(data []byte) bool {
	existed := bf.Exists(data)
	positions := bf.getPositions(data)
	for _, pos := range positions {
		bf.bits[pos] = true
	}
	if !existed {
		bf.count++
	}
	return !existed
}

// Exists checks if an element might exist in the filter
// Returns true if the element might exist, false if it definitely doesn't exist
func (bf *BloomFilter) Exists(data []byte) bool {
	positions := bf.getPositions(data)
	for _, pos := range positions {
		if !bf.bits[pos] {
			return false
		}
	}
	return true
}

// Merge merges another Bloom filter into this one
// Both filters must have the same size
func (bf *BloomFilter) Merge(other *BloomFilter) error {
	if bf.size != other.size {
		return ErrFilterSizeMismatch
	}

	for i := uint(0); i < bf.size; i++ {
		bf.bits[i] = bf.bits[i] || other.bits[i]
	}

	bf.count += other.count
	return nil
}

// Info returns filter information
func (bf *BloomFilter) Info() map[string]interface{} {
	// Calculate bit count
	bitCount := 0
	for _, b := range bf.bits {
		if b {
			bitCount++
		}
	}

	// Calculate current error rate
	// (1 - e^(-kn/m))^k
	loadFactor := float64(bitCount) / float64(bf.size)
	currentError := math.Pow(1-math.Exp(-float64(bf.hashNum)*loadFactor), float64(bf.hashNum))

	return map[string]interface{}{
		"size":         bf.size,
		"hashNum":      bf.hashNum,
		"count":        bf.count,
		"bitCount":     bitCount,
		"loadFactor":   loadFactor,
		"currentError": currentError,
		"expansion":    bf.expansion,
		"nonScaling":   bf.nonScaling,
	}
}

// getPositions returns the bit positions for an element
func (bf *BloomFilter) getPositions(data []byte) []uint {
	positions := make([]uint, bf.hashNum)

	// Use double hashing: h(i) = (h1 + i*h2) % size
	h1, h2 := bf.hash(data)

	for i := uint(0); i < bf.hashNum; i++ {
		pos := (h1 + i*h2) % bf.size
		positions[i] = pos
	}

	return positions
}

// hash computes two hash values for data
func (bf *BloomFilter) hash(data []byte) (uint, uint) {
	h1 := fnv.New64a()
	h1.Write(data)

	h2 := fnv.New64()
	h2.Write(data)

	return uint(h1.Sum64() % uint64(bf.size)), uint(h2.Sum64() % uint64(bf.size))
}

// Errors
var ErrFilterSizeMismatch = fmt.Errorf("filter size mismatch")

// MarshalBinary serializes the bloom filter.
func (bf *BloomFilter) MarshalBinary() []byte {
	byteLen := (bf.size + 7) / 8
	out := make([]byte, 8+8+8+byteLen)
	putU64(out[0:], uint64(bf.size))
	putU64(out[8:], uint64(bf.hashNum))
	putU64(out[16:], uint64(bf.count))
	for i, b := range bf.bits {
		if b {
			out[24+i/8] |= 1 << (i % 8)
		}
	}
	return out
}

// UnmarshalBloomFilter restores a bloom filter from MarshalBinary output.
func UnmarshalBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 24 {
		return nil, fmt.Errorf("invalid bloom data")
	}
	size := uint(getU64(data[0:]))
	hashNum := uint(getU64(data[8:]))
	count := uint(getU64(data[16:]))
	byteLen := (size + 7) / 8
	if uint(len(data)-24) < byteLen || size == 0 {
		return nil, fmt.Errorf("invalid bloom data")
	}
	bf := &BloomFilter{
		bits:    make([]bool, size),
		size:    size,
		hashNum: hashNum,
		count:   count,
	}
	for i := uint(0); i < size; i++ {
		if data[24+i/8]&(1<<(i%8)) != 0 {
			bf.bits[i] = true
		}
	}
	return bf, nil
}

func putU64(b []byte, v uint64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

func getU64(b []byte) uint64 {
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}
