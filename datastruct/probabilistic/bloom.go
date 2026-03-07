package probabilistic

import (
	"fmt"
	"hash/fnv"
	"math"
)

// BloomFilter is a Bloom filter implementation
type BloomFilter struct {
	bits     []bool
	size     uint
	hashNum  uint // Number of hash functions
	count    uint
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

// Add adds an element to the filter
func (bf *BloomFilter) Add(data []byte) {
	positions := bf.getPositions(data)
	for _, pos := range positions {
		bf.bits[pos] = true
	}
	bf.count++
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
		"size":          bf.size,
		"hashNum":       bf.hashNum,
		"count":         bf.count,
		"bitCount":      bitCount,
		"loadFactor":    loadFactor,
		"currentError":  currentError,
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
