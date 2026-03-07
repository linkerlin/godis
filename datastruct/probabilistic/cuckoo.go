package probabilistic

import (
	"fmt"
	"hash/fnv"
)

// CuckooFilter is a Cuckoo filter implementation
type CuckooFilter struct {
	buckets    [][]string // Each bucket can hold multiple fingerprints
	size       uint
	bucketSize uint
	count      uint
	maxKicks   uint
}

// NewCuckooFilter creates a new Cuckoo filter
func NewCuckooFilter(capacity uint) *CuckooFilter {
	bucketSize := uint(4)
	numBuckets := (capacity + bucketSize - 1) / bucketSize
	
	buckets := make([][]string, numBuckets)
	for i := range buckets {
		buckets[i] = make([]string, 0, bucketSize)
	}
	
	return &CuckooFilter{
		buckets:    buckets,
		size:       numBuckets,
		bucketSize: bucketSize,
		maxKicks:   500,
	}
}

// Add adds an element to the filter
// Returns error if filter is full
func (cf *CuckooFilter) Add(data []byte) error {
	fp := cf.fingerprint(data)
	i1, i2 := cf.positions(data, fp)
	
	// Try to insert in either bucket
	if len(cf.buckets[i1]) < int(cf.bucketSize) {
		cf.buckets[i1] = append(cf.buckets[i1], fp)
		cf.count++
		return nil
	}
	
	if len(cf.buckets[i2]) < int(cf.bucketSize) {
		cf.buckets[i2] = append(cf.buckets[i2], fp)
		cf.count++
		return nil
	}
	
	// Both buckets full, need to kick
	i := i1
	for n := uint(0); n < cf.maxKicks; n++ {
		// Random entry in bucket
		j := uint(cf.hash([]byte(fmt.Sprintf("%d", n)))) % uint(len(cf.buckets[i]))
		
		// Swap
		fp, cf.buckets[i][j] = cf.buckets[i][j], fp
		
		// Alternate position
		i = cf.alternatePosition(i, fp)
		
		if len(cf.buckets[i]) < int(cf.bucketSize) {
			cf.buckets[i] = append(cf.buckets[i], fp)
			cf.count++
			return nil
		}
	}
	
	return ErrFilterFull
}

// Exists checks if an element might exist
func (cf *CuckooFilter) Exists(data []byte) bool {
	fp := cf.fingerprint(data)
	i1, i2 := cf.positions(data, fp)
	
	// Check both buckets
	for _, f := range cf.buckets[i1] {
		if f == fp {
			return true
		}
	}
	
	for _, f := range cf.buckets[i2] {
		if f == fp {
			return true
		}
	}
	
	return false
}

// Delete removes an element (may delete false positives)
func (cf *CuckooFilter) Delete(data []byte) bool {
	fp := cf.fingerprint(data)
	i1, i2 := cf.positions(data, fp)
	
	// Try to delete from both buckets
	for i, f := range cf.buckets[i1] {
		if f == fp {
			cf.buckets[i1] = append(cf.buckets[i1][:i], cf.buckets[i1][i+1:]...)
			cf.count--
			return true
		}
	}
	
	for i, f := range cf.buckets[i2] {
		if f == fp {
			cf.buckets[i2] = append(cf.buckets[i2][:i], cf.buckets[i2][i+1:]...)
			cf.count--
			return true
		}
	}
	
	return false
}

// Count returns the number of elements
func (cf *CuckooFilter) Count() uint {
	return cf.count
}

// Info returns filter info
func (cf *CuckooFilter) Info() map[string]interface{} {
	usedBuckets := 0
	for _, b := range cf.buckets {
		if len(b) > 0 {
			usedBuckets++
		}
	}
	
	return map[string]interface{}{
		"size":        cf.size * cf.bucketSize,
		"buckets":     cf.size,
		"bucketSize":  cf.bucketSize,
		"count":       cf.count,
		"usedBuckets": usedBuckets,
		"loadFactor":  float64(cf.count) / float64(cf.size*cf.bucketSize),
	}
}

// fingerprint generates a fingerprint for data
func (cf *CuckooFilter) fingerprint(data []byte) string {
	h := fnv.New32a()
	h.Write(data)
	return fmt.Sprintf("%x", h.Sum32())[:8]
}

// positions returns the two possible bucket positions
func (cf *CuckooFilter) positions(data []byte, fp string) (uint, uint) {
	h := cf.hash(data)
	i1 := h % cf.size
	i2 := cf.alternatePosition(i1, fp)
	return i1, i2
}

// alternatePosition calculates the alternate position
func (cf *CuckooFilter) alternatePosition(i uint, fp string) uint {
	h := cf.hash([]byte(fp))
	return (i ^ (h % cf.size)) % cf.size
}

// hash computes a hash value
func (cf *CuckooFilter) hash(data []byte) uint {
	h := fnv.New64a()
	h.Write(data)
	return uint(h.Sum64() % uint64(cf.size))
}

// Errors
var ErrFilterFull = fmt.Errorf("cuckoo filter is full")
