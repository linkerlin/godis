package probabilistic

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

// CuckooFilter is a Cuckoo filter implementation
type CuckooFilter struct {
	buckets    [][]string // Each bucket can hold multiple fingerprints
	size       uint
	bucketSize uint
	count      uint
	maxKicks   uint
	expansion  uint // grow factor when full; 0 = no auto-expand
}

// NewCuckooFilter creates a new Cuckoo filter
func NewCuckooFilter(capacity uint) *CuckooFilter {
	return NewCuckooFilterOpts(capacity, 4, 500)
}

// NewCuckooFilterOpts creates a Cuckoo filter with explicit bucket size and max kicks.
func NewCuckooFilterOpts(capacity, bucketSize, maxKicks uint) *CuckooFilter {
	if bucketSize == 0 {
		bucketSize = 4
	}
	if maxKicks == 0 {
		maxKicks = 500
	}
	numBuckets := (capacity + bucketSize - 1) / bucketSize
	if numBuckets == 0 {
		numBuckets = 1
	}

	buckets := make([][]string, numBuckets)
	for i := range buckets {
		buckets[i] = make([]string, 0, bucketSize)
	}

	return &CuckooFilter{
		buckets:    buckets,
		size:       numBuckets,
		bucketSize: bucketSize,
		maxKicks:   maxKicks,
		expansion:  1,
	}
}

// SetExpansion sets auto-expansion factor (0 disables).
func (cf *CuckooFilter) SetExpansion(exp uint) {
	cf.expansion = exp
}

// Expansion returns the configured expansion factor.
func (cf *CuckooFilter) Expansion() uint {
	return cf.expansion
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

	if cf.expansion > 0 && cf.expand() {
		return cf.Add(data)
	}
	return ErrFilterFull
}

// expand grows the bucket array by expansion factor. Returns false if disabled.
func (cf *CuckooFilter) expand() bool {
	if cf.expansion == 0 {
		return false
	}
	newSize := cf.size * cf.expansion
	if newSize <= cf.size {
		newSize = cf.size + 1
	}
	oldBuckets := cf.buckets
	oldCount := cf.count
	cf.buckets = make([][]string, newSize)
	for i := range cf.buckets {
		cf.buckets[i] = make([]string, 0, cf.bucketSize)
	}
	cf.size = newSize
	cf.count = 0
	// Re-insert fingerprints best-effort (positions change with size).
	for _, bucket := range oldBuckets {
		for _, fp := range bucket {
			placed := false
			for n := uint(0); n < cf.maxKicks+cf.bucketSize; n++ {
				idx := cf.hash([]byte(fp+strconv.FormatUint(uint64(n), 10))) % cf.size
				if len(cf.buckets[idx]) < int(cf.bucketSize) {
					cf.buckets[idx] = append(cf.buckets[idx], fp)
					cf.count++
					placed = true
					break
				}
			}
			if !placed {
				// restore on failure
				cf.buckets = oldBuckets
				cf.size = uint(len(oldBuckets))
				cf.count = oldCount
				return false
			}
		}
	}
	return true
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

// CountItem returns how many times item's fingerprint appears in the filter.
func (cf *CuckooFilter) CountItem(data []byte) int {
	fp := cf.fingerprint(data)
	count := 0
	for _, bucket := range cf.buckets {
		for _, f := range bucket {
			if f == fp {
				count++
			}
		}
	}
	return count
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
		"expansion":   cf.expansion,
		"maxKicks":    cf.maxKicks,
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

// MarshalBinary serializes the cuckoo filter (bucket fingerprints).
func (cf *CuckooFilter) MarshalBinary() []byte {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("%d %d %d %d %d\n", cf.size, cf.bucketSize, cf.count, cf.maxKicks, cf.expansion))
	for _, bucket := range cf.buckets {
		b.WriteString(strconv.Itoa(len(bucket)))
		for _, fp := range bucket {
			b.WriteByte(' ')
			b.WriteString(fp)
		}
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

// UnmarshalCuckooFilter restores a filter from MarshalBinary output.
func UnmarshalCuckooFilter(data []byte) (*CuckooFilter, error) {
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) < 1 {
		return nil, fmt.Errorf("invalid cuckoo data")
	}
	var size, bucketSize, count, maxKicks, expansion uint
	n, err := fmt.Sscanf(lines[0], "%d %d %d %d %d", &size, &bucketSize, &count, &maxKicks, &expansion)
	if err != nil || n < 4 {
		return nil, fmt.Errorf("invalid cuckoo header")
	}
	if n == 4 {
		expansion = 1
	}
	if int(size) != len(lines)-1 {
		return nil, fmt.Errorf("invalid cuckoo bucket count")
	}
	cf := &CuckooFilter{
		buckets:    make([][]string, size),
		size:       size,
		bucketSize: bucketSize,
		count:      count,
		maxKicks:   maxKicks,
		expansion:  expansion,
	}
	for i := uint(0); i < size; i++ {
		fields := strings.Fields(lines[i+1])
		if len(fields) == 0 {
			return nil, fmt.Errorf("invalid cuckoo bucket")
		}
		n, err := strconv.Atoi(fields[0])
		if err != nil || n < 0 || len(fields)-1 != n {
			return nil, fmt.Errorf("invalid cuckoo bucket")
		}
		cf.buckets[i] = append([]string{}, fields[1:]...)
	}
	return cf, nil
}
