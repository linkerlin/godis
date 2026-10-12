package array

import "sync"

// Array is the Redis 8.8 preview Array type: a sparse sequence of string
// slots where nil means empty. Len is max index + 1; Count is the number of
// non-empty slots.
type Array struct {
	mu    sync.RWMutex
	slots [][]byte
	count int
}

func New() *Array {
	return &Array{}
}

// Set stores val at index, extending with empty slots as needed.
// Returns 1 when the slot was previously empty.
func (a *Array) Set(index int64, val []byte) int {
	if index < 0 {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if index >= int64(len(a.slots)) {
		grown := make([][]byte, index+1)
		copy(grown, a.slots)
		a.slots = grown
	}
	wasNew := a.slots[index] == nil
	a.slots[index] = append([]byte(nil), val...)
	if wasNew {
		a.count++
	}
	if wasNew {
		return 1
	}
	return 0
}

// Get returns the value at index (nil, false when empty or out of range).
func (a *Array) Get(index int64) ([]byte, bool) {
	if index < 0 {
		return nil, false
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if index >= int64(len(a.slots)) || a.slots[index] == nil {
		return nil, false
	}
	return a.slots[index], true
}

// Len returns the array length (max index + 1).
func (a *Array) Len() int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return int64(len(a.slots))
}

// Count returns the number of non-empty slots.
func (a *Array) Count() int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return int64(a.count)
}

// Del empties the given indices and returns how many were non-empty.
func (a *Array) Del(indices ...int64) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	deleted := int64(0)
	for _, idx := range indices {
		if idx < 0 || idx >= int64(len(a.slots)) || a.slots[idx] == nil {
			continue
		}
		a.slots[idx] = nil
		a.count--
		deleted++
	}
	return deleted
}

// DelRanges empties inclusive [start,end] ranges (start>end normalized
// ascending; overlaps counted once) and returns how many were non-empty.
// ponytail: marks via a scratch map — O(ranges + touched), fine for the
// preview subset.
func (a *Array) DelRanges(ranges [][2]int64) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	deleted := int64(0)
	marked := make(map[int64]struct{})
	for _, r := range ranges {
		lo, hi := r[0], r[1]
		if lo > hi {
			lo, hi = hi, lo
		}
		if hi < 0 || lo >= int64(len(a.slots)) {
			continue
		}
		if lo < 0 {
			lo = 0
		}
		if hi >= int64(len(a.slots)) {
			hi = int64(len(a.slots)) - 1
		}
		for i := lo; i <= hi; i++ {
			if a.slots[i] != nil {
				if _, seen := marked[i]; !seen {
					marked[i] = struct{}{}
					deleted++
				}
			}
		}
	}
	for i := range marked {
		a.slots[i] = nil
	}
	a.count -= int(deleted)
	return deleted
}

// Range returns slots from start to end inclusive (reverse order when
// start > end); indices outside the array yield nil entries.
func (a *Array) Range(start, end int64) [][]byte {
	a.mu.RLock()
	defer a.mu.RUnlock()
	step := int64(1)
	if start > end {
		step = -1
	}
	n := end - start
	if n < 0 {
		n = -n
	}
	out := make([][]byte, 0, n+1)
	for i := start; ; i += step {
		if i < 0 || i >= int64(len(a.slots)) {
			out = append(out, nil)
		} else {
			out = append(out, a.slots[i])
		}
		if i == end {
			break
		}
	}
	return out
}

// Slots exposes the raw slot slice for opaque persistence.
func (a *Array) Slots() [][]byte {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.slots
}

// Restore rebuilds an Array from persisted slots.
func Restore(slots [][]byte) *Array {
	a := &Array{slots: append([][]byte(nil), slots...)}
	for _, s := range a.slots {
		if s != nil {
			a.count++
		}
	}
	return a
}
