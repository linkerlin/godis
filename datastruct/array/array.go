package array

import "sync"

// MaxIndex bounds the maximum sparse slot index in the Array preview to prevent
// unbounded single-command slice allocations.
const MaxIndex int64 = 1 << 20

// Array is the Redis 8.8 preview Array type: a sparse sequence of string
// slots where nil means empty. Len is max index + 1; Count is the number of
// non-empty slots.
type Array struct {
	mu    sync.RWMutex
	slots [][]byte
	count int

	// cursor is the ARINSERT/ARRING insertion cursor. order records indices in
	// insertion order for ARLASTITEMS (an overwrite moves the index to the end).
	cursor   int64
	order    []int64
	ringSize int64
}

func New() *Array {
	return &Array{}
}

func (a *Array) trimTrailingNilLocked() {
	n := len(a.slots)
	for n > 0 && a.slots[n-1] == nil {
		n--
	}
	if n == 0 {
		a.slots = nil
	} else {
		a.slots = a.slots[:n]
	}
}

// Set stores val at index, extending with empty slots as needed.
// Returns 1 when the slot was previously empty.
func (a *Array) Set(index int64, val []byte) int {
	if index < 0 || index > MaxIndex {
		return 0
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.setLocked(index, val)
}

func (a *Array) setLocked(index int64, val []byte) int {
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
	a.touchOrderLocked(index)
	if wasNew {
		return 1
	}
	return 0
}

// touchOrderLocked moves index to the end of the insertion-order log.
func (a *Array) touchOrderLocked(index int64) {
	for i, idx := range a.order {
		if idx == index {
			copy(a.order[i:], a.order[i+1:])
			a.order = a.order[:len(a.order)-1]
			break
		}
	}
	a.order = append(a.order, index)
}

// Insert writes values at consecutive cursor positions and advances the cursor.
// Returns the last inserted index and false when the cursor is exhausted.
func (a *Array) Insert(values ...[]byte) (int64, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	last := int64(-1)
	for _, val := range values {
		if a.cursor < 0 || a.cursor > MaxIndex {
			return last, false
		}
		a.setLocked(a.cursor, val)
		last = a.cursor
		a.cursor++
	}
	return last, true
}

// NextIndex returns the next ARINSERT index, or false when the cursor is exhausted.
func (a *Array) NextIndex() (int64, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.cursor < 0 || a.cursor > MaxIndex {
		return 0, false
	}
	return a.cursor, true
}

// SetCursor sets the insertion cursor.
func (a *Array) SetCursor(index int64) {
	a.mu.Lock()
	a.cursor = index
	a.mu.Unlock()
}

// RingInsert writes values as a ring buffer of the given size. When size is
// smaller than the current window the array is truncated to fit.
func (a *Array) RingInsert(size int64, values ...[]byte) (int64, bool) {
	if size <= 0 || size > MaxIndex+1 {
		return -1, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if size < int64(len(a.slots)) {
		a.slots = a.slots[:size]
		a.recountLocked()
		kept := a.order[:0]
		for _, idx := range a.order {
			if idx < size {
				kept = append(kept, idx)
			}
		}
		a.order = kept
	}
	a.ringSize = size
	last := int64(-1)
	for _, val := range values {
		idx := a.cursor % size
		if idx < 0 {
			idx += size
		}
		a.setLocked(idx, val)
		last = idx
		a.cursor++
	}
	return last, true
}

func (a *Array) recountLocked() {
	a.count = 0
	for _, s := range a.slots {
		if s != nil {
			a.count++
		}
	}
}

// LastItems returns the last count slots. In ring mode this follows insertion
// order (ring window); for plain/sparse arrays it returns the highest indices,
// including empty slots, matching Redis 8.10 ARLASTITEMS.
func (a *Array) LastItems(count int64, rev bool) [][]byte {
	if count <= 0 {
		return nil
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.ringSize == 0 {
		n := int64(len(a.slots))
		if count > n {
			count = n
		}
		start := n - count
		out := make([][]byte, 0, count)
		if rev {
			for i := n - 1; i >= start; i-- {
				out = append(out, a.slots[i])
			}
		} else {
			for i := start; i < n; i++ {
				out = append(out, a.slots[i])
			}
		}
		return out
	}
	n := len(a.order)
	if count > int64(n) {
		count = int64(n)
	}
	start := n - int(count)
	out := make([][]byte, 0, n-start)
	for i := n - 1; i >= start; i-- {
		idx := a.order[i]
		if idx < 0 || idx >= int64(len(a.slots)) || a.slots[idx] == nil {
			continue
		}
		out = append(out, a.slots[idx])
	}
	if !rev {
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	return out
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
	if deleted > 0 {
		a.trimTrailingNilLocked()
		a.pruneOrderLocked()
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
	if deleted > 0 {
		a.trimTrailingNilLocked()
		a.pruneOrderLocked()
	}
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

// State returns a consistent snapshot for opaque persistence.
func (a *Array) State() (slots [][]byte, order []int64, cursor, ringSize int64) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.slots, append([]int64(nil), a.order...), a.cursor, a.ringSize
}

func (a *Array) pruneOrderLocked() {
	kept := a.order[:0]
	for _, idx := range a.order {
		if idx >= 0 && idx < int64(len(a.slots)) && a.slots[idx] != nil {
			kept = append(kept, idx)
		}
	}
	a.order = kept
}

// Restore rebuilds an Array from persisted slots.
func Restore(slots [][]byte) *Array {
	return RestoreState(slots, nil, 0, 0)
}

// RestoreState rebuilds an Array including the insertion cursor and order log.
func RestoreState(slots [][]byte, order []int64, cursor, ringSize int64) *Array {
	a := &Array{
		slots:    append([][]byte(nil), slots...),
		cursor:   cursor,
		ringSize: ringSize,
	}
	for _, s := range a.slots {
		if s != nil {
			a.count++
		}
	}
	if len(order) == 0 {
		for i, s := range a.slots {
			if s != nil {
				a.order = append(a.order, int64(i))
			}
		}
		return a
	}
	seen := make(map[int64]struct{}, len(order))
	for _, idx := range order {
		if _, dup := seen[idx]; dup {
			continue
		}
		if idx < 0 || idx >= int64(len(a.slots)) || a.slots[idx] == nil {
			continue
		}
		seen[idx] = struct{}{}
		a.order = append(a.order, idx)
	}
	// Include any slots missing from the persisted log (defensive).
	for i, s := range a.slots {
		if s == nil {
			continue
		}
		if _, ok := seen[int64(i)]; !ok {
			a.order = append(a.order, int64(i))
		}
	}
	return a
}
