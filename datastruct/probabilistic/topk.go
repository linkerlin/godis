package probabilistic

// Faithful Go port of RedisBloom's Top-K "heavy keeper" (topk.c, Ariel
// Shtul): count-min-style bucket arrays with probabilistic decay, a
// fingerprint, and a min-heap of size k. Hashing is MurmurHash2 (32-bit)
// per row; decay draws use SplitMix64. Ported so bucket layout, eviction,
// and drop reporting match redis:8.10-alpine. The only approximation:
// TOPK.LIST tie order (real = musl qsort permutation; Go sort is stable —
// same membership, ties may order differently).

import (
	"encoding/json"
	"math"
	"sort"
	"strconv"
)

const (
	topkGA               = 1919 // fingerprint hash seed (RedisBloom constant)
	topkDecayLookupTable = 256
)

// murmurHash2 is the canonical 32-bit MurmurHash2 (Austin Appleby), the
// exact variant RedisBloom uses (little-endian word loads).
func murmurHash2(data []byte, seed uint32) uint32 {
	const m = 0x5bd1e995
	const r = 24

	length := uint32(len(data))
	h := seed ^ length

	i := 0
	for ; i+4 <= len(data); i += 4 {
		k := uint32(data[i]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24
		k *= m
		k ^= k >> r
		k *= m

		h *= m
		h ^= k
	}
	switch len(data) - i {
	case 3:
		h ^= uint32(data[i+2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[i+1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[i])
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}

// topkRandom is RedisBloom's SplitMix64 to double in [0,1).
func topkRandom(rngState *uint64) float64 {
	*rngState += 0x9e3779b97f4a7c15
	z := *rngState
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	z ^= z >> 31
	return float64(z>>11) * (1.0 / 9007199254740992.0)
}

type hkBucket struct {
	Fp    uint32 `json:"fp"`
	Count uint32 `json:"c"`
}

type heapBucket struct {
	Fp      uint32 `json:"fp"`
	ItemLen uint32 `json:"l"`
	Item    string `json:"i"`
	Count   uint32 `json:"c"`
}

// TopK is the heavy-keeper structure (RedisBloom-compatible).
type TopK struct {
	K     uint32       `json:"k"`
	Width uint32       `json:"w"`
	Depth uint32       `json:"d"`
	Decay float64      `json:"dec"`
	Data  []hkBucket   `json:"data"`
	Heap  []heapBucket `json:"heap"`

	lookup [topkDecayLookupTable]float64
}

// NewTopK creates a new Top-K with real defaults (width 8, depth 7, decay 0.9).
func NewTopK(k int) *TopK {
	return NewTopKOpts(k, 8, 7, 0.9)
}

// NewTopKOpts creates Top-K with explicit parameters (invalid values fall
// back to defaults after the handler's own validation).
func NewTopKOpts(k, width, depth int, decay float64) *TopK {
	if width <= 0 {
		width = 8
	}
	if depth <= 0 {
		depth = 7
	}
	if decay <= 0 || decay > 1 {
		decay = 0.9
	}
	tk := &TopK{
		K:     uint32(k),
		Width: uint32(width),
		Depth: uint32(depth),
		Decay: decay,
		Data:  make([]hkBucket, width*depth),
		Heap:  make([]heapBucket, k),
	}
	for i := 0; i < topkDecayLookupTable; i++ {
		tk.lookup[i] = math.Pow(decay, float64(i))
	}
	return tk
}

// heapifyDown mirrors the C sift-down exactly (min-heap on count).
func heapifyDown(array []heapBucket, start int) {
	child := start
	if len(array) < 2 || (len(array)-2)/2 < child {
		return
	}
	child = 2*child + 1
	if child+1 < len(array) && array[child].Count > array[child+1].Count {
		child++
	}
	if array[child].Count > array[start].Count {
		return
	}
	top := array[start]
	for {
		array[start] = array[child]
		start = child
		if (len(array)-2)/2 < child {
			break
		}
		child = 2*child + 1
		if child+1 < len(array) && array[child].Count > array[child+1].Count {
			child++
		}
		if !(array[child].Count < top.Count) {
			break
		}
	}
	array[start] = top
}

func (tk *TopK) checkExistInHeap(item []byte) int {
	fp := murmurHash2(item, topkGA)
	for i := len(tk.Heap) - 1; i >= 0; i-- {
		b := &tk.Heap[i]
		if b.Fp == fp && b.ItemLen == uint32(len(item)) && b.Item == string(item) {
			return i
		}
	}
	return -1
}

// AddSeeded inserts an item (heavy keeper). rngState is advanced in place;
// the caller seeds it per command (random for TOPK.ADD, fixed for
// replication). Returns the expelled item when a full heap head was
// replaced (empty-slot fills report no drop, like the C code).
func (tk *TopK) AddSeeded(item []byte, increment uint32, rngState *uint64) (string, bool) {
	var maxCount uint32
	fp := murmurHash2(item, topkGA)
	heapMin := tk.Heap[0].Count

	for i := uint32(0); i < tk.Depth; i++ {
		loc := murmurHash2(item, i) % tk.Width
		b := &tk.Data[i*tk.Width+loc]
		switch {
		case b.Count == 0:
			b.Fp = fp
			b.Count = increment
			if b.Count > maxCount {
				maxCount = b.Count
			}
		case b.Fp == fp:
			b.Count += increment
			if b.Count > maxCount {
				maxCount = b.Count
			}
		default:
			for localIncr := increment; localIncr > 0; localIncr-- {
				var decay float64
				if b.Count < topkDecayLookupTable {
					decay = tk.lookup[b.Count]
				} else {
					decay = math.Pow(tk.lookup[topkDecayLookupTable-1],
						float64(b.Count/(topkDecayLookupTable-1))) *
						tk.lookup[b.Count%(topkDecayLookupTable-1)]
				}
				if topkRandom(rngState) < decay {
					b.Count--
					if b.Count == 0 {
						b.Fp = fp
						b.Count = localIncr
						if b.Count > maxCount {
							maxCount = b.Count
						}
						break
					}
				}
			}
		}
	}

	if maxCount >= heapMin {
		if idx := tk.checkExistInHeap(item); idx >= 0 {
			tk.Heap[idx].Count = maxCount // may have decayed, not max of two
			heapifyDown(tk.Heap, idx)
		} else {
			expelled := tk.Heap[0].Item
			tk.Heap[0] = heapBucket{
				Count:   maxCount,
				Fp:      fp,
				Item:    string(item),
				ItemLen: uint32(len(item)),
			}
			heapifyDown(tk.Heap, 0)
			if expelled != "" {
				return expelled, true
			}
		}
	}
	return "", false
}

// Query reports whether an item is in the top-k heap.
func (tk *TopK) Query(item []byte) bool {
	return tk.checkExistInHeap(item) >= 0
}

// Count returns the heavy-keeper estimate: max bucket count across rows
// whose fingerprint matches, gated by the heap minimum when in the heap.
func (tk *TopK) Count(item []byte) uint64 {
	fp := murmurHash2(item, topkGA)
	heapMin := tk.Heap[0].Count
	heapIdx := tk.checkExistInHeap(item)
	var res uint32
	for i := uint32(0); i < tk.Depth; i++ {
		loc := murmurHash2(item, i) % tk.Width
		b := &tk.Data[i*tk.Width+loc]
		if b.Fp == fp && (heapIdx < 0 || b.Count >= heapMin) {
			if b.Count > res {
				res = b.Count
			}
		}
	}
	return uint64(res)
}

// ListEntry is one LIST result item.
type ListEntry struct {
	Item  string
	Count uint64
}

// List returns heap entries sorted by count desc, stopping at the first
// empty slot. Tie order is Go-stable (real uses musl qsort — same
// membership, tie permutation may differ; documented approximation).
func (tk *TopK) List() []ListEntry {
	sorted := make([]heapBucket, len(tk.Heap))
	copy(sorted, tk.Heap)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Count > sorted[j].Count })
	out := make([]ListEntry, 0, len(sorted))
	for _, b := range sorted {
		if b.Count == 0 {
			break
		}
		out = append(out, ListEntry{Item: b.Item, Count: uint64(b.Count)})
	}
	return out
}

// Info returns the real 8.10 TOPK.INFO field set (k, width, depth, decay).
func (tk *TopK) Info() [][2]interface{} {
	return [][2]interface{}{
		{"k", int64(tk.K)},
		{"width", int64(tk.Width)},
		{"depth", int64(tk.Depth)},
		{"decay", strconv.FormatFloat(tk.Decay, 'g', -1, 64)},
	}
}

type topKWire struct {
	K     uint32       `json:"k"`
	Width uint32       `json:"w"`
	Depth uint32       `json:"d"`
	Decay float64      `json:"dec"`
	Data  []hkBucket   `json:"data"`
	Heap  []heapBucket `json:"heap"`
}

// EncodeJSON serializes Top-K for godis opaque DUMP/RESTORE.
func (tk *TopK) EncodeJSON() ([]byte, error) {
	return json.Marshal(topKWire{
		K:     tk.K,
		Width: tk.Width,
		Depth: tk.Depth,
		Decay: tk.Decay,
		Data:  tk.Data,
		Heap:  tk.Heap,
	})
}

// DecodeTopK restores a Top-K from EncodeJSON output.
func DecodeTopK(data []byte) (*TopK, error) {
	var w topKWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	tk := &TopK{
		K:     w.K,
		Width: w.Width,
		Depth: w.Depth,
		Decay: w.Decay,
		Data:  w.Data,
		Heap:  w.Heap,
	}
	for i := 0; i < topkDecayLookupTable; i++ {
		tk.lookup[i] = math.Pow(tk.Decay, float64(i))
	}
	return tk, nil
}
