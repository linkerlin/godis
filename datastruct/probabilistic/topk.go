package probabilistic

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
)

// TopK maintains the top-k frequent items
type TopK struct {
	k      int
	width  int
	depth  int
	decay  float64
	items  map[string]*TopKItem
	minHeap *topKHeap
}

// TopKItem represents an item in Top-K
type TopKItem struct {
	Item  string
	Count uint64
	Error uint64 // Over-estimation error
	index int    // Index in heap
}

// NewTopK creates a new Top-K structure
func NewTopK(k int) *TopK {
	return NewTopKOpts(k, 8, 7, 0.9)
}

// NewTopKOpts creates Top-K with RedisBloom width/depth/decay parameters.
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
	return &TopK{
		k:       k,
		width:   width,
		depth:   depth,
		decay:   decay,
		items:   make(map[string]*TopKItem),
		minHeap: &topKHeap{},
	}
}

// Add adds an item. If another item was dropped from the top-k to make room,
// dropped is that item's name and ok is true.
func (tk *TopK) Add(item []byte) (dropped string, ok bool) {
	itemStr := string(item)

	// Check if item already exists
	if existing, found := tk.items[itemStr]; found {
		existing.Count++
		if existing.index >= 0 {
			tk.minHeap.Fix(existing.index)
		}
		return "", false
	}

	// New item
	newItem := &TopKItem{
		Item:  itemStr,
		Count: 1,
		Error: 0,
		index: -1,
	}

	// If we haven't reached k items, just add
	if tk.minHeap.Len() < tk.k {
		tk.items[itemStr] = newItem
		heap.Push(tk.minHeap, newItem)
		return "", false
	}

	// Check if this item should replace the minimum
	minItem := (*tk.minHeap)[0]
	if 1 > minItem.Count {
		droppedName := minItem.Item
		delete(tk.items, minItem.Item)
		tk.items[itemStr] = newItem
		newItem.Error = minItem.Count
		heap.Pop(tk.minHeap)
		heap.Push(tk.minHeap, newItem)
		return droppedName, true
	}

	// Item not in top-k, just track it
	tk.items[itemStr] = newItem
	return "", false
}

// Query returns the count for an item
func (tk *TopK) Query(item []byte) (uint64, uint64, bool) {
	itemStr := string(item)
	if item, ok := tk.items[itemStr]; ok {
		return item.Count, item.Error, true
	}
	return 0, 0, false
}

// List returns the top-k items
func (tk *TopK) List() []*TopKItem {
	result := make([]*TopKItem, tk.minHeap.Len())
	copy(result, *tk.minHeap)
	
	// Sort by count descending
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	
	return result
}

// Info returns Top-K info
func (tk *TopK) Info() map[string]interface{} {
	return map[string]interface{}{
		"k":     tk.k,
		"width": tk.width,
		"depth": tk.depth,
		"decay": tk.decay,
		"size":  len(tk.items),
		"added": tk.minHeap.Len(),
	}
}

// topKHeap implements a min-heap for TopKItem
type topKHeap []*TopKItem

func (h topKHeap) Len() int           { return len(h) }
func (h topKHeap) Less(i, j int) bool { return h[i].Count < h[j].Count }
func (h topKHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *topKHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*TopKItem)
	item.index = n
	*h = append(*h, item)
}

func (h *topKHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[:n-1]
	return item
}

func (h *topKHeap) Fix(i int) {
	heap.Fix(h, i)
}

// hash for TopK
func (tk *TopK) hash(item []byte) uint64 {
	h := fnv.New64a()
	h.Write(item)
	return h.Sum64()
}

type topKItemWire struct {
	Item  string `json:"i"`
	Count uint64 `json:"c"`
	Error uint64 `json:"e"`
}

type topKWire struct {
	K     int            `json:"k"`
	Width int            `json:"w"`
	Depth int            `json:"d"`
	Decay float64        `json:"decay"`
	Items []topKItemWire `json:"items"`
}

// EncodeJSON serializes Top-K for Godis opaque DUMP/RESTORE.
func (tk *TopK) EncodeJSON() ([]byte, error) {
	items := make([]topKItemWire, 0, len(tk.items))
	for _, it := range tk.items {
		items = append(items, topKItemWire{Item: it.Item, Count: it.Count, Error: it.Error})
	}
	return json.Marshal(topKWire{
		K:     tk.k,
		Width: tk.width,
		Depth: tk.depth,
		Decay: tk.decay,
		Items: items,
	})
}

// DecodeTopK restores Top-K from EncodeJSON output.
func DecodeTopK(data []byte) (*TopK, error) {
	var w topKWire
	if err := json.Unmarshal(data, &w); err != nil {
		return nil, err
	}
	if w.K <= 0 {
		return nil, fmt.Errorf("invalid topk data")
	}
	tk := NewTopKOpts(w.K, w.Width, w.Depth, w.Decay)
	rankedItems := make([]*TopKItem, 0, len(w.Items))
	for _, it := range w.Items {
		item := &TopKItem{Item: it.Item, Count: it.Count, Error: it.Error, index: -1}
		tk.items[it.Item] = item
		rankedItems = append(rankedItems, item)
	}
	sort.Slice(rankedItems, func(i, j int) bool {
		return rankedItems[i].Count > rankedItems[j].Count
	})
	limit := tk.k
	if limit > len(rankedItems) {
		limit = len(rankedItems)
	}
	for i := 0; i < limit; i++ {
		heap.Push(tk.minHeap, rankedItems[i])
	}
	return tk, nil
}
