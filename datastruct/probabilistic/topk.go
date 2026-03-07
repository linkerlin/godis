package probabilistic

import (
	"container/heap"
	"hash/fnv"
)

// TopK maintains the top-k frequent items
type TopK struct {
	k         int
	items     map[string]*TopKItem
	minHeap   *topKHeap
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
	return &TopK{
		k:       k,
		items:   make(map[string]*TopKItem),
		minHeap: &topKHeap{},
	}
}

// Add adds an item
func (tk *TopK) Add(item []byte) *TopKItem {
	itemStr := string(item)
	
	// Check if item already exists
	if existing, ok := tk.items[itemStr]; ok {
		existing.Count++
		if existing.index >= 0 {
			tk.minHeap.Fix(existing.index)
		}
		return existing
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
		return newItem
	}
	
	// Check if this item should replace the minimum
	minItem := (*tk.minHeap)[0]
	if 1 > minItem.Count {
		// Remove min and add new
		delete(tk.items, minItem.Item)
		tk.items[itemStr] = newItem
		newItem.Error = minItem.Count
		heap.Pop(tk.minHeap)
		heap.Push(tk.minHeap, newItem)
		return newItem
	}
	
	// Item not in top-k, just track it
	tk.items[itemStr] = newItem
	return newItem
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
