package vector

import (
	"container/heap"
	"fmt"
	"sort"
	"sync"
)

// VectorItem represents a vector with metadata
type VectorItem struct {
	ID       string
	Vector   *Vector
	Metadata map[string]string
}

// SearchResult represents a search result with similarity score
type SearchResult struct {
	ID         string
	Vector     *Vector
	Score      float32 // Similarity score (higher is more similar for cosine)
	Distance   float32 // For distance-based metrics
	Metadata   map[string]string
}

// VectorSet is a collection of vectors supporting similarity search
type VectorSet struct {
	vectors   map[string]*VectorItem
	dimension int
	mu        sync.RWMutex
	
	// Index for approximate nearest neighbor search
	// Using simple flat index for now, can be upgraded to HNSW
	indexed   bool
}

// NewVectorSet creates a new VectorSet
func NewVectorSet() *VectorSet {
	return &VectorSet{
		vectors: make(map[string]*VectorItem),
		indexed: false,
	}
}

// Add adds a vector to the set
// Returns true if new, false if updated
func (vs *VectorSet) Add(id string, vec *Vector, metadata map[string]string) bool {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	
	// Validate dimension consistency
	if vs.dimension == 0 {
		vs.dimension = vec.Dim
	} else if vs.dimension != vec.Dim {
		return false // Dimension mismatch
	}
	
	_, exists := vs.vectors[id]
	vs.vectors[id] = &VectorItem{
		ID:       id,
		Vector:   vec,
		Metadata: metadata,
	}
	
	vs.indexed = false // Invalidate index
	return !exists
}

// Get retrieves a vector by ID
func (vs *VectorSet) Get(id string) (*VectorItem, bool) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	
	item, ok := vs.vectors[id]
	return item, ok
}

// Delete removes a vector by ID
// Returns true if deleted
func (vs *VectorSet) Delete(id string) bool {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	
	_, ok := vs.vectors[id]
	if ok {
		delete(vs.vectors, id)
		vs.indexed = false
	}
	return ok
}

// Len returns the number of vectors in the set
func (vs *VectorSet) Len() int {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return len(vs.vectors)
}

// Dimension returns the vector dimension
func (vs *VectorSet) Dimension() int {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.dimension
}

// Search performs k-nearest neighbor search using cosine similarity
// Returns the k most similar vectors to the query
func (vs *VectorSet) Search(query *Vector, k int) []*SearchResult {
	return vs.SearchWithMetric(query, k, CosineSimilarity)
}

// SearchMetric specifies the similarity metric type
type SearchMetric int

const (
	CosineSimilarity SearchMetric = iota
	EuclideanDistance
	DotProduct
)

// SearchWithMetric performs k-NN search with specified metric
func (vs *VectorSet) SearchWithMetric(query *Vector, k int, metric SearchMetric) []*SearchResult {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	
	if k <= 0 || len(vs.vectors) == 0 {
		return nil
	}
	
	// Use min-heap for efficient top-k
	h := &searchResultHeap{}
	heap.Init(h)
	
	for id, item := range vs.vectors {
		var score, distance float32
		
		switch metric {
		case CosineSimilarity:
			score = item.Vector.CosineSimilarity(query)
			distance = 1 - score
		case EuclideanDistance:
			distance = item.Vector.EuclideanDistance(query)
			score = -distance // Negative so higher is better
		case DotProduct:
			score = item.Vector.DotProduct(query)
			distance = -score
		}
		
		result := &SearchResult{
			ID:       id,
			Vector:   item.Vector,
			Score:    score,
			Distance: distance,
			Metadata: item.Metadata,
		}
		
		if h.Len() < k {
			heap.Push(h, result)
		} else if (*h)[0].Score < score {
			heap.Pop(h)
			heap.Push(h, result)
		}
	}
	
	// Extract results from heap (in reverse order)
	results := make([]*SearchResult, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		results[i] = heap.Pop(h).(*SearchResult)
	}
	
	return results
}

// SearchByID searches for similar vectors using an existing ID as query
func (vs *VectorSet) SearchByID(queryID string, k int, metric SearchMetric) []*SearchResult {
	item, ok := vs.Get(queryID)
	if !ok {
		return nil
	}
	return vs.SearchWithMetric(item.Vector, k, metric)
}

// RangeSearch finds all vectors within a radius (for Euclidean distance)
// or above a threshold (for cosine similarity)
func (vs *VectorSet) RangeSearch(query *Vector, threshold float32, metric SearchMetric) []*SearchResult {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	
	var results []*SearchResult
	
	for id, item := range vs.vectors {
		var score, distance float32
		pass := false
		
		switch metric {
		case CosineSimilarity:
			score = item.Vector.CosineSimilarity(query)
			distance = 1 - score
			pass = score >= threshold
		case EuclideanDistance:
			distance = item.Vector.EuclideanDistance(query)
			score = -distance
			pass = distance <= threshold
		case DotProduct:
			score = item.Vector.DotProduct(query)
			distance = -score
			pass = score >= threshold
		}
		
		if pass {
			results = append(results, &SearchResult{
				ID:       id,
				Vector:   item.Vector,
				Score:    score,
				Distance: distance,
				Metadata: item.Metadata,
			})
		}
	}
	
	// Sort by score descending
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})
	
	return results
}

// BatchSearch performs multiple searches efficiently
func (vs *VectorSet) BatchSearch(queries []*Vector, k int, metric SearchMetric) [][]*SearchResult {
	results := make([][]*SearchResult, len(queries))
	for i, query := range queries {
		results[i] = vs.SearchWithMetric(query, k, metric)
	}
	return results
}

// GetAllIDs returns all vector IDs
func (vs *VectorSet) GetAllIDs() []string {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	
	ids := make([]string, 0, len(vs.vectors))
	for id := range vs.vectors {
		ids = append(ids, id)
	}
	return ids
}

// Clear removes all vectors
func (vs *VectorSet) Clear() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	
	vs.vectors = make(map[string]*VectorItem)
	vs.dimension = 0
	vs.indexed = false
}

// ForEach iterates over all vectors
func (vs *VectorSet) ForEach(fn func(id string, item *VectorItem) bool) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	
	for id, item := range vs.vectors {
		if !fn(id, item) {
			break
		}
	}
}

// Centroid computes the centroid (average) of all vectors
func (vs *VectorSet) Centroid() *Vector {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	
	if len(vs.vectors) == 0 {
		return nil
	}
	
	centroid := make([]float32, vs.dimension)
	count := 0
	
	for _, item := range vs.vectors {
		for i, v := range item.Vector.Data {
			centroid[i] += v
		}
		count++
	}
	
	for i := range centroid {
		centroid[i] /= float32(count)
	}
	
	return NewVector(centroid)
}

// MinHeap implementation for top-k search
type searchResultHeap []*SearchResult

func (h searchResultHeap) Len() int           { return len(h) }
func (h searchResultHeap) Less(i, j int) bool { return h[i].Score < h[j].Score }
func (h searchResultHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *searchResultHeap) Push(x interface{}) {
	*h = append(*h, x.(*SearchResult))
}

func (h *searchResultHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// ParseVector parses a vector from string representation
// Format: "[f1,f2,f3,...]" or "f1 f2 f3 ..."
func ParseVector(s string) (*Vector, error) {
	// Simple implementation - assume space-separated values
	var values []float64
	
	// Try parsing as bracketed format [a,b,c]
	if len(s) > 2 && s[0] == '[' && s[len(s)-1] == ']' {
		s = s[1 : len(s)-1]
	}
	
	// Parse comma or space separated values
	var current float64
	var hasValue bool
	
	for i, ch := range s {
		if ch == ',' || ch == ' ' {
			if hasValue {
				values = append(values, current)
				current = 0
				hasValue = false
			}
			continue
		}
		if ch >= '0' && ch <= '9' || ch == '.' || ch == '-' || ch == '+' || ch == 'e' || ch == 'E' {
			// Parse number - simplified
			var j int
			for j = i; j < len(s); j++ {
				c := s[j]
				if !(c >= '0' && c <= '9' || c == '.' || c == '-' || c == '+' || c == 'e' || c == 'E') {
					break
				}
			}
			fmt.Sscanf(s[i:j], "%f", &current)
			hasValue = true
			break
		}
	}
	
	if hasValue {
		values = append(values, current)
	}
	
	return NewVectorFromFloat64(values), nil
}
