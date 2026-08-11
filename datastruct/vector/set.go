package vector

import (
	"container/heap"
	"fmt"
	"math"
	"sort"
	"sync"
)

// VectorItem represents a vector with metadata
type VectorItem struct {
	ID       string
	Vector   *Vector
	Metadata map[string]string
	// Attributes is Redis Vector Set JSON attributes (VSETATTR/VGETATTR).
	Attributes string
	// Q8 / Q8Range hold true int8 quantization when the set uses QuantQ8.
	// Vector.Data is the dequantized approximation used for HNSW / VSIM.
	Q8      []int8
	Q8Range float32
}

// SearchResult represents a search result with similarity score
type SearchResult struct {
	ID         string
	Vector     *Vector
	Score      float32 // Similarity score (higher is more similar for cosine)
	Distance   float32 // For distance-based metrics
	Metadata   map[string]string
	Attributes string // Redis VSETATTR JSON
}

const hnswQueryKey = "\x00__hnsw_query__"

// VectorSet is a collection of vectors supporting similarity search
// via an in-memory HNSW graph. Default storage is float32; Q8 stores int8
// codes (search uses dequantized f32).
type VectorSet struct {
	vectors   map[string]*VectorItem
	dimension int
	mu        sync.RWMutex

	hnsw *HNSW
	// pendingM / pendingEf are applied on first Add (or ConfigureHNSW).
	pendingM  int
	pendingEf int
	// quant is locked after the first element is inserted.
	quant     QuantMode
	quantSet  bool
}

// NewVectorSet creates a new VectorSet with default HNSW parameters.
func NewVectorSet() *VectorSet {
	return &VectorSet{
		vectors: make(map[string]*VectorItem),
		hnsw:    NewHNSW(defaultHNSWM, defaultHNSWEfConstruction),
	}
}

// ConfigureHNSW sets M and/or efConstruction. M is locked after the first
// element is inserted; efConstruction may be updated on later VADD EF.
func (vs *VectorSet) ConfigureHNSW(m, efConstruction int) {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if m > 0 {
		vs.pendingM = m
	}
	if efConstruction > 0 {
		vs.pendingEf = efConstruction
	}
	if vs.hnsw == nil || vs.hnsw.Len() == 0 {
		mm, ef := vs.pendingM, vs.pendingEf
		if mm <= 0 {
			mm = defaultHNSWM
		}
		if ef <= 0 {
			ef = defaultHNSWEfConstruction
		}
		vs.hnsw = NewHNSW(mm, ef)
		return
	}
	if efConstruction > 0 {
		vs.hnsw.SetEfConstruction(efConstruction)
	}
}

// SetQuantMode selects quantization before the first insert. After the set has
// elements, only the same mode is accepted (Redis Q8/NOQUANT format check).
// Returns false if the mode conflicts with an already-populated set.
func (vs *VectorSet) SetQuantMode(mode QuantMode) bool {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if len(vs.vectors) > 0 || vs.quantSet {
		return vs.quant == mode
	}
	vs.quant = mode
	vs.quantSet = true
	return true
}

// QuantMode returns the set's quantization mode.
func (vs *VectorSet) QuantMode() QuantMode {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.quant
}

// HNSWInfo returns graph metadata for VINFO.
func (vs *VectorSet) HNSWInfo() (m, efConstruction int, maxUID uint64, maxLevel int) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	if vs.hnsw == nil {
		return 0, 0, 0, 0
	}
	ml := vs.hnsw.MaxLevel()
	if ml < 0 {
		ml = 0
	}
	return vs.hnsw.M(), vs.hnsw.EfConstruction(), vs.hnsw.MaxNodeUID(), ml
}

// HNSWLinks returns per-layer neighbor ids for VLINKS.
func (vs *VectorSet) HNSWLinks(id string) (layers [][]string, ok bool) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	if vs.hnsw == nil {
		return nil, false
	}
	return vs.hnsw.Links(id)
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
	// Lock default f32 when first element arrives without an explicit SetQuantMode.
	if !vs.quantSet {
		vs.quantSet = true
	}

	if vs.hnsw == nil {
		m, ef := vs.pendingM, vs.pendingEf
		if m <= 0 {
			m = defaultHNSWM
		}
		if ef <= 0 {
			ef = defaultHNSWEfConstruction
		}
		vs.hnsw = NewHNSW(m, ef)
	}

	_, exists := vs.vectors[id]
	store := vec
	var q8 []int8
	var qrange float32
	if vs.quant == QuantQ8 {
		q8, qrange = QuantizeQ8(vec.Data)
		store = NewVector(DequantizeQ8(q8, qrange))
	}
	item := &VectorItem{
		ID:       id,
		Vector:   store,
		Metadata: metadata,
		Q8:       q8,
		Q8Range:  qrange,
	}
	if exists {
		if old := vs.vectors[id]; old != nil {
			item.Attributes = old.Attributes
		}
	}
	vs.vectors[id] = item
	vs.hnsw.Insert(id, vs.distFnLocked(CosineSimilarity))
	return !exists
}

// AddQ8 inserts a pre-quantized vector (used by opaque restore). Does not
// re-quantize; Vector.Data is the dequantized approximation for search.
func (vs *VectorSet) AddQ8(id string, codes []int8, qrange float32, metadata map[string]string) bool {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if !vs.quantSet {
		vs.quant = QuantQ8
		vs.quantSet = true
	} else if vs.quant != QuantQ8 {
		return false
	}
	dim := len(codes)
	if vs.dimension == 0 {
		vs.dimension = dim
	} else if vs.dimension != dim {
		return false
	}
	if vs.hnsw == nil {
		m, ef := vs.pendingM, vs.pendingEf
		if m <= 0 {
			m = defaultHNSWM
		}
		if ef <= 0 {
			ef = defaultHNSWEfConstruction
		}
		vs.hnsw = NewHNSW(m, ef)
	}
	_, exists := vs.vectors[id]
	item := &VectorItem{
		ID:       id,
		Vector:   NewVector(DequantizeQ8(codes, qrange)),
		Metadata: metadata,
		Q8:       append([]int8(nil), codes...),
		Q8Range:  qrange,
	}
	if exists {
		if old := vs.vectors[id]; old != nil {
			item.Attributes = old.Attributes
		}
	}
	vs.vectors[id] = item
	vs.hnsw.Insert(id, vs.distFnLocked(CosineSimilarity))
	return !exists
}

// SetAttributes sets JSON attributes on an element. Returns false if id missing.
func (vs *VectorSet) SetAttributes(id string, json string) bool {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	item, ok := vs.vectors[id]
	if !ok {
		return false
	}
	item.Attributes = json
	return true
}

// GetAttributes returns JSON attributes; ok false if id missing.
func (vs *VectorSet) GetAttributes(id string) (string, bool) {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	item, ok := vs.vectors[id]
	if !ok {
		return "", false
	}
	return item.Attributes, true
}

// SortedIDs returns element ids sorted lexicographically.
func (vs *VectorSet) SortedIDs() []string {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	ids := make([]string, 0, len(vs.vectors))
	for id := range vs.vectors {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
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
		if vs.hnsw != nil {
			vs.hnsw.Delete(id)
		}
		delete(vs.vectors, id)
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

// SearchWithMetric performs k-NN search with specified metric (default ef).
func (vs *VectorSet) SearchWithMetric(query *Vector, k int, metric SearchMetric) []*SearchResult {
	return vs.SearchWithMetricEF(query, k, metric, 0, false)
}

// SearchWithMetricEF performs k-NN with optional ef override.
// If exact is true (VSIM TRUTH), falls back to a full scan.
func (vs *VectorSet) SearchWithMetricEF(query *Vector, k int, metric SearchMetric, ef int, exact bool) []*SearchResult {
	vs.mu.RLock()
	defer vs.mu.RUnlock()

	if k <= 0 || len(vs.vectors) == 0 {
		return nil
	}

	// Brute force when requested, when the graph is missing, for non-cosine
	// metrics (graph is built on cosine distance), or when the set is tiny.
	if exact || metric != CosineSimilarity || vs.hnsw == nil || vs.hnsw.Len() == 0 || len(vs.vectors) <= 64 {
		return vs.bruteSearchLocked(query, k, metric)
	}

	ids := vs.hnsw.Search(hnswQueryKey, k, ef, vs.queryDistFnLocked(query, metric))
	results := make([]*SearchResult, 0, len(ids))
	for _, id := range ids {
		item, ok := vs.vectors[id]
		if !ok {
			continue
		}
		score, distance := scoreDistance(item.Vector, query, metric)
		results = append(results, &SearchResult{
			ID:         id,
			Vector:     item.Vector,
			Score:      score,
			Distance:   distance,
			Metadata:   item.Metadata,
			Attributes: item.Attributes,
		})
	}
	return results
}

func scoreDistance(item, query *Vector, metric SearchMetric) (score, distance float32) {
	switch metric {
	case CosineSimilarity:
		score = item.CosineSimilarity(query)
		distance = 1 - score
	case EuclideanDistance:
		distance = item.EuclideanDistance(query)
		score = -distance
	case DotProduct:
		score = item.DotProduct(query)
		distance = -score
	}
	return
}

func (vs *VectorSet) bruteSearchLocked(query *Vector, k int, metric SearchMetric) []*SearchResult {
	h := &searchResultHeap{}
	heap.Init(h)

	for id, item := range vs.vectors {
		score, distance := scoreDistance(item.Vector, query, metric)
		result := &SearchResult{
			ID:         id,
			Vector:     item.Vector,
			Score:      score,
			Distance:   distance,
			Metadata:   item.Metadata,
			Attributes: item.Attributes,
		}
		if h.Len() < k {
			heap.Push(h, result)
		} else if (*h)[0].Score < score {
			heap.Pop(h)
			heap.Push(h, result)
		}
	}

	results := make([]*SearchResult, h.Len())
	for i := h.Len() - 1; i >= 0; i-- {
		results[i] = heap.Pop(h).(*SearchResult)
	}
	return results
}

// distFnLocked returns a pairwise distance over element ids (caller holds lock).
func (vs *VectorSet) distFnLocked(metric SearchMetric) func(a, b string) float32 {
	return func(a, b string) float32 {
		ia, oa := vs.vectors[a]
		ib, ob := vs.vectors[b]
		if !oa || !ob {
			return float32(math.MaxFloat32)
		}
		_, distance := scoreDistance(ia.Vector, ib.Vector, metric)
		return distance
	}
}

// queryDistFnLocked distances a synthetic query key against element ids.
func (vs *VectorSet) queryDistFnLocked(query *Vector, metric SearchMetric) func(a, b string) float32 {
	return func(a, b string) float32 {
		resolve := func(id string) *Vector {
			if id == hnswQueryKey {
				return query
			}
			if item, ok := vs.vectors[id]; ok {
				return item.Vector
			}
			return nil
		}
		va, vb := resolve(a), resolve(b)
		if va == nil || vb == nil {
			return float32(math.MaxFloat32)
		}
		_, distance := scoreDistance(va, vb, metric)
		return distance
	}
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
	m, ef := defaultHNSWM, defaultHNSWEfConstruction
	if vs.pendingM > 0 {
		m = vs.pendingM
	}
	if vs.pendingEf > 0 {
		ef = vs.pendingEf
	}
	vs.hnsw = NewHNSW(m, ef)
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
