package vector

import (
	"container/heap"
	"math"
	"math/rand"
	"time"
)

// Default HNSW parameters aligned with common Redis Vector Set defaults.
const (
	defaultHNSWM              = 16
	defaultHNSWEfConstruction = 200
	defaultHNSWEfSearch       = 50
)

// hnswNode is one element in the hierarchical NSW graph.
type hnswNode struct {
	id        string
	uid       uint64
	level     int
	neighbors [][]string // neighbors[layer] lists neighbor element ids
}

// HNSW is a Hierarchical Navigable Small World graph over vector ids.
// Distance is always "lower is closer"; callers pass a distFn.
type HNSW struct {
	m              int
	mMax0          int
	efConstruction int
	efSearch       int
	ml             float64
	entryPoint     string
	maxLevel       int
	nodes          map[string]*hnswNode
	nextUID        uint64
	rng            *rand.Rand
}

// NewHNSW creates an empty HNSW index. m / efConstruction <= 0 use defaults.
func NewHNSW(m, efConstruction int) *HNSW {
	if m <= 0 {
		m = defaultHNSWM
	}
	if efConstruction <= 0 {
		efConstruction = defaultHNSWEfConstruction
	}
	return &HNSW{
		m:              m,
		mMax0:          2 * m,
		efConstruction: efConstruction,
		efSearch:       defaultHNSWEfSearch,
		ml:             1.0 / math.Log(float64(m)),
		maxLevel:       -1,
		nodes:          make(map[string]*hnswNode),
		rng:            rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// M returns the HNSW M parameter.
func (h *HNSW) M() int { return h.m }

// EfConstruction returns the construction ef.
func (h *HNSW) EfConstruction() int { return h.efConstruction }

// SetEfConstruction updates construction ef (used by subsequent inserts).
func (h *HNSW) SetEfConstruction(ef int) {
	if ef > 0 {
		h.efConstruction = ef
	}
}

// SetEfSearch updates the default search ef.
func (h *HNSW) SetEfSearch(ef int) {
	if ef > 0 {
		h.efSearch = ef
	}
}

// MaxLevel returns the current graph max level (-1 if empty).
func (h *HNSW) MaxLevel() int { return h.maxLevel }

// MaxNodeUID returns the highest assigned node uid (0 if none).
func (h *HNSW) MaxNodeUID() uint64 {
	if h.nextUID == 0 {
		return 0
	}
	return h.nextUID
}

// Len returns the number of indexed nodes.
func (h *HNSW) Len() int { return len(h.nodes) }

// Links returns neighbor ids per layer for element id (layer 0 .. node.level).
// ok is false when the element is not in the graph.
func (h *HNSW) Links(id string) (layers [][]string, ok bool) {
	n, ok := h.nodes[id]
	if !ok {
		return nil, false
	}
	layers = make([][]string, n.level+1)
	for lc := 0; lc <= n.level; lc++ {
		if lc < len(n.neighbors) {
			layers[lc] = append([]string(nil), n.neighbors[lc]...)
		}
	}
	return layers, true
}

func (h *HNSW) randomLevel() int {
	// level ~ Geometric; P(level >= l) = (1/M)^l in the classic formulation.
	r := h.rng.Float64()
	if r < 1e-9 {
		r = 1e-9
	}
	return int(math.Floor(-math.Log(r) * h.ml))
}

// Insert adds id into the graph. dist(a,b) must be symmetric and lower=closer.
// If id already exists it is removed first (update path).
func (h *HNSW) Insert(id string, dist func(a, b string) float32) {
	if _, exists := h.nodes[id]; exists {
		h.Delete(id)
	}

	level := h.randomLevel()
	h.nextUID++
	node := &hnswNode{
		id:        id,
		uid:       h.nextUID,
		level:     level,
		neighbors: make([][]string, level+1),
	}

	if h.entryPoint == "" || len(h.nodes) == 0 {
		h.nodes[id] = node
		h.entryPoint = id
		h.maxLevel = level
		return
	}

	ep := h.entryPoint
	// Greedy search from top layer down to level+1.
	for lc := h.maxLevel; lc > level; lc-- {
		ep = h.searchLayerNearest(id, ep, lc, dist)
	}

	ef := h.efConstruction
	for lc := min(level, h.maxLevel); lc >= 0; lc-- {
		candidates := h.searchLayer(id, ep, ef, lc, dist)
		maxConn := h.m
		if lc == 0 {
			maxConn = h.mMax0
		}
		neighbors := h.selectNeighbors(candidates, maxConn)
		node.neighbors[lc] = neighbors

		for _, nb := range neighbors {
			h.addLink(nb, id, lc, dist, maxConn)
		}
		if len(candidates) > 0 {
			ep = candidates[0].id
		}
	}

	h.nodes[id] = node
	if level > h.maxLevel {
		h.maxLevel = level
		h.entryPoint = id
	}
}

// Delete removes id from the graph and cleans neighbor lists.
func (h *HNSW) Delete(id string) {
	node, ok := h.nodes[id]
	if !ok {
		return
	}
	for lc := 0; lc <= node.level; lc++ {
		for _, nb := range node.neighbors[lc] {
			h.removeLink(nb, id, lc)
		}
	}
	delete(h.nodes, id)

	if h.entryPoint == id {
		h.entryPoint = ""
		h.maxLevel = -1
		for _, n := range h.nodes {
			if n.level > h.maxLevel {
				h.maxLevel = n.level
				h.entryPoint = n.id
			}
		}
	}
}

// Search returns up to k element ids closest to queryID-or-query via distFn.
// queryKey is a synthetic key used only for distance lookups (may equal a real id).
func (h *HNSW) Search(queryKey string, k, ef int, dist func(a, b string) float32) []string {
	if k <= 0 || len(h.nodes) == 0 || h.entryPoint == "" {
		return nil
	}
	if ef < k {
		ef = k
	}
	if ef < h.efSearch {
		ef = h.efSearch
	}

	ep := h.entryPoint
	for lc := h.maxLevel; lc > 0; lc-- {
		ep = h.searchLayerNearest(queryKey, ep, lc, dist)
	}
	candidates := h.searchLayer(queryKey, ep, ef, 0, dist)
	if len(candidates) > k {
		candidates = candidates[:k]
	}
	out := make([]string, len(candidates))
	for i, c := range candidates {
		out[i] = c.id
	}
	return out
}

func (h *HNSW) searchLayerNearest(query, ep string, lc int, dist func(a, b string) float32) string {
	best := ep
	bestDist := dist(query, ep)
	changed := true
	for changed {
		changed = false
		n, ok := h.nodes[best]
		if !ok || lc >= len(n.neighbors) {
			break
		}
		for _, nb := range n.neighbors[lc] {
			if _, ok := h.nodes[nb]; !ok {
				continue
			}
			d := dist(query, nb)
			if d < bestDist {
				bestDist = d
				best = nb
				changed = true
			}
		}
	}
	return best
}

type hnswCandidate struct {
	id   string
	dist float32
}

// searchLayer performs ef-bounded beam search at one layer.
// Returns candidates sorted by ascending distance.
func (h *HNSW) searchLayer(query, ep string, ef, lc int, dist func(a, b string) float32) []hnswCandidate {
	visited := make(map[string]struct{}, ef*2)
	visited[ep] = struct{}{}

	d0 := dist(query, ep)
	candidates := &hnswMinHeap{{id: ep, dist: d0}} // closest first
	heap.Init(candidates)
	w := &hnswMaxHeap{{id: ep, dist: d0}} // farthest of result set first
	heap.Init(w)

	for candidates.Len() > 0 {
		c := heap.Pop(candidates).(hnswCandidate)
		farthest := (*w)[0]
		if c.dist > farthest.dist {
			break
		}
		node, ok := h.nodes[c.id]
		if !ok || lc >= len(node.neighbors) {
			continue
		}
		for _, nb := range node.neighbors[lc] {
			if _, seen := visited[nb]; seen {
				continue
			}
			if _, ok := h.nodes[nb]; !ok {
				continue
			}
			visited[nb] = struct{}{}
			d := dist(query, nb)
			if w.Len() < ef || d < (*w)[0].dist {
				heap.Push(candidates, hnswCandidate{id: nb, dist: d})
				heap.Push(w, hnswCandidate{id: nb, dist: d})
				if w.Len() > ef {
					heap.Pop(w)
				}
			}
		}
	}

	out := make([]hnswCandidate, w.Len())
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(w).(hnswCandidate)
	}
	return out
}

func (h *HNSW) selectNeighbors(candidates []hnswCandidate, m int) []string {
	if len(candidates) <= m {
		out := make([]string, len(candidates))
		for i, c := range candidates {
			out[i] = c.id
		}
		return out
	}
	out := make([]string, m)
	for i := 0; i < m; i++ {
		out[i] = candidates[i].id
	}
	return out
}

func (h *HNSW) addLink(from, to string, lc int, dist func(a, b string) float32, maxConn int) {
	n, ok := h.nodes[from]
	if !ok {
		return
	}
	for len(n.neighbors) <= lc {
		n.neighbors = append(n.neighbors, nil)
	}
	for _, existing := range n.neighbors[lc] {
		if existing == to {
			return
		}
	}
	n.neighbors[lc] = append(n.neighbors[lc], to)
	if len(n.neighbors[lc]) <= maxConn {
		return
	}
	// Prune to maxConn closest neighbors of `from`.
	type pair struct {
		id   string
		dist float32
	}
	tmp := make([]pair, 0, len(n.neighbors[lc]))
	for _, nb := range n.neighbors[lc] {
		if _, ok := h.nodes[nb]; !ok && nb != to {
			continue
		}
		tmp = append(tmp, pair{id: nb, dist: dist(from, nb)})
	}
	// Simple selection: keep m closest.
	for i := 0; i < len(tmp); i++ {
		best := i
		for j := i + 1; j < len(tmp); j++ {
			if tmp[j].dist < tmp[best].dist {
				best = j
			}
		}
		tmp[i], tmp[best] = tmp[best], tmp[i]
	}
	if len(tmp) > maxConn {
		tmp = tmp[:maxConn]
	}
	n.neighbors[lc] = n.neighbors[lc][:0]
	for _, p := range tmp {
		n.neighbors[lc] = append(n.neighbors[lc], p.id)
	}
}

func (h *HNSW) removeLink(from, to string, lc int) {
	n, ok := h.nodes[from]
	if !ok || lc >= len(n.neighbors) {
		return
	}
	ns := n.neighbors[lc]
	for i, nb := range ns {
		if nb == to {
			n.neighbors[lc] = append(ns[:i], ns[i+1:]...)
			return
		}
	}
}

// --- heaps for beam search ---

type hnswMinHeap []hnswCandidate // min-dist at [0]
type hnswMaxHeap []hnswCandidate // max-dist at [0]

func (h hnswMinHeap) Len() int            { return len(h) }
func (h hnswMinHeap) Less(i, j int) bool  { return h[i].dist < h[j].dist }
func (h hnswMinHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *hnswMinHeap) Push(x interface{}) { *h = append(*h, x.(hnswCandidate)) }
func (h *hnswMinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (h hnswMaxHeap) Len() int            { return len(h) }
func (h hnswMaxHeap) Less(i, j int) bool  { return h[i].dist > h[j].dist }
func (h hnswMaxHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *hnswMaxHeap) Push(x interface{}) { *h = append(*h, x.(hnswCandidate)) }
func (h *hnswMaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}
