package redisearch

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	godisvector "github.com/linkerlin/godis/datastruct/vector"
)

// Vector algorithm identifiers (FT.CREATE ... VECTOR {FLAT|HNSW}).
const (
	VectorAlgoFlat  = "FLAT"
	VectorAlgoHNSW  = "HNSW"
	VectorAlgoVAMANA = "SVS-VAMANA" // 8.2+; OSS Redis runs an 8-bit scalar fallback
)

// Vector type identifiers (VECTOR ... TYPE <FLOAT32|FLOAT64|...>).
const (
	VectorTypeFloat32 = "FLOAT32"
	VectorTypeFloat64 = "FLOAT64"
	VectorTypeFloat16 = "FLOAT16"
	VectorTypeBFloat16 = "BFLOAT16"
	VectorTypeInt8    = "INT8"
	VectorTypeUint8   = "UINT8"
)

// Distance metric identifiers (VECTOR ... DISTANCE_METRIC <L2|IP|COSINE>).
const (
	VectorMetricL2     = "L2"
	VectorMetricIP     = "IP"
	VectorMetricCosine = "COSINE"
)

// VectorFieldConfig holds the parsed VECTOR field attributes from FT.CREATE.
// Required: Algorithm, Type, Dim, DistanceMetric. The rest are optional with
// Redis defaults applied in ParseVectorFieldConfig.
type VectorFieldConfig struct {
	Algorithm       string // FLAT | HNSW
	Type            string // FLOAT32 (primary), FLOAT64, INT8, ...
	Dim             int
	DistanceMetric  string // L2 | IP | COSINE
	M               int // HNSW: max outgoing edges per layer (default 16)
	EFConstruction  int // HNSW: build-time candidate pool (default 200)
	EFRuntime       int // HNSW: search-time candidate pool (default 10)
	InitialCap      int // preallocated slot count
	BlockSize       int // FLAT: block size (default 1024)
}

// ParseVectorFieldConfig parses the tokens following "field VECTOR" in a
// SCHEMA. The grammar is: VECTOR <ALGO> <count> <attr> <val> ... where count
// is the TOTAL number of attribute/value tokens that follow (i.e. 2 × number
// of pairs), matching the Redis convention. Required attrs are TYPE, DIM,
// DISTANCE_METRIC; M/EF_CONSTRUCTION/EF_RUNTIME/INITIAL_CAP/BLOCK_SIZE are
// optional. Returns an error if a required attr is missing or values are bad.
func ParseVectorFieldConfig(args [][]byte) (*VectorFieldConfig, int, error) {
	// args[0] = ALGO, args[1] = count (total remaining tokens), then count tokens.
	if len(args) < 2 {
		return nil, 0, fmt.Errorf("VECTOR field requires algorithm and attribute count")
	}
	algo := string(args[0])
	if algo != VectorAlgoFlat && algo != VectorAlgoHNSW && algo != VectorAlgoVAMANA {
		return nil, 0, fmt.Errorf("Invalid VECTOR algorithm '%s'", algo)
	}
	count, err := parseInt(string(args[1]))
	if err != nil || count < 0 || count%2 != 0 {
		return nil, 0, fmt.Errorf("Invalid VECTOR attribute count")
	}
	if len(args) < 2+count {
		return nil, 0, fmt.Errorf("VECTOR field declares %d attribute tokens but fewer were provided", count)
	}

	cfg := &VectorFieldConfig{
		Algorithm:      algo,
		Type:           VectorTypeFloat32,
		DistanceMetric: VectorMetricL2,
		M:              16,
		EFConstruction: 200,
		EFRuntime:      10,
		InitialCap:     0,
		BlockSize:      1024,
	}

	pairs := args[2 : 2+count]
	consumed := 2 + count
	for i := 0; i+1 < len(pairs); i += 2 {
		key := string(pairs[i])
		val := string(pairs[i+1])
		switch key {
		case "TYPE":
			switch val {
			case VectorTypeFloat32, VectorTypeFloat64, VectorTypeFloat16, VectorTypeBFloat16, VectorTypeInt8, VectorTypeUint8:
				cfg.Type = val
			default:
				return nil, 0, fmt.Errorf("Invalid VECTOR TYPE '%s'", val)
			}
		case "DIM":
			d, err := parseInt(val)
			if err != nil || d <= 0 {
				return nil, 0, fmt.Errorf("Invalid VECTOR DIM '%s'", val)
			}
			cfg.Dim = d
		case "DISTANCE_METRIC":
			switch val {
			case VectorMetricL2, VectorMetricIP, VectorMetricCosine:
				cfg.DistanceMetric = val
			default:
				return nil, 0, fmt.Errorf("Invalid VECTOR DISTANCE_METRIC '%s'", val)
			}
		case "COMPRESSION":
			// SVS-VAMANA compression modes (LVQ4/LVQ8/LeanVec...) are Intel
			// proprietary; OSS Redis falls back to 8-bit scalar. godis stores
			// full FLOAT32 which is strictly more accurate, so the mode is
			// accepted and ignored (the memory tradeoff is a non-goal here).
			// ponytail: add INT8 storage if memory-constrained deployments
			// require it.
		case "M":
			m, err := parseInt(val)
			if err != nil || m <= 0 {
				return nil, 0, fmt.Errorf("Invalid VECTOR M '%s'", val)
			}
			cfg.M = m
		case "EF_CONSTRUCTION":
			ef, err := parseInt(val)
			if err != nil || ef <= 0 {
				return nil, 0, fmt.Errorf("Invalid VECTOR EF_CONSTRUCTION '%s'", val)
			}
			cfg.EFConstruction = ef
		case "EF_RUNTIME":
			ef, err := parseInt(val)
			if err != nil || ef <= 0 {
				return nil, 0, fmt.Errorf("Invalid VECTOR EF_RUNTIME '%s'", val)
			}
			cfg.EFRuntime = ef
		case "INITIAL_CAP":
			c, err := parseInt(val)
			if err != nil || c < 0 {
				return nil, 0, fmt.Errorf("Invalid VECTOR INITIAL_CAP '%s'", val)
			}
			cfg.InitialCap = c
		case "BLOCK_SIZE":
			b, err := parseInt(val)
			if err != nil || b <= 0 {
				return nil, 0, fmt.Errorf("Invalid VECTOR BLOCK_SIZE '%s'", val)
			}
			cfg.BlockSize = b
		default:
			// Unknown attrs are ignored for forward-compat (Redis does the same
			// for attrs it doesn't recognize in some versions).
		}
	}

	if cfg.Dim <= 0 {
		return nil, 0, fmt.Errorf("VECTOR field requires DIM")
	}
	return cfg, consumed, nil
}

// FTVectorIndex stores the vectors for one VECTOR field of an FT index and
// answers KNN queries. FLAT algorithm brute-forces every vector; HNSW keeps a
// graph (datastruct/vector.HNSW) for approximate search, falling back to brute
// force when the graph is empty or the query is scoped to a candidate subset.
type FTVectorIndex struct {
	cfg     *VectorFieldConfig
	vectors map[string][]float32 // docID -> components
	hnsw    *godisvector.HNSW    // non-nil only when Algorithm == HNSW
	mu      sync.RWMutex
}

// NewFTVectorIndex creates an empty vector index for the given config.
func NewFTVectorIndex(cfg *VectorFieldConfig) *FTVectorIndex {
	vi := &FTVectorIndex{
		cfg:     cfg,
		vectors: make(map[string][]float32),
	}
	// HNSW and SVS-VAMANA are both graph-based approximate indexes; godis runs
	// VAMANA on the HNSW graph backend (OSS Redis likewise runs a scalar
	// fallback rather than the proprietary VAMANA implementation).
	if cfg.Algorithm == VectorAlgoHNSW || cfg.Algorithm == VectorAlgoVAMANA {
		vi.hnsw = godisvector.NewHNSW(cfg.M, cfg.EFConstruction)
	}
	return vi
}

// Config returns the field's vector configuration (read-only).
func (vi *FTVectorIndex) Config() *VectorFieldConfig { return vi.cfg }

// Len returns the number of indexed vectors.
func (vi *FTVectorIndex) Len() int {
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	return len(vi.vectors)
}

// DecodeVector parses a raw blob into a float32 component slice according to
// the field's declared TYPE. FLOAT32 is the common case; other types are
// decoded then widened to float32 for uniform distance math.
func (vi *FTVectorIndex) DecodeVector(blob []byte) ([]float32, error) {
	return DecodeVectorByType(blob, vi.cfg.Type, vi.cfg.Dim)
}

// DecodeVectorByType parses a raw blob for a given TYPE and DIM.
func DecodeVectorByType(blob []byte, vtype string, dim int) ([]float32, error) {
	switch vtype {
	case VectorTypeFloat32:
		if len(blob) != dim*4 {
			return nil, fmt.Errorf("expected %d bytes for %d-dim FLOAT32 vector, got %d", dim*4, dim, len(blob))
		}
		out := make([]float32, dim)
		for i := 0; i < dim; i++ {
			bits := binary.LittleEndian.Uint32(blob[i*4:])
			out[i] = math.Float32frombits(bits)
		}
		return out, nil
	case VectorTypeFloat64:
		if len(blob) != dim*8 {
			return nil, fmt.Errorf("expected %d bytes for %d-dim FLOAT64 vector, got %d", dim*8, dim, len(blob))
		}
		out := make([]float32, dim)
		for i := 0; i < dim; i++ {
			bits := binary.LittleEndian.Uint64(blob[i*8:])
			out[i] = float32(math.Float64frombits(bits))
		}
		return out, nil
	case VectorTypeFloat16:
		if len(blob) != dim*2 {
			return nil, fmt.Errorf("expected %d bytes for %d-dim FLOAT16 vector, got %d", dim*2, dim, len(blob))
		}
		out := make([]float32, dim)
		for i := 0; i < dim; i++ {
			out[i] = float16BitsToFloat32(binary.LittleEndian.Uint16(blob[i*2:]))
		}
		return out, nil
	case VectorTypeBFloat16:
		if len(blob) != dim*2 {
			return nil, fmt.Errorf("expected %d bytes for %d-dim BFLOAT16 vector, got %d", dim*2, dim, len(blob))
		}
		out := make([]float32, dim)
		for i := 0; i < dim; i++ {
			// bfloat16 = float32 top 16 bits (little-endian wire)
			bits := uint32(binary.LittleEndian.Uint16(blob[i*2:])) << 16
			out[i] = math.Float32frombits(bits)
		}
		return out, nil
	case VectorTypeInt8:
		if len(blob) != dim {
			return nil, fmt.Errorf("expected %d bytes for %d-dim INT8 vector, got %d", dim, dim, len(blob))
		}
		out := make([]float32, dim)
		for i := 0; i < dim; i++ {
			out[i] = float32(int8(blob[i]))
		}
		return out, nil
	case VectorTypeUint8:
		if len(blob) != dim {
			return nil, fmt.Errorf("expected %d bytes for %d-dim UINT8 vector, got %d", dim, dim, len(blob))
		}
		out := make([]float32, dim)
		for i := 0; i < dim; i++ {
			out[i] = float32(blob[i])
		}
		return out, nil
	default:
		return nil, fmt.Errorf("VECTOR TYPE '%s' decoding not yet implemented", vtype)
	}
}

// float16BitsToFloat32 converts IEEE 754 binary16 bits to float32.
func float16BitsToFloat32(u uint16) float32 {
	sign := uint32(u>>15) & 1
	exp := uint32(u>>10) & 0x1f
	frac := uint32(u & 0x3ff)
	switch exp {
	case 0:
		if frac == 0 {
			return math.Float32frombits(sign << 31)
		}
		// subnormal → ± 2^-14 * (frac/1024)
		v := float32(frac) / 1024.0 / 16384.0 // 2^14
		if sign == 1 {
			return -v
		}
		return v
	case 0x1f:
		if frac == 0 {
			return math.Float32frombits((sign << 31) | 0x7f800000)
		}
		return math.Float32frombits((sign << 31) | 0x7fc00000)
	default:
		return math.Float32frombits((sign << 31) | ((exp + 112) << 23) | (frac << 13))
	}
}

// AddVector stores (or replaces) the vector for docID. The blob is decoded per
// cfg.Type. For HNSW fields the graph is updated too.
func (vi *FTVectorIndex) AddVector(docID string, blob []byte) error {
	vec, err := vi.DecodeVector(blob)
	if err != nil {
		return err
	}
	vi.mu.Lock()
	defer vi.mu.Unlock()
	vi.vectors[docID] = vec
	if vi.hnsw != nil {
		// distFn resolves both stored ids and the synthetic query key via the
		// shared vectors map. For Insert the two args are always stored ids.
		distFn := func(a, b string) float32 {
			va, ok := vi.vectors[a]
			if !ok {
				return math.MaxFloat32
			}
			vb, ok := vi.vectors[b]
			if !ok {
				return math.MaxFloat32
			}
			return vectorDistance(va, vb, vi.cfg.DistanceMetric)
		}
		vi.hnsw.Insert(docID, distFn)
	}
	return nil
}

// DeleteVector removes docID from the index (and HNSW graph if applicable).
func (vi *FTVectorIndex) DeleteVector(docID string) {
	vi.mu.Lock()
	defer vi.mu.Unlock()
	delete(vi.vectors, docID)
	if vi.hnsw != nil {
		vi.hnsw.Delete(docID)
	}
}

// VectorScore is one KNN hit: the doc id, its distance, and the derived score.
type VectorScore struct {
	DocID    string
	Distance float32
	Score    float32 // 1/(1+distance), Redis convention for the AS score field
}

// SearchKNN returns up to k nearest neighbors to query. ef overrides the
// HNSW search-time candidate pool when > 0. For FLAT fields (or when the HNSW
// graph is unavailable) this brute-forces every stored vector.
func (vi *FTVectorIndex) SearchKNN(query []float32, k, ef int) []VectorScore {
	vi.mu.RLock()
	defer vi.mu.RUnlock()

	if len(vi.vectors) == 0 || k <= 0 {
		return nil
	}

	// Candidate ids. When a pre-filter narrows the doc set, callers pass the
	// allowed ids via SearchKNNFiltered; here we consider all vectors.
	ids := make([]string, 0, len(vi.vectors))
	for id := range vi.vectors {
		ids = append(ids, id)
	}
	return vi.knnAmongLocked(query, ids, k, ef)
}

// SearchKNNFiltered is the hybrid path: only docs in allowed (intersected with
// the stored vector ids) are scored. This backs "@filter=>[KNN k @v $v]".
func (vi *FTVectorIndex) SearchKNNFiltered(query []float32, allowed []string, k, ef int) []VectorScore {
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	if len(allowed) == 0 || k <= 0 {
		return nil
	}
	// Intersect allowed with what we actually have.
	ids := make([]string, 0, len(allowed))
	for _, id := range allowed {
		if _, ok := vi.vectors[id]; ok {
			ids = append(ids, id)
		}
	}
	return vi.knnAmongLocked(query, ids, k, ef)
}

// knnAmongLocked computes distances from query to every id in ids and returns
// the top-k by ascending distance. HNSW acceleration kicks in only for the
// unfiltered case; a filtered query always brute-forces the allowed subset
// (Redis ADHOC_BF policy equivalent).
func (vi *FTVectorIndex) knnAmongLocked(query []float32, ids []string, k, ef int) []VectorScore {
	// HNSW acceleration: only when unfiltered (ids == all), graph non-empty,
	// and enough vectors for approximation to be worth it.
	if vi.hnsw != nil && len(ids) == len(vi.vectors) && len(vi.vectors) > 1 {
		// Register the transient query vector under a reserved key so the distFn
		// closure can compute query->node distances without storing the vector.
		const queryKey = "\x00query"
		vi.vectors[queryKey] = query
		defer delete(vi.vectors, queryKey)

		distFn := func(a, b string) float32 {
			va, ok := vi.vectors[a]
			if !ok {
				return math.MaxFloat32
			}
			vb, ok := vi.vectors[b]
			if !ok {
				return math.MaxFloat32
			}
			return vectorDistance(va, vb, vi.cfg.DistanceMetric)
		}
		if ef < k {
			ef = k
		}
		hits := vi.hnsw.Search(queryKey, k, ef, distFn)
		out := make([]VectorScore, 0, len(hits))
		for _, id := range hits {
			if id == queryKey {
				continue
			}
			d := vectorDistance(query, vi.vectors[id], vi.cfg.DistanceMetric)
			out = append(out, VectorScore{DocID: id, Distance: d, Score: 1.0 / (1.0 + d)})
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Distance < out[j].Distance })
		return out
	}

	// Brute force.
	type cand struct {
		id   string
		dist float32
	}
	cands := make([]cand, 0, len(ids))
	for _, id := range ids {
		v, ok := vi.vectors[id]
		if !ok {
			continue
		}
		cands = append(cands, cand{id: id, dist: vectorDistance(query, v, vi.cfg.DistanceMetric)})
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].dist < cands[j].dist })
	if len(cands) > k {
		cands = cands[:k]
	}
	out := make([]VectorScore, 0, len(cands))
	for _, c := range cands {
		out = append(out, VectorScore{DocID: c.id, Distance: c.dist, Score: 1.0 / (1.0 + c.dist)})
	}
	return out
}

// vectorDistance computes the metric-space distance (lower = closer) between
// two equal-length float32 vectors. L2 is Euclidean; IP returns negative inner
// product so "nearest" = largest IP; COSINE returns 1 - cosine_similarity.
func vectorDistance(a, b []float32, metric string) float32 {
	if len(a) != len(b) {
		return math.MaxFloat32
	}
	switch metric {
	case VectorMetricL2:
		var sum float32
		for i := range a {
			d := a[i] - b[i]
			sum += d * d
		}
		return float32(math.Sqrt(float64(sum)))
	case VectorMetricIP:
		var dot float32
		for i := range a {
			dot += a[i] * b[i]
		}
		return -dot // nearest = largest dot product
	case VectorMetricCosine:
		var dot, na, nb float32
		for i := range a {
			dot += a[i] * b[i]
			na += a[i] * a[i]
			nb += b[i] * b[i]
		}
		if na == 0 || nb == 0 {
			return 1
		}
		return 1 - dot/(float32(math.Sqrt(float64(na)))*float32(math.Sqrt(float64(nb))))
	default:
		return math.MaxFloat32
	}
}

func parseInt(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

// KNNClause captures a parsed "=>[KNN K @field $param [AS score] [EF_RUNTIME n]
// [HYBRID_POLICY policy] [BATCH_SIZE n] [EPSILON e]]" suffix from a
// FT.SEARCH/FT.AGGREGATE query, optionally followed by
// "=>{$YIELD_DISTANCE_AS: alias; $SHARD_K_RATIO: r; $BATCH_SIZE: n; $EPSILON: e}".
// A nil return from SplitKNNClause means the query has no KNN clause.
type KNNClause struct {
	K            int
	Field        string  // vector field name (without leading @)
	Param        string  // $-parameter name carrying the query blob
	ScoreAs      string  // AS / $YIELD_DISTANCE_AS alias ("" = no alias)
	EFRuntime    int     // 0 = use index default
	HybridPolicy string  // ADHOC_BF or BATCHES (both resolve to filtered brute-force)
	BatchSize    int     // >0 when parsed; BATCHES hint — standalone brute-force ignores
	Epsilon      float64 // >0 keeps hits with distance < Epsilon (VSIM-like); 0 = off
}

// SplitKNNClause separates a query of the form "<base>=>[KNN ...]" into the
// base query and the KNN clause. Returns knn==nil when no =>[KNN is present.
// Supports trailing attribute blocks: =>{$YIELD_DISTANCE_AS: dist}.
func SplitKNNClause(query string) (base string, knn *KNNClause, err error) {
	idx := indexOfKNNMarker(query)
	if idx < 0 {
		return query, nil, nil
	}
	base = strings.TrimSpace(query[:idx])
	inner := query[idx:]
	open := strings.Index(inner, "[")
	if open < 0 {
		return query, nil, fmt.Errorf("Invalid KNN clause syntax")
	}
	// Match the KNN body's closing ]; attribute blocks use {} so Index is fine.
	relClose := strings.Index(inner[open+1:], "]")
	if relClose < 0 {
		return query, nil, fmt.Errorf("Invalid KNN clause syntax")
	}
	closeBracket := open + 1 + relClose
	body := strings.TrimSpace(inner[open+1 : closeBracket])
	tokens := strings.Fields(body)
	if len(tokens) < 1 || !strings.EqualFold(tokens[0], "KNN") {
		return query, nil, fmt.Errorf("Expected KNN after =>[")
	}
	clause := &KNNClause{}
	ti := 1
	for ti < len(tokens) {
		switch strings.ToUpper(tokens[ti]) {
		case "AS":
			if ti+1 >= len(tokens) {
				return query, nil, fmt.Errorf("KNN AS requires a field name")
			}
			clause.ScoreAs = tokens[ti+1]
			ti += 2
		case "EF_RUNTIME":
			if ti+1 >= len(tokens) {
				return query, nil, fmt.Errorf("KNN EF_RUNTIME requires a value")
			}
			n, perr := parseInt(tokens[ti+1])
			if perr != nil || n <= 0 {
				return query, nil, fmt.Errorf("Invalid KNN EF_RUNTIME")
			}
			clause.EFRuntime = n
			ti += 2
		case "HYBRID_POLICY":
			if ti+1 >= len(tokens) {
				return query, nil, fmt.Errorf("KNN HYBRID_POLICY requires a value")
			}
			policy := strings.ToUpper(tokens[ti+1])
			if policy != "ADHOC_BF" && policy != "BATCHES" {
				return query, nil, fmt.Errorf("Invalid KNN HYBRID_POLICY '%s'", tokens[ti+1])
			}
			// Both policies currently resolve to brute-force over the filtered set.
			clause.HybridPolicy = policy
			ti += 2
		case "BATCH_SIZE":
			// Redis requires a following token; non-numeric values are accepted
			// (ponytail). Standalone Godis ignores the hint after parse.
			if ti+1 >= len(tokens) {
				return query, nil, fmt.Errorf("KNN BATCH_SIZE requires a value")
			}
			if n, perr := parseInt(tokens[ti+1]); perr == nil && n > 0 {
				clause.BatchSize = n
			}
			ti += 2
		case "EPSILON":
			// Missing value → ERR (Redis). Parseable float>0 filters distances;
			// garbage tokens accepted and ignored (Redis ponytail).
			if ti+1 >= len(tokens) {
				return query, nil, fmt.Errorf("KNN EPSILON requires a value")
			}
			if e, perr := strconv.ParseFloat(tokens[ti+1], 64); perr == nil && e > 0 {
				clause.Epsilon = e
			}
			ti += 2
		default:
			// Positional args: K, @field, $param — in order.
			if clause.K == 0 {
				k, perr := parseInt(tokens[ti])
				if perr != nil || k <= 0 {
					return query, nil, fmt.Errorf("Invalid KNN K value")
				}
				clause.K = k
			} else if clause.Field == "" {
				f := tokens[ti]
				f = strings.TrimPrefix(f, "@")
				clause.Field = f
			} else if clause.Param == "" {
				p := tokens[ti]
				if !strings.HasPrefix(p, "$") {
					return query, nil, fmt.Errorf("KNN query vector must be a $parameter")
				}
				clause.Param = p
			} else {
				return query, nil, fmt.Errorf("Unexpected KNN token '%s'", tokens[ti])
			}
			ti++
		}
	}
	if clause.K == 0 || clause.Field == "" || clause.Param == "" {
		return query, nil, fmt.Errorf("KNN clause requires K, @field, and $param")
	}

	// Optional trailing attribute block: =>{$YIELD_DISTANCE_AS: alias; ...}
	rest := strings.TrimSpace(inner[closeBracket+1:])
	if rest != "" {
		if err := applyKNNAttrBlock(clause, rest); err != nil {
			return query, nil, err
		}
	}
	return base, clause, nil
}

// applyKNNAttrBlock parses "=>{$YIELD_DISTANCE_AS: dist; $SHARD_K_RATIO: 0.5;
// $BATCH_SIZE: 10; $EPSILON: 0.5}" after a KNN clause. Unknown $-keys are
// ignored; empty values for known keys error (Redis syntax).
func applyKNNAttrBlock(clause *KNNClause, rest string) error {
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "=>") {
		return fmt.Errorf("Unexpected token after KNN clause")
	}
	rest = strings.TrimSpace(rest[2:])
	if !strings.HasPrefix(rest, "{") {
		return fmt.Errorf("Invalid KNN attribute block")
	}
	end := strings.LastIndex(rest, "}")
	if end < 0 {
		return fmt.Errorf("Invalid KNN attribute block")
	}
	inner := strings.TrimSpace(rest[1:end])
	if trailing := strings.TrimSpace(rest[end+1:]); trailing != "" {
		return fmt.Errorf("Unexpected token after KNN attribute block")
	}
	if inner == "" {
		return nil
	}
	// Split on ';' then parse "$KEY: value" pairs (whitespace around ':' ok).
	parts := strings.Split(inner, ";")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		colon := strings.Index(part, ":")
		if colon < 0 {
			return fmt.Errorf("Invalid KNN attribute '%s'", part)
		}
		key := strings.TrimSpace(part[:colon])
		val := strings.TrimSpace(part[colon+1:])
		key = strings.TrimPrefix(key, "$")
		switch strings.ToUpper(key) {
		case "YIELD_DISTANCE_AS":
			if val == "" {
				return fmt.Errorf("YIELD_DISTANCE_AS requires a field name")
			}
			// Attribute alias wins when AS was not set; AS already set keeps AS
			// (Redis treats them equivalently — last writer in practice).
			if clause.ScoreAs == "" {
				clause.ScoreAs = val
			} else {
				clause.ScoreAs = val
			}
		case "SHARD_K_RATIO":
			// Cluster-only hint; accept and ignore in standalone.
			// Redis requires a float in (0, 1]; reject empty / non-float / out-of-range.
			if val == "" {
				return fmt.Errorf("SHARD_K_RATIO requires a value")
			}
			r, perr := strconv.ParseFloat(val, 64)
			if perr != nil || r <= 0 || r > 1 {
				return fmt.Errorf("Invalid KNN SHARD_K_RATIO")
			}
		case "EF_RUNTIME":
			if val == "" {
				return fmt.Errorf("EF_RUNTIME requires a value")
			}
			n, perr := parseInt(val)
			if perr != nil || n <= 0 {
				return fmt.Errorf("Invalid KNN EF_RUNTIME")
			}
			clause.EFRuntime = n
		case "BATCH_SIZE":
			if val == "" {
				return fmt.Errorf("BATCH_SIZE requires a value")
			}
			if n, perr := parseInt(val); perr == nil && n > 0 {
				clause.BatchSize = n
			}
		case "EPSILON":
			if val == "" {
				return fmt.Errorf("EPSILON requires a value")
			}
			if e, perr := strconv.ParseFloat(val, 64); perr == nil && e > 0 {
				clause.Epsilon = e
			}
		default:
			// Accept unknown keys for forward-compat (ponytail).
		}
	}
	return nil
}

// StripTrailingAttrBlock removes a trailing "=>{$KEY: val; ...}" query attribute
// block (VECTOR_RANGE … =>{$YIELD_DISTANCE_AS; $EPSILON}). Returns the base
// query and UPPER-case key→value map. No `{…}` block after `=>` → unchanged.
// Callers should run SplitKNNClause first so "=>[KNN …]" is not mistaken.
func StripTrailingAttrBlock(query string) (base string, attrs map[string]string, err error) {
	q := strings.TrimSpace(query)
	idx := strings.LastIndex(q, "=>")
	if idx < 0 {
		return query, nil, nil
	}
	rest := strings.TrimSpace(q[idx+2:])
	if !strings.HasPrefix(rest, "{") {
		return query, nil, nil
	}
	end := strings.LastIndex(rest, "}")
	if end < 0 {
		return query, nil, fmt.Errorf("Invalid query attribute block")
	}
	if trailing := strings.TrimSpace(rest[end+1:]); trailing != "" {
		return query, nil, fmt.Errorf("Unexpected token after query attribute block")
	}
	inner := strings.TrimSpace(rest[1:end])
	attrs = make(map[string]string)
	if inner != "" {
		for _, part := range strings.Split(inner, ";") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			colon := strings.Index(part, ":")
			if colon < 0 {
				return query, nil, fmt.Errorf("Invalid query attribute '%s'", part)
			}
			key := strings.TrimPrefix(strings.TrimSpace(part[:colon]), "$")
			val := strings.TrimSpace(part[colon+1:])
			if key == "" {
				return query, nil, fmt.Errorf("Invalid query attribute '%s'", part)
			}
			attrs[strings.ToUpper(key)] = val
		}
	}
	return strings.TrimSpace(q[:idx]), attrs, nil
}

// SearchRangeFiltered returns vectors among allowed whose distance to query is
// ≤ radius (inclusive). Results are sorted by ascending distance.
func (vi *FTVectorIndex) SearchRangeFiltered(query []float32, allowed []string, radius float32) []VectorScore {
	vi.mu.RLock()
	defer vi.mu.RUnlock()
	if len(allowed) == 0 || radius < 0 {
		return nil
	}
	out := make([]VectorScore, 0)
	for _, id := range allowed {
		v, ok := vi.vectors[id]
		if !ok {
			continue
		}
		d := vectorDistance(query, v, vi.cfg.DistanceMetric)
		if d <= radius {
			out = append(out, VectorScore{DocID: id, Distance: d, Score: 1.0 / (1.0 + d)})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Distance < out[j].Distance })
	return out
}

// indexOfKNNMarker finds the start of "=>[KNN" in the query, or -1. It avoids
// matching "=>" that appears inside quoted phrases.
func indexOfKNNMarker(query string) int {
	target := "=>["
	for i := 0; i+len(target) <= len(query); i++ {
		if query[i] == '"' {
			// Skip quoted phrase.
			j := i + 1
			for j < len(query) && query[j] != '"' {
				j++
			}
			i = j
			continue
		}
		if query[i:i+len(target)] == target {
			rest := strings.TrimSpace(query[i+len(target):])
			if strings.HasPrefix(strings.ToUpper(rest), "KNN") {
				return i
			}
		}
	}
	return -1
}
