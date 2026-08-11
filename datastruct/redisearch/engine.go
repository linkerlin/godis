package redisearch

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrTimeout is returned when FT TIMEOUT soft deadline is exceeded.
var ErrTimeout = errors.New("Timeout limit was reached")

// RediSearchEngine is the main search engine
type RediSearchEngine struct {
	name   string
	index  *InvertedIndex
	schema map[string]*Field

	// Geo indices for each geo field
	geoIndices map[string]*GeoIndex // field name -> geo index

	// Vector indices for each VECTOR field. Lazily created at CreateIndex time
	// from the field's VectorConfig; populated by AddDocument/DeleteDocument.
	vectorIndices map[string]*FTVectorIndex // field name -> vector index

	// geoshapeIndices stores parsed GEOSHAPE values per field/doc for spatial
	// predicate queries (@geom:[WITHIN $poly]). map[field]docID -> *GeoShape.
	geoshapeIndices map[string]map[string]*GeoShape

	// Autocomplete for suggestions
	autocomplete *Autocomplete

	// stopFilter holds this index's STOPWORDS override; nil means the index's
	// default English stopword list applies (see NewInvertedIndex).
	stopFilter *StopWordFilter

	// synonymExpander optionally expands a query term into synonym alternatives
	// (e.g. FT.SYNADD groups); nil disables synonym expansion.
	synonymExpander func(term string) []string

	// Options
	defaultLanguage string
	scoreField      string
	payloadField    string

	// Index-level flags mirrored from EngineConfig for FT.INFO reporting.
	noOffsets     bool
	noHL          bool
	maxTextFields bool
	temporary     int
	filterExpr    string
	indexAll      string
	indexMissingAll bool

	// createArgs is the FT.CREATE argument list (index name first) kept so AOF
	// rewrite / GODIS opaque can replay a minimal index definition.
	createArgs [][]byte

	mu sync.RWMutex
}

// EngineConfig holds configuration for creating an engine
type EngineConfig struct {
	Name            string
	DefaultLanguage string
	ScoreField      string
	PayloadField    string
	// StopWords and HasStopWords implement FT.CREATE ... STOPWORDS count [word ...].
	// HasStopWords distinguishes "not specified" (use default English list) from
	// an explicit list, including STOPWORDS 0 (empty list disables filtering).
	StopWords    []string
	HasStopWords bool
	// Redis 8.x index-level options. Parsed from FT.CREATE; the behavioral
	// subset (NoOffsets/NoFreqs/NoFields) is honored at index+query time, the
	// rest (MaxTextFields/Temporary/Filter/IndexAll/IndexMissing) are stored
	// for FT.INFO and later wiring (Temporary needs an idle timer; Filter needs
	// an expression evaluator — both deferred).
	NoOffsets     bool
	NoFields      bool
	NoFreqs       bool
	NoHL          bool
	MaxTextFields bool
	Temporary     int    // seconds; 0 = permanent
	Filter        string // per-key FILTER expression
	IndexAll      string // "ENABLE" | "DISABLE" | ""
	IndexMissing  bool   // index-wide INDEXMISSING
}

// NewRediSearchEngine creates a new search engine
func NewRediSearchEngine(config *EngineConfig) *RediSearchEngine {
	idx := NewInvertedIndex()
	e := &RediSearchEngine{
		name:            config.Name,
		index:           idx,
		schema:          make(map[string]*Field),
		geoIndices:      make(map[string]*GeoIndex),
		autocomplete:    NewAutocomplete(),
		defaultLanguage: config.DefaultLanguage,
		scoreField:      config.ScoreField,
		payloadField:    config.PayloadField,
	}
	if config.HasStopWords {
		e.stopFilter = NewStopWordFilterFrom(config.StopWords)
		idx.stopFilter = e.stopFilter
	}
	idx.SetIndexFlags(config.NoOffsets, config.NoFields, config.NoFreqs)
	// Store remaining index-level options for FT.INFO / later wiring.
	e.noOffsets = config.NoOffsets
	e.noHL = config.NoHL || config.NoOffsets
	e.maxTextFields = config.MaxTextFields
	e.temporary = config.Temporary
	e.filterExpr = config.Filter
	e.indexAll = config.IndexAll
	e.indexMissingAll = config.IndexMissing
	return e
}

// SetSynonymExpander configures a callback used by Search to expand query
// terms into synonym alternatives (term OR syn1 OR syn2 ...).
func (e *RediSearchEngine) SetSynonymExpander(expand func(term string) []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.synonymExpander = expand
}

// Name returns the engine name
func (e *RediSearchEngine) Name() string {
	return e.name
}

// SetCreateArgs stores a copy of the FT.CREATE args for persistence replay.
func (e *RediSearchEngine) SetCreateArgs(args [][]byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.createArgs = cloneCmdArgs(args)
}

// CreateArgs returns a copy of the stored FT.CREATE args (nil if unset).
func (e *RediSearchEngine) CreateArgs() [][]byte {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return cloneCmdArgs(e.createArgs)
}

func cloneCmdArgs(args [][]byte) [][]byte {
	if len(args) == 0 {
		return nil
	}
	out := make([][]byte, len(args))
	for i, a := range args {
		out[i] = append([]byte(nil), a...)
	}
	return out
}

// CreateIndex creates the index with the given schema
func (e *RediSearchEngine) CreateIndex(fields []*Field) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, field := range fields {
		e.schema[field.Name] = field
		e.index.AddField(field)
		e.registerVectorField(field)
		e.registerGeoshapeField(field)
	}
	return nil
}

// AlterAddFields adds fields to an existing index schema.
func (e *RediSearchEngine) AlterAddFields(fields []*Field) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, field := range fields {
		if _, exists := e.schema[field.Name]; exists {
			return fmt.Errorf("Duplicate field in schema - %s", field.Name)
		}
		e.schema[field.Name] = field
		e.index.AddField(field)
		e.registerVectorField(field)
		e.registerGeoshapeField(field)
	}
	return nil
}

// registerVectorField creates a per-field FTVectorIndex when field is a VECTOR
// field with a parsed config. Caller must hold e.mu.
func (e *RediSearchEngine) registerVectorField(field *Field) {
	if field.Type != FieldTypeVector || field.VectorConfig == nil {
		return
	}
	if e.vectorIndices == nil {
		e.vectorIndices = make(map[string]*FTVectorIndex)
	}
	e.vectorIndices[field.Name] = NewFTVectorIndex(field.VectorConfig)
}

// registerGeoshapeField initializes the per-field docID→shape map for a
// GEOSHAPE field. Caller must hold e.mu.
func (e *RediSearchEngine) registerGeoshapeField(field *Field) {
	if field.Type != FieldTypeGeoShape {
		return
	}
	if e.geoshapeIndices == nil {
		e.geoshapeIndices = make(map[string]map[string]*GeoShape)
	}
	if e.geoshapeIndices[field.Name] == nil {
		e.geoshapeIndices[field.Name] = make(map[string]*GeoShape)
	}
}

// VectorIndex returns the per-field vector index, or nil if the field is not a
// VECTOR field. Used by FT.SEARCH to run KNN queries.
func (e *RediSearchEngine) VectorIndex(field string) *FTVectorIndex {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.vectorIndices[field]
}

// DropIndex drops the index and optionally deletes documents
func (e *RediSearchEngine) DropIndex(deleteDocs bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if deleteDocs {
		e.index.Clear()
	}

	e.schema = make(map[string]*Field)
	e.geoIndices = make(map[string]*GeoIndex)
	return nil
}

// AddDocument adds a document to the index
func (e *RediSearchEngine) AddDocument(docID string, fields map[string]interface{}, score float64, payload []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	doc := &Document{
		ID:      docID,
		Fields:  fields,
		Score:   score,
		Payload: payload,
	}

	if err := e.index.IndexDocument(doc); err != nil {
		return err
	}
	e.indexGeoFieldsLocked(doc)
	e.indexVectorFieldsLocked(docID, doc)
	e.indexGeoshapeFieldsLocked(docID, doc)
	return nil
}

// DeleteDocument deletes a document from the index
func (e *RediSearchEngine) DeleteDocument(docID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, gi := range e.geoIndices {
		gi.Remove(docID)
	}
	for _, vi := range e.vectorIndices {
		vi.DeleteVector(docID)
	}
	for _, gi := range e.geoshapeIndices {
		delete(gi, docID)
	}
	return e.index.DeleteDocument(docID)
}

// indexGeoshapeFieldsLocked parses and stores GEOSHAPE field values. Values
// arrive as a WKT string (Redis stores GEOSHAPE as WKT text). A malformed WKT
// is dropped (counted as an indexing failure, mirroring Redis). Caller holds e.mu.
func (e *RediSearchEngine) indexGeoshapeFieldsLocked(docID string, doc *Document) {
	for name, store := range e.geoshapeIndices {
		raw, ok := doc.Fields[name]
		if !ok || raw == nil {
			delete(store, docID)
			continue
		}
		wkt := fmt.Sprintf("%v", raw)
		shape, err := ParseWKT(wkt)
		if err != nil {
			// Bad WKT: drop the doc's shape for this field; Redis counts it in
			// hash_indexing_failures. ponytail: surface via FT.INFO counters.
			delete(store, docID)
			continue
		}
		store[docID] = shape
	}
}

// GeoshapeIndex returns the per-field docID→shape map for a GEOSHAPE field, or
// nil if the field isn't a GEOSHAPE field. Used by GeoShapeNode.Evaluate.
func (e *RediSearchEngine) GeoshapeIndex(field string) map[string]*GeoShape {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.geoshapeIndices[field]
}

// indexVectorFieldsLocked stores VECTOR field blobs into the per-field vector
// index. The blob may arrive as []byte (raw HSET value), string, or a decoded
// []float32 / []float64; []byte is decoded per the field's declared TYPE.
// Caller holds e.mu.
func (e *RediSearchEngine) indexVectorFieldsLocked(docID string, doc *Document) {
	for name, vi := range e.vectorIndices {
		raw, ok := doc.Fields[name]
		if !ok || raw == nil {
			continue
		}
		switch v := raw.(type) {
		case []byte:
			if err := vi.AddVector(docID, v); err != nil {
				// Indexing failure for one vector field doesn't abort the doc;
				// Redis counts these in hash_indexing_failures. We drop silently
				// for now (ponytail: surface via FT.INFO failure counters later).
				continue
			}
		case string:
			if err := vi.AddVector(docID, []byte(v)); err != nil {
				continue
			}
		case []float32:
			// Already decoded: encode back to FLOAT32 bytes for uniform path.
			buf := make([]byte, 4*len(v))
			for i, x := range v {
				binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(x))
			}
			_ = vi.AddVector(docID, buf)
		default:
			// Best-effort: stringify then treat as raw bytes.
			_ = vi.AddVector(docID, []byte(fmt.Sprintf("%v", v)))
		}
	}
}

// indexGeoFieldsLocked indexes GEO schema fields into geoIndices (caller holds e.mu).
func (e *RediSearchEngine) indexGeoFieldsLocked(doc *Document) {
	for name, field := range e.schema {
		if field.Type != FieldTypeGeo {
			continue
		}
		raw, ok := doc.Fields[name]
		if !ok || raw == nil {
			continue
		}
		pt, ok := ParseGeoPoint(fmt.Sprintf("%v", raw))
		if !ok {
			continue
		}
		if e.geoIndices[name] == nil {
			e.geoIndices[name] = NewGeoIndex()
		}
		e.geoIndices[name].Add(doc.ID, pt)
	}
}

// GetDocument retrieves a document by ID
func (e *RediSearchEngine) GetDocument(docID string) (*Document, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.index.GetDocument(docID)
}

// TagVals returns distinct values for a TAG field.
func (e *RediSearchEngine) TagVals(field string) []string {
	if e.index == nil {
		return nil
	}
	return e.index.TagVals(field)
}

// SearchResult represents a search result
type SearchResult struct {
	Document   *Document
	Score      float64
	Fields     map[string]interface{}
	Highlights map[string]string // field -> highlighted value
}

// SearchKNN runs a hybrid vector KNN query: it evaluates baseQuery to obtain
// the candidate document set (or all docs when baseQuery is "*"), then returns
// the k nearest vectors in knn.Field to queryVec. The returned SearchResults
// are ordered by ascending distance; each result's Score is the distance and,
// when knn.ScoreAs != "", the distance is also attached as a document field of
// that name so the AS clause surfaces in the reply.
func (e *RediSearchEngine) SearchKNN(baseQuery string, opts *SearchOptions, knn *KNNClause, queryVec []float32) (*SearchResults, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	vi := e.vectorIndices[knn.Field]
	if vi == nil {
		return nil, fmt.Errorf("Vector field '%s' not found in index", knn.Field)
	}

	// Evaluate the base query to get the candidate document set. "*" means all.
	var candidates []string
	if baseQuery == "" || baseQuery == "*" {
		for _, doc := range e.index.GetAllDocuments() {
			candidates = append(candidates, doc.ID)
		}
	} else {
		parser := NewExpressionParser(baseQuery)
		node, err := parser.Parse()
		if err != nil {
			node, err = NewQueryParser().Parse(baseQuery)
			if err != nil {
				return nil, err
			}
		}
		candidates = node.Evaluate(e.index)
	}

	// Run KNN restricted to the candidate set.
	hits := vi.SearchKNNFiltered(queryVec, candidates, knn.K, knn.EFRuntime)
	if len(hits) == 0 {
		return &SearchResults{Total: 0}, nil
	}

	results := &SearchResults{Total: len(hits), Results: make([]*SearchResult, 0, len(hits))}
	for _, hit := range hits {
		doc, ok := e.index.GetDocument(hit.DocID)
		if !ok {
			continue
		}
		// Attach the distance as the score so WITHSCORES surfaces it. When the
		// query named the score (AS), also plant it as a synthetic field so the
		// RETURN/default field list shows it.
		fields := doc.Fields
		if knn.ScoreAs != "" {
			if fields == nil {
				fields = make(map[string]interface{}, 1)
			} else {
				cp := make(map[string]interface{}, len(fields)+1)
				for k, v := range doc.Fields {
					cp[k] = v
				}
				fields = cp
			}
			fields[knn.ScoreAs] = fmt.Sprintf("%g", hit.Distance)
		}
		results.Results = append(results.Results, &SearchResult{
			Document: doc,
			Score:    float64(hit.Distance),
			Fields:   fields,
		})
	}
	return results, nil
}

// Search performs a search query
func (e *RediSearchEngine) Search(query string, opts *SearchOptions) (*SearchResults, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var deadline time.Time
	if opts != nil {
		deadline = ftDeadline(opts.TimeoutMs, opts.Deadline)
	}

	// Parse query
	parser := NewExpressionParser(query)
	if opts != nil {
		parser.SetDialect(opts.Dialect)
	}
	node, err := parser.Parse()
	if err != nil {
		// Fallback to simple parser
		simpleParser := NewQueryParser()
		node, err = simpleParser.Parse(query)
		if err != nil {
			return nil, err
		}
	}

	if opts != nil {
		ApplyPhraseOpts(node, opts.Slop, opts.InOrder)
		if len(opts.InFields) > 0 {
			node = ExpandInFields(node, opts.InFields)
		}
	}
	if e.synonymExpander != nil {
		node = ExpandSynonyms(node, e.synonymExpander)
	}

	// Enforce FT.CONFIG MINPREFIX / MAXEXPANSIONS before fanning out.
	if opts != nil && (opts.MinPrefix > 0 || opts.MaxExpansions > 0) {
		if err := ValidateExpansions(node, e.index, opts.MinPrefix, opts.MaxExpansions); err != nil {
			return nil, err
		}
	}
	// DIALECT 2-only constructs (comparison ops, ismissing) require Dialect >= 2.
	if opts != nil && opts.Dialect > 0 && opts.Dialect < 2 && RequiresDialect2(node) {
		return nil, fmt.Errorf("DIALECT 2+ required for this query")
	}
	// DIALECT 3-only constructs (GEOSHAPE predicates) require Dialect >= 3.
	if opts != nil && opts.Dialect > 0 && opts.Dialect < 3 && RequiresDialect3(node) {
		return nil, fmt.Errorf("DIALECT 3+ required for this query")
	}

	// Execute query (* = all documents, same as AGGREGATE)
	var docIDs []string
	if query == "*" {
		for _, doc := range e.index.GetAllDocuments() {
			docIDs = append(docIDs, doc.ID)
		}
	} else {
		docIDs = node.Evaluate(e.index)
		docIDs = e.filterByGeoNodes(docIDs, node)
		// GEOSHAPE spatial predicates: GeoShapeNode.Evaluate only narrows to
		// "field present"; the real predicate runs here against the query WKT
		// resolved from opts.Params.
		if opts != nil && len(opts.Params) > 0 {
			docIDs = e.filterByGeoshapeNodes(docIDs, node, opts.Params)
		}
	}

	// Apply geo filter if specified
	if opts != nil && opts.GeoFilter != nil {
		docIDs = e.applyGeoFilter(docIDs, opts.GeoFilter)
	}

	// Apply numeric FILTER clauses
	if opts != nil && len(opts.Filters) > 0 {
		docIDs = e.applyFieldFilters(docIDs, opts.Filters)
	}

	// Fetch documents and calculate scores. Build the score context once so
	// every doc scores against the same corpus snapshot; collect optional ~
	// terms from the AST so the chosen scorer can boost matches on them.
	scorerName := ""
	var queryPayload []byte
	verbatim := false
	noStop := false
	if opts != nil {
		scorerName = opts.Scorer
		queryPayload = opts.Payload
		verbatim = opts.Verbatim
		noStop = opts.NoStopWords
	}
	sc := e.buildScoreContext(query, CollectOptionalTerms(node), queryPayload, verbatim, noStop)
	if opts != nil && opts.BM25STDTanhFactor > 0 {
		sc.tanhFactor = opts.BM25STDTanhFactor
	}

	results := make([]*SearchResult, 0, len(docIDs))
	for _, docID := range docIDs {
		if ftDeadlineExceeded(deadline) {
			return nil, ErrTimeout
		}
		doc, ok := e.index.GetDocument(docID)
		if !ok {
			continue
		}

		score := e.calculateScore(doc, sc, scorerName)

		result := &SearchResult{
			Document: doc,
			Score:    score,
			Fields:   doc.Fields,
		}

		// Apply highlighting if requested
		if opts != nil && opts.Highlight {
			result.Highlights = e.highlightFields(doc, query, opts)
		}

		results = append(results, result)
	}

	// BM25STD.NORM: min-max over the full hit set before sort/LIMIT (Redis).
	if strings.EqualFold(scorerName, ScorerBM25STDNorm) {
		normalizeBM25STDNorm(results)
	}

	// Sort results. SORTBY uses numeric compare when both values parse as
	// numbers; otherwise lexicographic (TEXT/SORTABLE). Tiebreak by doc ID.
	var cmp func(a, b *SearchResult) int
	if opts != nil && opts.SortBy != "" {
		field := opts.SortBy
		desc := opts.SortDesc
		cmp = func(a, b *SearchResult) int {
			va, oka := numField(a, field)
			vb, okb := numField(b, field)
			if oka && okb {
				if va < vb {
					if desc {
						return 1
					}
					return -1
				} else if va > vb {
					if desc {
						return -1
					}
					return 1
				}
				return 0
			}
			sa, oka := strField(a, field)
			sb, okb := strField(b, field)
			if oka && okb {
				if sa < sb {
					if desc {
						return 1
					}
					return -1
				} else if sa > sb {
					if desc {
						return -1
					}
					return 1
				}
			}
			return 0
		}
	} else {
		cmp = func(a, b *SearchResult) int {
			if a.Score > b.Score {
				return -1
			} else if a.Score < b.Score {
				return 1
			}
			return 0
		}
	}
	sort.SliceStable(results, func(i, j int) bool {
		if c := cmp(results[i], results[j]); c != 0 {
			return c < 0
		}
		return results[i].Document.ID < results[j].Document.ID
	})

	// Apply pagination
	total := len(results)
	if opts != nil {
		if opts.Limit > 0 {
			start := opts.Offset
			end := opts.Offset + opts.Limit
			if start > total {
				start = total
			}
			if end > total {
				end = total
			}
			results = results[start:end]
		}
	}

	return &SearchResults{
		Total:   total,
		Results: results,
	}, nil
}

// GeoFilterOptions holds geo filter options
type GeoFilterOptions struct {
	Field  string
	Lat    float64
	Lon    float64
	Radius float64
	Unit   string // m, km, mi, ft
}

// numField reads a numeric field value from a search result's stored fields.
func ftDeadline(timeoutMs int, explicit time.Time) time.Time {
	if !explicit.IsZero() {
		return explicit
	}
	if timeoutMs > 0 {
		return time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
	}
	return time.Time{}
}

func ftDeadlineExceeded(deadline time.Time) bool {
	return !deadline.IsZero() && time.Now().After(deadline)
}

func numField(r *SearchResult, field string) (float64, bool) {
	raw, ok := r.Fields[field]
	if !ok {
		return 0, false
	}
	v, err := strconv.ParseFloat(fmt.Sprintf("%v", raw), 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func strField(r *SearchResult, field string) (string, bool) {
	if r == nil {
		return "", false
	}
	if r.Fields != nil {
		if raw, ok := r.Fields[field]; ok {
			return fmt.Sprintf("%v", raw), true
		}
	}
	if r.Document != nil && r.Document.Fields != nil {
		if raw, ok := r.Document.Fields[field]; ok {
			return fmt.Sprintf("%v", raw), true
		}
	}
	return "", false
}

// SearchOptions holds search options (legacy alias kept for callers).
type SearchOptions struct {
	Offset       int
	Limit        int
	SortBy       string
	SortDesc     bool
	WithScores   bool
	WithPayloads bool
	WithSortKeys bool
	Verbatim     bool
	NoStopWords  bool
	Slop         int
	InOrder      bool
	TimeoutMs    int // soft deadline in ms; cancellation mid-scan
	// Deadline overrides TimeoutMs when set (tests / callers with absolute time).
	Deadline        time.Time
	InFields        []string
	Summarize       bool
	SummarizeFields []string
	SummarizeLen    int // max chars per field; 0 = default 20
	GeoFilter       *GeoFilterOptions
	Filters         []FieldFilter
	// Highlight options
	Highlight         bool
	HighlightFields   []string
	HighlightOpenTag  string
	HighlightCloseTag string
	// Expansion limits (FT.CONFIG MINPREFIX / MAXEXPANSIONS). Zero = no check.
	MinPrefix     int
	MaxExpansions int
	// Dialect controls DIALECT 1 vs 2 query semantics. D2-only constructs
	// (comparison ops, ismissing) are rejected when Dialect < 2.
	Dialect int
	// Scorer selects the ranking function. Empty/"BM25STD" = default.
	// Also: TFIDF, TFIDF.DOCNORM, DISMAX, DOCSCORE, HAMMING, BM25 (deprecated
	// alias of BM25STD), BM25STD.NORM, BM25STD.TANH.
	Scorer string
	// BM25STDTanhFactor is the divisor for BM25STD.TANH (tanh(raw/factor)).
	// Zero means the Redis default of 4. Set via FT.SEARCH BM25STD_TANH_FACTOR.
	BM25STDTanhFactor float64
	// Payload carries the FT.SEARCH PAYLOAD value used by the HAMMING scorer.
	Payload []byte
	// Params carries FT.SEARCH PARAMS name→value bindings, used by GEOSHAPE
	// spatial predicates (@geom:[WITHIN $poly]) to resolve the query WKT.
	Params map[string][]byte
}

// FieldFilter represents a filter on a field
type FieldFilter struct {
	Field string
	Min   interface{}
	Max   interface{}
}

// SearchResults holds search results
type SearchResults struct {
	Total   int
	Results []*SearchResult
}

// calculateScore calculates TF-IDF like score
// buildScoreContext prepares the per-query statistics shared by every doc's
// scoring pass. queryTerms are the post-stopword (and optionally stemmed)
// tokens; optional marks which of them came from ~optional clauses so the
// scorer can boost without requiring them.
func (e *RediSearchEngine) buildScoreContext(query string, optionalTerms []string, payload []byte, verbatim, noStopWords bool) *scoreContext {
	tokens := e.index.tokenizer.Tokenize(query)
	skipStop := verbatim || noStopWords
	if !skipStop {
		tokens = e.index.stopFilter.Filter(tokens)
	}
	opt := make(map[string]bool, len(optionalTerms))
	for _, t := range optionalTerms {
		opt[t] = true
	}
	N := float64(e.index.DocCount())
	avgdl := 0.0
	if N > 0 {
		avgdl = float64(e.index.totalLength) / N
	}
	return &scoreContext{
		idx:        e.index,
		queryTerms: tokens,
		optional:   opt,
		docCount:   N,
		avgdl:      avgdl,
		payload:    payload,
	}
}

func (e *RediSearchEngine) calculateScore(doc *Document, sc *scoreContext, scorerName string) float64 {
	if sc == nil {
		return doc.Score
	}
	return e.computeScore(doc, sc, scorerName)
}

func (e *RediSearchEngine) getTermFrequency(docID, term string) float64 {
	docs := e.index.terms[term]
	if docs == nil {
		return 0
	}
	positions := docs[docID]
	return float64(len(positions))
}

// Aggregate performs an aggregation query
func (e *RediSearchEngine) Aggregate(req *AggregationRequest) (*AggregationResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	deadline := ftDeadline(req.TimeoutMs, req.Deadline)

	var docs []*Document

	// Handle wildcard query
	if req.Query == "*" {
		// Get all documents
		docs = e.index.GetAllDocuments()
	} else {
		// First, get matching documents
		parser := NewExpressionParser(req.Query)
		parser.SetDialect(req.Dialect)
		node, err := parser.Parse()
		if err != nil {
			return nil, err
		}

		// Enforce FT.CONFIG MINPREFIX / MAXEXPANSIONS for aggregation queries too.
		if req.MinPrefix > 0 || req.MaxExpansions > 0 {
			if err := ValidateExpansions(node, e.index, req.MinPrefix, req.MaxExpansions); err != nil {
				return nil, err
			}
		}

		docIDs := node.Evaluate(e.index)

		// Fetch documents
		docs = make([]*Document, 0, len(docIDs))
		for _, docID := range docIDs {
			if ftDeadlineExceeded(deadline) {
				return nil, ErrTimeout
			}
			doc, ok := e.index.GetDocument(docID)
			if ok {
				docs = append(docs, doc)
			}
		}
	}
	if ftDeadlineExceeded(deadline) {
		return nil, ErrTimeout
	}

	// Apply LOAD
	if req.LoadAll {
		// LOAD *: document fields already present
	} else {
		for _, load := range req.Load {
			_ = load // Fields already loaded in documents
		}
	}

	// Apply APPLY clauses that appeared before GROUPBY, against each
	// document's own fields (so a following GROUPBY/REDUCE can reference
	// the computed field).
	var preApply, postApply []ApplyClause
	for _, ac := range req.Apply {
		if ac.PreGroup {
			preApply = append(preApply, ac)
		} else {
			postApply = append(postApply, ac)
		}
	}
	docs = applyPreGroupClauses(docs, preApply)

	// Apply GROUPBY. When neither GROUPBY nor REDUCE is given, RediSearch
	// returns one row per matching document instead of collapsing them.
	var groups []*Group
	if len(req.GroupBy) == 0 && len(req.Reduce) == 0 {
		groups = passthroughGroups(docs)
	} else {
		groups = e.groupBy(docs, req.GroupBy, req.Reduce)
	}

	// Apply APPLY clauses that appeared after GROUPBY, against each result row.
	applyPostGroupClauses(groups, postApply)

	// Apply HAVING clause
	if req.Having != nil {
		groups = e.applyHaving(groups, req.Having)
	}

	// Apply FILTER
	if req.Filter != "" {
		groups = e.filterGroups(groups, req.Filter)
	}

	// Apply SORTBY
	if req.SortBy != "" {
		groups = e.sortGroups(groups, req.SortBy, req.SortDesc)
	}

	// Apply LIMIT
	total := len(groups)
	if req.Limit > 0 {
		start := req.Offset
		end := req.Offset + req.Limit
		if start > total {
			start = total
		}
		if end > total {
			end = total
		}
		groups = groups[start:end]
	}

	return &AggregationResult{
		Total:  total,
		Groups: groups,
	}, nil
}

// AggregationRequest represents an aggregation request
type AggregationRequest struct {
	Query     string
	Load      []string
	LoadAll   bool // LOAD *
	Verbatim  bool
	TimeoutMs int // soft deadline in ms
	// Deadline overrides TimeoutMs when set.
	Deadline time.Time
	GroupBy  []string      // Support multiple group by fields
	Having   *HavingClause // HAVING clause for group filtering
	Reduce   []Reducer
	SortBy   string
	SortDesc bool
	Offset   int
	Limit    int
	Filter   string        // FILTER expression
	Apply    []ApplyClause // APPLY <expr> AS <name> clauses, in pipeline order
	// Expansion limits (FT.CONFIG MINPREFIX / MAXEXPANSIONS). Zero = no check.
	MinPrefix     int
	MaxExpansions int
	// Dialect controls DIALECT 1 vs 2 query precedence (| vs space).
	Dialect int
}

// HavingClause represents a HAVING clause for group filtering
type HavingClause struct {
	Left     string      // Field name
	Operator string      // >, <, =, >=, <=
	Right    interface{} // Value to compare
}

// Reducer represents a reduction operation
type Reducer struct {
	Function string
	Field    string   // first arg with leading @ stripped (the field to reduce over)
	Args     []string // raw reducer arguments (e.g. QUANTILE: [@field, "0.5"])
	As       string
}

// AggregationResult represents aggregation results
type AggregationResult struct {
	Total  int
	Groups []*Group
}

// Group represents an aggregation group
type Group struct {
	By     interface{}
	Fields map[string]interface{}
}

func (e *RediSearchEngine) groupBy(docs []*Document, groupByFields []string, reducers []Reducer) []*Group {
	groupMap := make(map[string][]*Document)

	for _, doc := range docs {
		// Build composite key from multiple group by fields
		var keyParts []string
		for _, field := range groupByFields {
			f := strings.TrimPrefix(field, "@")
			keyParts = append(keyParts, fmt.Sprintf("%v", doc.Fields[f]))
		}
		key := strings.Join(keyParts, "|$")
		groupMap[key] = append(groupMap[key], doc)
	}

	var groups []*Group
	for key, groupDocs := range groupMap {
		group := &Group{
			By:     key,
			Fields: make(map[string]interface{}),
		}

		// Store individual group by field values (without @ prefix)
		keyParts := strings.Split(key, "|$")
		for i, field := range groupByFields {
			if i < len(keyParts) {
				group.Fields[strings.TrimPrefix(field, "@")] = keyParts[i]
			}
		}

		// Apply reducers
		for _, r := range reducers {
			value := e.applyReducer(groupDocs, r)
			if r.As != "" {
				group.Fields[r.As] = value
			} else {
				group.Fields[strings.TrimPrefix(r.Field, "@")] = value
			}
		}

		groups = append(groups, group)
	}

	return groups
}

// applyHaving filters groups based on HAVING clause
func (e *RediSearchEngine) applyHaving(groups []*Group, having *HavingClause) []*Group {
	var result []*Group

	for _, group := range groups {
		leftValue, exists := group.Fields[having.Left]
		if !exists {
			continue
		}

		// Compare values
		if e.compareHaving(leftValue, having.Operator, having.Right) {
			result = append(result, group)
		}
	}

	return result
}

// compareHaving compares two values with an operator
func (e *RediSearchEngine) compareHaving(left interface{}, op string, right interface{}) bool {
	// Convert to float64 for numeric comparison
	leftFloat, leftOk := toFloat64(left)
	rightFloat, rightOk := toFloat64(right)

	if leftOk && rightOk {
		switch op {
		case "=":
			return leftFloat == rightFloat
		case "!=":
			return leftFloat != rightFloat
		case ">":
			return leftFloat > rightFloat
		case ">=":
			return leftFloat >= rightFloat
		case "<":
			return leftFloat < rightFloat
		case "<=":
			return leftFloat <= rightFloat
		}
	}

	// String comparison
	leftStr := fmt.Sprintf("%v", left)
	rightStr := fmt.Sprintf("%v", right)

	switch op {
	case "=":
		return leftStr == rightStr
	case "!=":
		return leftStr != rightStr
	case ">":
		return leftStr > rightStr
	case ">=":
		return leftStr >= rightStr
	case "<":
		return leftStr < rightStr
	case "<=":
		return leftStr <= rightStr
	}

	return false
}

// toFloat64 converts an interface to float64
func toFloat64(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

func (e *RediSearchEngine) applyReducer(docs []*Document, r Reducer) interface{} {
	field := strings.TrimPrefix(r.Field, "@")
	switch strings.ToUpper(r.Function) {
	case "COUNT":
		return len(docs)
	case "SUM":
		var sum float64
		for _, doc := range docs {
			if v, ok := toFloat64(doc.Fields[field]); ok {
				sum += v
			}
		}
		return sum
	case "MIN":
		min := math.MaxFloat64
		for _, doc := range docs {
			if v, ok := toFloat64(doc.Fields[field]); ok {
				if v < min {
					min = v
				}
			}
		}
		if min == math.MaxFloat64 {
			return nil
		}
		return min
	case "MAX":
		max := -math.MaxFloat64
		for _, doc := range docs {
			if v, ok := toFloat64(doc.Fields[field]); ok {
				if v > max {
					max = v
				}
			}
		}
		if max == -math.MaxFloat64 {
			return nil
		}
		return max
	case "AVG":
		var sum float64
		var count int
		for _, doc := range docs {
			if v, ok := toFloat64(doc.Fields[field]); ok {
				sum += v
				count++
			}
		}
		if count == 0 {
			return nil
		}
		return sum / float64(count)
	case "TOLIST":
		var list []interface{}
		for _, doc := range docs {
			list = append(list, doc.Fields[field])
		}
		return list
	case "STDDEV":
		return reducerStdDev(docs, field)
	case "QUANTILE":
		// REDUCE QUANTILE 2 @field q   (q ∈ [0,1]; 0.5 = median)
		if len(r.Args) < 2 {
			return nil
		}
		q, err := strconv.ParseFloat(r.Args[1], 64)
		if err != nil || q < 0 || q > 1 {
			return nil
		}
		return reducerQuantile(docs, field, q)
	case "COUNT_DISTINCT":
		// Exact distinct count over the field's values.
		seen := make(map[string]struct{}, len(docs))
		for _, doc := range docs {
			if v, ok := doc.Fields[field]; ok {
				seen[fmt.Sprintf("%v", v)] = struct{}{}
			}
		}
		return len(seen)
	case "COUNT_DISTINCTISH":
		// Approximate distinct count via a 14-bit HyperLogLog (~0.81% error,
		// 16K registers). Redis uses HLL with ~3% error at 1024B; this is a
		// slightly more accurate variant at higher memory cost. ponytail: drop
		// to 12-bit if memory matters.
		return reducerCountDistinctishHLL(docs, field, 14)
	case "FIRST_VALUE":
		// REDUCE FIRST_VALUE nargs @field [BY @sortfield [ASC|DESC]]
		// Returns the first value of field, optionally ordered by another field.
		if len(docs) == 0 {
			return nil
		}
		byField, desc := parseFirstValueArgs(r.Args)
		chosen := docs[0]
		if byField != "" {
			best, bestOK := toFloat64(chosen.Fields[byField])
			for _, d := range docs[1:] {
				v, ok := toFloat64(d.Fields[byField])
				if !ok {
					continue
				}
				if !bestOK || (desc && v > best) || (!desc && v < best) {
					best = v
					bestOK = true
					chosen = d
				}
			}
		}
		if v, ok := chosen.Fields[field]; ok {
			return v
		}
		return nil
	case "RANDOM_SAMPLE":
		// REDUCE RANDOM_SAMPLE nargs @field sample_size — reservoir sampling.
		if len(r.Args) < 2 {
			return nil
		}
		size, err := strconv.Atoi(r.Args[1])
		if err != nil || size <= 0 {
			return nil
		}
		return reducerRandomSample(docs, field, size)
	case "COLLECT":
		// REDUCE COLLECT nargs FIELDS (*|n @f...) [DISTINCT] [SORTBY @f [ASC|DESC]] [LIMIT o c]
		return reducerCollect(docs, r.Args)
	default:
		return nil
	}
}

// CollectEntry is one collected document in a COLLECT reducer result: a
// projected field map plus the source doc id (@__key) and score (@__score)
// when requested. aggRowBytes serializes []CollectEntry as a nested array.
type CollectEntry struct {
	Fields map[string]interface{}
}

// reducerCollect implements the 8.8+ COLLECT reducer: it gathers each document
// of the group into an array of projected maps (the "top-N per group" pattern).
// Args grammar (all optional except FIELDS):
//
//	FIELDS *            project every doc field (plus @__key)
//	FIELDS n @f1 @f2..  project exactly those fields (n = count)
//	DISTINCT            drop duplicate projected values
//	SORTBY @f [ASC|DESC] order the collected entries
//	LIMIT offset count  page the collected entries
//
// Returned as []CollectEntry so the wire serializer can emit a proper nested
// array (rather than Go's %v map printing).
func reducerCollect(docs []*Document, args []string) interface{} {
	if len(docs) == 0 {
		return []CollectEntry{}
	}

	// Default: project all fields plus the key.
	fieldsAny := true
	var fields []string
	distinct := false
	sortField := ""
	sortDesc := false
	limitOffset, limitCount := 0, -1

	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "FIELDS":
			if i+1 < len(args) && args[i+1] == "*" {
				fieldsAny = true
				fields = nil
				i++
				continue
			}
			if i+1 < len(args) {
				n, err := strconv.Atoi(args[i+1])
				if err != nil || n < 0 {
					return nil
				}
				fieldsAny = false
				fields = nil
				i += 2
				for j := 0; j < n && i < len(args); j++ {
					fields = append(fields, strings.TrimPrefix(args[i], "@"))
					i++
				}
				i--
			}
		case "DISTINCT":
			distinct = true
		case "SORTBY":
			if i+1 < len(args) {
				sortField = strings.TrimPrefix(args[i+1], "@")
				i++
				if i+1 < len(args) && strings.EqualFold(args[i+1], "DESC") {
					sortDesc = true
					i++
				} else if i+1 < len(args) && strings.EqualFold(args[i+1], "ASC") {
					i++
				}
			}
		case "LIMIT":
			if i+2 < len(args) {
				off, err1 := strconv.Atoi(args[i+1])
				cnt, err2 := strconv.Atoi(args[i+2])
				if err1 == nil && err2 == nil && off >= 0 && cnt >= 0 {
					limitOffset, limitCount = off, cnt
				}
				i += 2
			}
		}
	}

	build := func(d *Document) map[string]interface{} {
		m := make(map[string]interface{})
		if fieldsAny {
			for k, v := range d.Fields {
				m[k] = v
			}
			m["__key"] = d.ID
		} else {
			for _, f := range fields {
				if f == "__key" {
					m["__key"] = d.ID
					continue
				}
				if f == "__score" {
					m["__score"] = d.Score
					continue
				}
				if v, ok := d.Fields[f]; ok {
					m[f] = v
				}
			}
		}
		return m
	}

	entries := make([]CollectEntry, 0, len(docs))
	seen := make(map[string]struct{})
	for _, d := range docs {
		m := build(d)
		if distinct {
			key := fmt.Sprintf("%v", m)
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
		}
		entries = append(entries, CollectEntry{Fields: m})
	}

	if sortField != "" {
		sort.SliceStable(entries, func(i, j int) bool {
			a, oka := toFloat64(entries[i].Fields[sortField])
			b, okb := toFloat64(entries[j].Fields[sortField])
			if oka && okb {
				if sortDesc {
					return a > b
				}
				return a < b
			}
			sa := fmt.Sprintf("%v", entries[i].Fields[sortField])
			sb := fmt.Sprintf("%v", entries[j].Fields[sortField])
			if sortDesc {
				return sa > sb
			}
			return sa < sb
		})
	}

	if limitCount >= 0 {
		off := limitOffset
		if off > len(entries) {
			off = len(entries)
		}
		end := off + limitCount
		if end > len(entries) {
			end = len(entries)
		}
		entries = entries[off:end]
	}
	return entries
}

// reducerStdDev returns the population standard deviation of the field's
// numeric values across docs. Returns nil when fewer than 1 numeric value.
func reducerStdDev(docs []*Document, field string) interface{} {
	var values []float64
	for _, doc := range docs {
		if v, ok := toFloat64(doc.Fields[field]); ok {
			values = append(values, v)
		}
	}
	if len(values) == 0 {
		return nil
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	mean := sum / float64(len(values))
	var sq float64
	for _, v := range values {
		sq += (v - mean) * (v - mean)
	}
	return math.Sqrt(sq / float64(len(values)))
}

// reducerQuantile returns the value at quantile q (0..1) using nearest-rank
// interpolation. Empty input returns nil.
func reducerQuantile(docs []*Document, field string, q float64) interface{} {
	var values []float64
	for _, doc := range docs {
		if v, ok := toFloat64(doc.Fields[field]); ok {
			values = append(values, v)
		}
	}
	if len(values) == 0 {
		return nil
	}
	sort.Float64s(values)
	idx := int(q * float64(len(values)-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

// reducerCountDistinctishHLL estimates distinct count via a HyperLogLog of the
// given precision (register bits). 14 bits ≈ 16K registers ≈ 0.81% std error.
func reducerCountDistinctishHLL(docs []*Document, field string, precision int) int {
	m := uint64(1) << precision
	registers := make([]uint8, m)
	mask := m - 1
	for _, doc := range docs {
		v, ok := doc.Fields[field]
		if !ok {
			continue
		}
		h := fnv1a64(fmt.Sprintf("%v", v))
		idx := h & mask
		w := h >> precision
		rho := uint8(1)
		// count leading zeros in the (64-precision)-bit w
		for bit := uint64(1) << (63 - precision); bit > 0 && w&bit == 0; bit >>= 1 {
			rho++
		}
		if rho > registers[idx] {
			registers[idx] = rho
		}
	}
	// Harmonic-mean alpha — standard HLL estimator.
	var sum float64
	zeros := 0
	for _, r := range registers {
		if r == 0 {
			zeros++
			sum += 1
		} else {
			sum += 1.0 / math.Pow(2.0, float64(r))
		}
	}
	alphaM := 0.7213 / (1.0 + 1.079/float64(m)) // correction for m>=128
	est := alphaM * float64(m) * float64(m) / sum
	// Small-range correction (linear counting) when estimate is small.
	if est <= 2.5*float64(m) && zeros > 0 {
		est = float64(m) * math.Log(float64(m)/float64(zeros))
	}
	return int(est + 0.5)
}

// fnv1a64 is a 64-bit FNV-1a hash used by the HLL reducer.
func fnv1a64(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// parseFirstValueArgs interprets REDUCE FIRST_VALUE nargs @field [BY @sortfield [ASC|DESC]].
// Returns the BY field name ("" if none) and whether DESC was requested.
func parseFirstValueArgs(args []string) (byField string, desc bool) {
	for i := 1; i < len(args); i++ {
		up := strings.ToUpper(args[i])
		if up == "BY" && i+1 < len(args) {
			byField = strings.TrimPrefix(args[i+1], "@")
			i++
			if i+1 < len(args) {
				if strings.EqualFold(args[i+1], "DESC") {
					desc = true
					i++
				} else if strings.EqualFold(args[i+1], "ASC") {
					i++
				}
			}
		}
	}
	return byField, desc
}

// reducerRandomSample returns up to size values sampled uniformly without
// replacement (reservoir sampling) from the field across docs.
func reducerRandomSample(docs []*Document, field string, size int) interface{} {
	if size <= 0 || len(docs) == 0 {
		return nil
	}
	reservoir := make([]interface{}, 0, size)
	for i, doc := range docs {
		v, ok := doc.Fields[field]
		if !ok {
			continue
		}
		if len(reservoir) < size {
			reservoir = append(reservoir, v)
			continue
		}
		// Replace a random element with probability size/(i+1).
		j := applyRandIntn(i + 1)
		if j < size {
			reservoir[j] = v
		}
	}
	return reservoir
}

// applyRandIntn returns a non-negative pseudo-random int in [0,n). It uses the
// package-level rand source which is safe for concurrent use (rand.Intn is
// goroutine-safe in Go 1.20+).
func applyRandIntn(n int) int {
	if n <= 0 {
		return 0
	}
	return rand.Intn(n)
}

func (e *RediSearchEngine) sortGroups(groups []*Group, field string, desc bool) []*Group {
	sort.Slice(groups, func(i, j int) bool {
		vi := groups[i].Fields[field]
		vj := groups[j].Fields[field]

		// Try numeric comparison
		fi, oki := vi.(float64)
		fj, okj := vj.(float64)
		if oki && okj {
			if desc {
				return fi > fj
			}
			return fi < fj
		}

		// String comparison
		si := fmt.Sprintf("%v", vi)
		sj := fmt.Sprintf("%v", vj)
		if desc {
			return si > sj
		}
		return si < sj
	})

	return groups
}

// filterGroups filters groups based on a filter expression
// Simple filter format: "field > 10", "field = value", "field < 100"
func (e *RediSearchEngine) filterGroups(groups []*Group, filter string) []*Group {
	// FILTER uses the full aggregation expression grammar (comparison + boolean
	// operators + functions), evaluated per group row. Pre-2.10 a single binary
	// comparison was all that was supported; the unified evaluator subsumes it.
	filter = strings.TrimSpace(filter)
	if filter == "" {
		return groups
	}
	var result []*Group
	for _, group := range groups {
		ok, err := EvalFilterExpr(filter, group.Fields)
		if err != nil {
			// Treat a malformed filter as non-matching rather than dropping the
			// whole result set; Redis errors out, but best-effort is safer here
			// until the parser is wired to surface errors from the pipeline.
			continue
		}
		if ok {
			result = append(result, group)
		}
	}
	return result
}

func (e *RediSearchEngine) matchesFilter(fieldValue interface{}, op string, numValue float64, strValue string, isNum bool) bool {
	// Try numeric comparison first
	if fv, ok := fieldValue.(float64); ok && isNum {
		switch op {
		case ">":
			return fv > numValue
		case "<":
			return fv < numValue
		case "=", "==":
			return fv == numValue
		case ">=":
			return fv >= numValue
		case "<=":
			return fv <= numValue
		}
	}

	// String comparison
	fv := fmt.Sprintf("%v", fieldValue)
	switch op {
	case "=", "==":
		return fv == strValue
	case ">":
		return fv > strValue
	case "<":
		return fv < strValue
	case ">=":
		return fv >= strValue
	case "<=":
		return fv <= strValue
	}

	return false
}

// Info returns index information
func (e *RediSearchEngine) Info() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return map[string]interface{}{
		"index_name":    e.name,
		"index_options": []string{},
		"index_definition": map[string]interface{}{
			"key_type":       "HASH",
			"prefixes":       []string{},
			"language_field": e.defaultLanguage,
			"score_field":    e.scoreField,
			"payload_field":  e.payloadField,
		},
		"attributes":                  e.getAttributesInfo(),
		"num_docs":                    e.index.DocCount(),
		"max_doc_id":                  e.index.DocCount(),
		"num_terms":                   e.index.TermCount(),
		"num_records":                 e.index.TermCount() * 2, // Approximation
		"inverted_sz_mb":              0.1,
		"total_inverted_index_blocks": 1,
		// Redis 8.x index-level flags declared at FT.CREATE.
		"no_offsets":     e.noOffsets,
		"no_highlight":   e.noHL,
		"max_text_fields": e.maxTextFields,
		"temporary":      e.temporary,
		"filter":         e.filterExpr,
		"index_all":      e.indexAll,
		"index_missing":  e.indexMissingAll,
	}
}

func (e *RediSearchEngine) getAttributesInfo() []map[string]interface{} {
	var attrs []map[string]interface{}

	for name, field := range e.schema {
		attr := map[string]interface{}{
			"identifier": name,
			"attribute":  name,
			"type":       fieldTypeToString(field.Type),
			"weight":     field.Weight,
			"sortable":   field.Sortable,
			"no_index":   field.NoIndex,
		}
		// Surface 8.x field options so FT.INFO reflects the declared schema.
		// Only include the flag when it differs from the default, to keep the
		// reply close to Redis (which omits unset options).
		if field.SortableUNF {
			attr["sortable_unf"] = true
		}
		if field.Phonetic != "" {
			attr["phonetic"] = field.Phonetic
		}
		if field.IndexMissing {
			attr["index_missing"] = true
		}
		if field.IndexEmpty {
			attr["index_empty"] = true
		}
		if field.WithSuffixTrie {
			attr["withsuffixtrie"] = true
		}
		if field.CaseSensitive {
			attr["case_sensitive"] = true
		}
		if field.Type == FieldTypeGeoShape && field.CoordinateSystem != "" {
			attr["coordinate_system"] = field.CoordinateSystem
		}
		attrs = append(attrs, attr)
	}

	return attrs
}

func fieldTypeToString(t FieldType) string {
	switch t {
	case FieldTypeText:
		return "TEXT"
	case FieldTypeNumeric:
		return "NUMERIC"
	case FieldTypeTag:
		return "TAG"
	case FieldTypeGeo:
		return "GEO"
	case FieldTypeVector:
		return "VECTOR"
	case FieldTypeGeoShape:
		return "GEOSHAPE"
	default:
		return "TEXT"
	}
}

// Suggest provides autocomplete suggestions using the autocomplete trie
func (e *RediSearchEngine) Suggest(prefix string, max int, fuzzy bool) []*Suggestion {
	if e.autocomplete == nil {
		return nil
	}
	return e.autocomplete.Get(prefix, max, fuzzy)
}

// AddSuggestion adds a suggestion to autocomplete
func (e *RediSearchEngine) AddSuggestion(term string, score float64, payload string, incr bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.autocomplete == nil {
		return
	}

	if incr {
		e.autocomplete.AddIncr(term, score, payload)
	} else {
		e.autocomplete.Add(term, score, payload)
	}
}

// DelSuggestion deletes a suggestion; returns true if removed.
func (e *RediSearchEngine) DelSuggestion(term string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.autocomplete == nil {
		return false
	}
	return e.autocomplete.Del(term)
}

// SuggestionCount returns the number of autocomplete suggestions.
func (e *RediSearchEngine) SuggestionCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.autocomplete == nil {
		return 0
	}
	return e.autocomplete.Len()
}

// SpellSuggestion is a single spell-check candidate with its score.
type SpellSuggestion struct {
	Term  string
	Score float64 // 1/(1+Levenshtein distance), Redis convention
}

// SpellCheck provides spelling corrections using only the index term dictionary.
func (e *RediSearchEngine) SpellCheck(term string, maxDist int) []string {
	sug := e.SpellCheckWithDicts(term, maxDist, nil, nil)
	out := make([]string, len(sug))
	for i, s := range sug {
		out[i] = s.Term
	}
	return out
}

// SpellCheckWithDicts builds the suggestion candidate pool from the index terms
// plus any TERMS INCLUDE dictionary entries, computes Levenshtein distance to
// `term`, drops any term appearing in the EXCLUDE set or identical to `term`,
// and returns candidates within maxDist sorted by score descending.
// This mirrors RediSearch FT.SPELLCHECK semantics: INCLUDE expands the pool,
// EXCLUDE removes suggestions, score = 1/(1+dist).
func (e *RediSearchEngine) SpellCheckWithDicts(term string, maxDist int, include, exclude map[string]bool) []SpellSuggestion {
	e.mu.RLock()
	defer e.mu.RUnlock()

	term = strings.ToLower(term)
	if maxDist < 1 {
		maxDist = 1
	}

	seen := make(map[string]int, 64) // term -> best distance
	add := func(candidate string) {
		candidate = strings.ToLower(candidate)
		if candidate == "" || candidate == term {
			return
		}
		if exclude != nil && exclude[candidate] {
			return
		}
		if strings.Contains(candidate, ":") {
			return // skip field-prefixed entries
		}
		dist := levenshteinDistance(term, candidate)
		if dist > maxDist {
			return
		}
		if prev, ok := seen[candidate]; !ok || dist < prev {
			seen[candidate] = dist
		}
	}

	for dictTerm := range e.index.terms {
		add(dictTerm)
	}
	for dictTerm := range include {
		add(dictTerm)
	}

	out := make([]SpellSuggestion, 0, len(seen))
	for t, d := range seen {
		out = append(out, SpellSuggestion{Term: t, Score: 1.0 / float64(1+d)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].Term < out[j].Term
	})
	return out
}

func levenshteinDistance(s1, s2 string) int {
	if len(s1) == 0 {
		return len(s2)
	}
	if len(s2) == 0 {
		return len(s1)
	}

	// Dynamic programming
	m, n := len(s1), len(s2)
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}

	for i := 0; i <= m; i++ {
		dp[i][0] = i
	}
	for j := 0; j <= n; j++ {
		dp[0][j] = j
	}

	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			cost := 0
			if s1[i-1] != s2[j-1] {
				cost = 1
			}
			dp[i][j] = min(dp[i-1][j]+1, min(dp[i][j-1]+1, dp[i-1][j-1]+cost))
		}
	}

	return dp[m][n]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// applyFieldFilters keeps docs whose numeric field values fall in [Min, Max].
func (e *RediSearchEngine) applyFieldFilters(docIDs []string, filters []FieldFilter) []string {
	if len(filters) == 0 {
		return docIDs
	}
	out := make([]string, 0, len(docIDs))
	for _, docID := range docIDs {
		doc, ok := e.index.GetDocument(docID)
		if !ok {
			continue
		}
		okDoc := true
		for _, f := range filters {
			raw, exists := doc.Fields[f.Field]
			if !exists {
				okDoc = false
				break
			}
			var v float64
			switch t := raw.(type) {
			case float64:
				v = t
			case int:
				v = float64(t)
			case int64:
				v = float64(t)
			case string:
				parsed, err := strconv.ParseFloat(t, 64)
				if err != nil {
					okDoc = false
				} else {
					v = parsed
				}
			default:
				okDoc = false
			}
			if !okDoc {
				break
			}
			minV, _ := f.Min.(float64)
			maxV, _ := f.Max.(float64)
			if v < minV || v > maxV {
				okDoc = false
				break
			}
		}
		if okDoc {
			out = append(out, docID)
		}
	}
	return out
}

// applyGeoFilter filters documents by geo location
func (e *RediSearchEngine) applyGeoFilter(docIDs []string, opts *GeoFilterOptions) []string {
	e.mu.RLock()
	geoIndex, ok := e.geoIndices[opts.Field]
	e.mu.RUnlock()

	if !ok {
		// No geo index for this field, return empty
		return nil
	}

	center := GeoPoint{Lat: opts.Lat, Lon: opts.Lon}

	var results []string
	for _, docID := range docIDs {
		// Check if doc is in geo index
		if point, ok := geoIndex.points[docID]; ok {
			filter := &GeoFilter{
				Center: center,
				Radius: opts.Radius,
				Unit:   opts.Unit,
			}
			if filter.Matches(point) {
				results = append(results, docID)
			}
		}
	}

	return results
}

// filterByGeoshapeNodes narrows docIDs by every inline @field:[OP $param]
// GEOSHAPE predicate in the AST. The GeoShapeNode's Evaluate only returned docs
// that have the field; this pass resolves the $param WKT from params, parses it,
// and applies the actual spatial predicate via RelateGeoShape.
func (e *RediSearchEngine) filterByGeoshapeNodes(docIDs []string, node QueryNode, params map[string][]byte) []string {
	var nodes []*GeoShapeNode
	collectGeoshapeNodes(node, &nodes)
	if len(nodes) == 0 {
		return docIDs
	}
	keep := make(map[string]bool, len(docIDs))
	for _, id := range docIDs {
		keep[id] = true
	}
	for _, gn := range nodes {
		store := e.geoshapeIndices[gn.Field]
		if store == nil {
			// Not a GEOSHAPE field; the node shouldn't have matched, drop all.
			return nil
		}
		wktBytes, ok := params[strings.TrimPrefix(gn.Param, "$")]
		if !ok {
			return nil // missing param -> no matches
		}
		queryShape, err := ParseWKT(string(wktBytes))
		if err != nil {
			return nil
		}
		for id := range keep {
			docShape, has := store[id]
			if !has {
				delete(keep, id)
				continue
			}
			if !RelateGeoShape(docShape, queryShape, gn.Op) {
				delete(keep, id)
			}
		}
	}
	out := make([]string, 0, len(keep))
	for _, id := range docIDs {
		if keep[id] {
			out = append(out, id)
		}
	}
	return out
}

// collectGeoshapeNodes walks the AST gathering all GeoShapeNode predicates.
func collectGeoshapeNodes(node QueryNode, out *[]*GeoShapeNode) {
	if node == nil {
		return
	}
	switch n := node.(type) {
	case *GeoShapeNode:
		*out = append(*out, n)
	case *AndNode:
		collectGeoshapeNodes(n.Left, out)
		collectGeoshapeNodes(n.Right, out)
	case *OrNode:
		collectGeoshapeNodes(n.Left, out)
		collectGeoshapeNodes(n.Right, out)
	case *NotNode:
		collectGeoshapeNodes(n.Child, out)
	case *OptionalNode:
		collectGeoshapeNodes(n.Child, out)
	}
}

// filterByGeoNodes narrows docIDs by every inline @field:[lon lat radius unit]
// GEO range clause found in the query AST. GeoRangeNode.Evaluate can only see
// the presence of the field (it has no access to geoIndices), so the actual
// radius test happens here as a post-filter, once per GeoRangeNode found.
func (e *RediSearchEngine) filterByGeoNodes(docIDs []string, node QueryNode) []string {
	geoNodes := collectGeoRangeNodes(node)
	for _, gn := range geoNodes {
		docIDs = e.geoRangeFilter(docIDs, gn)
	}
	return docIDs
}

// geoRangeFilter keeps only the docIDs whose geo field falls within gn's radius.
func (e *RediSearchEngine) geoRangeFilter(docIDs []string, gn *GeoRangeNode) []string {
	geoIndex, ok := e.geoIndices[gn.Field]
	if !ok {
		return nil
	}
	filter := &GeoFilter{
		Center: GeoPoint{Lat: gn.Lat, Lon: gn.Lon},
		Radius: gn.Radius,
		Unit:   gn.Unit,
	}
	out := make([]string, 0, len(docIDs))
	for _, id := range docIDs {
		if pt, ok := geoIndex.points[id]; ok && filter.Matches(pt) {
			out = append(out, id)
		}
	}
	return out
}

// AddGeoPoint adds a geo point for a document
func (e *RediSearchEngine) AddGeoPoint(docID string, field string, lat, lon float64) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.geoIndices[field] == nil {
		e.geoIndices[field] = NewGeoIndex()
	}

	e.geoIndices[field].Add(docID, GeoPoint{Lat: lat, Lon: lon})
}

// TermExists checks if a term exists in the index
func (e *RediSearchEngine) TermExists(term string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	term = strings.ToLower(term)

	// Check in index terms
	if _, ok := e.index.terms[term]; ok {
		return true
	}

	return false
}

// highlightFields generates highlighted versions of field values
func (e *RediSearchEngine) highlightFields(doc *Document, query string, opts *SearchOptions) map[string]string {
	highlights := make(map[string]string)

	// Default tags
	openTag := opts.HighlightOpenTag
	closeTag := opts.HighlightCloseTag
	if openTag == "" {
		openTag = "<b>"
	}
	if closeTag == "" {
		closeTag = "</b>"
	}

	// Determine which fields to highlight
	fieldsToHighlight := opts.HighlightFields
	if len(fieldsToHighlight) == 0 {
		// Highlight all text fields
		for fieldName := range doc.Fields {
			fieldsToHighlight = append(fieldsToHighlight, fieldName)
		}
	}

	// Extract query terms (simplified)
	queryTerms := extractQueryTerms(query)

	// Highlight each field
	for _, fieldName := range fieldsToHighlight {
		if value, ok := doc.Fields[fieldName]; ok {
			valueStr := fmt.Sprintf("%v", value)
			highlighted := highlightText(valueStr, queryTerms, openTag, closeTag)
			highlights[fieldName] = highlighted
		}
	}

	return highlights
}

// extractQueryTerms extracts search terms from query string
func extractQueryTerms(query string) []string {
	// Simplified: split by space and remove special characters
	terms := strings.Fields(query)
	var result []string
	for _, term := range terms {
		// Remove common punctuation
		term = strings.Trim(term, ".,!?;:\"'()[]{}*@")
		if term != "" && !strings.HasPrefix(term, "@") {
			result = append(result, strings.ToLower(term))
		}
	}
	return result
}

// highlightText wraps matching terms with highlight tags
func highlightText(text string, terms []string, openTag, closeTag string) string {
	if len(terms) == 0 {
		return text
	}

	// Simple case-insensitive replacement
	result := text
	lowerText := strings.ToLower(text)

	for _, term := range terms {
		if term == "" {
			continue
		}

		// Find all occurrences
		idx := strings.Index(lowerText, term)
		for idx != -1 {
			// Check if already highlighted
			before := result[:idx]
			after := result[idx+len(term):]

			// Wrap the match
			match := result[idx : idx+len(term)]
			result = before + openTag + match + closeTag + after

			// Update lowerText for next search
			lowerText = strings.ToLower(result)

			// Find next occurrence after this highlight
			nextIdx := strings.Index(lowerText[idx+len(openTag)+len(term)+len(closeTag):], term)
			if nextIdx != -1 {
				idx = idx + len(openTag) + len(term) + len(closeTag) + nextIdx
			} else {
				break
			}
		}
	}

	return result
}
