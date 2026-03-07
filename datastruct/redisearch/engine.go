package redisearch

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
)

// RediSearchEngine is the main search engine
type RediSearchEngine struct {
	name   string
	index  *InvertedIndex
	schema map[string]*Field
	
	// Options
	defaultLanguage string
	scoreField      string
	payloadField    string
	
	mu sync.RWMutex
}

// EngineConfig holds configuration for creating an engine
type EngineConfig struct {
	Name            string
	DefaultLanguage string
	ScoreField      string
	PayloadField    string
}

// NewRediSearchEngine creates a new search engine
func NewRediSearchEngine(config *EngineConfig) *RediSearchEngine {
	return &RediSearchEngine{
		name:            config.Name,
		index:           NewInvertedIndex(),
		schema:          make(map[string]*Field),
		defaultLanguage: config.DefaultLanguage,
		scoreField:      config.ScoreField,
		payloadField:    config.PayloadField,
	}
}

// Name returns the engine name
func (e *RediSearchEngine) Name() string {
	return e.name
}

// CreateIndex creates the index with the given schema
func (e *RediSearchEngine) CreateIndex(fields []*Field) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	for _, field := range fields {
		e.schema[field.Name] = field
		e.index.AddField(field)
	}
	
	return nil
}

// DropIndex drops the index and optionally deletes documents
func (e *RediSearchEngine) DropIndex(deleteDocs bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	if deleteDocs {
		e.index.Clear()
	}
	
	e.schema = make(map[string]*Field)
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
	
	return e.index.IndexDocument(doc)
}

// DeleteDocument deletes a document from the index
func (e *RediSearchEngine) DeleteDocument(docID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	
	return e.index.DeleteDocument(docID)
}

// GetDocument retrieves a document by ID
func (e *RediSearchEngine) GetDocument(docID string) (*Document, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	return e.index.GetDocument(docID)
}

// SearchResult represents a search result
type SearchResult struct {
	Document *Document
	Score    float64
	Fields   map[string]interface{}
}

// Search performs a search query
func (e *RediSearchEngine) Search(query string, opts *SearchOptions) (*SearchResults, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	// Parse query
	parser := NewExpressionParser(query)
	node, err := parser.Parse()
	if err != nil {
		// Fallback to simple parser
		simpleParser := NewQueryParser()
		node, err = simpleParser.Parse(query)
		if err != nil {
			return nil, err
		}
	}
	
	// Execute query
	docIDs := node.Evaluate(e.index)
	
	// Fetch documents and calculate scores
	results := make([]*SearchResult, 0, len(docIDs))
	for _, docID := range docIDs {
		doc, ok := e.index.GetDocument(docID)
		if !ok {
			continue
		}
		
		score := e.calculateScore(doc, query)
		
		results = append(results, &SearchResult{
			Document: doc,
			Score:    score,
			Fields:   doc.Fields,
		})
	}
	
	// Sort by score
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
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

// SearchOptions holds search options
type SearchOptions struct {
	Offset    int
	Limit     int
	SortBy    string
	SortDesc  bool
	WithScores bool
	WithPayloads bool
	Filters   []FieldFilter
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
func (e *RediSearchEngine) calculateScore(doc *Document, query string) float64 {
	// Simple BM25-like scoring
	score := doc.Score // Base score
	
	// Tokenize query
	tokens := e.index.tokenizer.Tokenize(query)
	tokens = e.index.stopFilter.Filter(tokens)
	
	docCount := float64(e.index.DocCount())
	
	for _, token := range tokens {
		// Get term frequency in document
		tf := e.getTermFrequency(doc.ID, token)
		
		// Get document frequency
		df := float64(len(e.index.terms[token]))
		
		// IDF calculation
		idf := math.Log((docCount - df + 0.5) / (df + 0.5) + 1)
		
		// TF-IDF
		score += tf * idf
	}
	
	return score
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
	
	// First, get matching documents
	parser := NewExpressionParser(req.Query)
	node, err := parser.Parse()
	if err != nil {
		return nil, err
	}
	
	docIDs := node.Evaluate(e.index)
	
	// Fetch documents
	docs := make([]*Document, 0, len(docIDs))
	for _, docID := range docIDs {
		doc, ok := e.index.GetDocument(docID)
		if ok {
			docs = append(docs, doc)
		}
	}
	
	// Apply LOAD
	for _, load := range req.Load {
		_ = load // Fields already loaded in documents
	}
	
	// Apply GROUPBY
	groups := e.groupBy(docs, req.GroupBy, req.Reduce)
	
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
	Query   string
	Load    []string
	GroupBy string
	Reduce  []Reducer
	SortBy  string
	SortDesc bool
	Offset  int
	Limit   int
}

// Reducer represents a reduction operation
type Reducer struct {
	Function string
	Field    string
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

func (e *RediSearchEngine) groupBy(docs []*Document, field string, reducers []Reducer) []*Group {
	groupMap := make(map[string][]*Document)
	
	for _, doc := range docs {
		key := fmt.Sprintf("%v", doc.Fields[field])
		groupMap[key] = append(groupMap[key], doc)
	}
	
	var groups []*Group
	for key, groupDocs := range groupMap {
		group := &Group{
			By:     key,
			Fields: make(map[string]interface{}),
		}
		
		// Apply reducers
		for _, r := range reducers {
			value := e.applyReducer(groupDocs, r)
			if r.As != "" {
				group.Fields[r.As] = value
			} else {
				group.Fields[r.Field] = value
			}
		}
		
		groups = append(groups, group)
	}
	
	return groups
}

func (e *RediSearchEngine) applyReducer(docs []*Document, r Reducer) interface{} {
	switch strings.ToUpper(r.Function) {
	case "COUNT":
		return len(docs)
	case "SUM":
		var sum float64
		for _, doc := range docs {
			if v, ok := doc.Fields[r.Field].(float64); ok {
				sum += v
			}
		}
		return sum
	case "MIN":
		min := math.MaxFloat64
		for _, doc := range docs {
			if v, ok := doc.Fields[r.Field].(float64); ok {
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
			if v, ok := doc.Fields[r.Field].(float64); ok {
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
			if v, ok := doc.Fields[r.Field].(float64); ok {
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
			list = append(list, doc.Fields[r.Field])
		}
		return list
	default:
		return nil
	}
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

// Info returns index information
func (e *RediSearchEngine) Info() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	return map[string]interface{}{
		"index_name":    e.name,
		"index_options": []string{},
		"index_definition": map[string]interface{}{
			"key_type":      "HASH",
			"prefixes":      []string{},
			"language_field": e.defaultLanguage,
			"score_field":   e.scoreField,
			"payload_field": e.payloadField,
		},
		"attributes":      e.getAttributesInfo(),
		"num_docs":        e.index.DocCount(),
		"max_doc_id":      e.index.DocCount(),
		"num_terms":       e.index.TermCount(),
		"num_records":     e.index.TermCount() * 2, // Approximation
		"inverted_sz_mb":  0.1,
		"total_inverted_index_blocks": 1,
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
	default:
		return "TEXT"
	}
}

// Suggest provides autocomplete suggestions
func (e *RediSearchEngine) Suggest(prefix string, max int) []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	
	// Get terms starting with prefix
	var suggestions []string
	seen := make(map[string]bool)
	
	for term := range e.index.terms {
		if strings.HasPrefix(term, prefix) && !strings.Contains(term, ":") {
			if !seen[term] {
				suggestions = append(suggestions, term)
				seen[term] = true
			}
		}
		if len(suggestions) >= max {
			break
		}
	}
	
	sort.Strings(suggestions)
	return suggestions
}

// SpellCheck provides spelling corrections
func (e *RediSearchEngine) SpellCheck(term string, maxDist int) []string {
	// Simple Levenshtein distance based spell check
	var corrections []string
	
	for dictTerm := range e.index.terms {
		if !strings.Contains(dictTerm, ":") {
			dist := levenshteinDistance(term, dictTerm)
			if dist <= maxDist {
				corrections = append(corrections, dictTerm)
			}
		}
	}
	
	return corrections
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
