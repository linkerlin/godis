package redisearch

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// RediSearchEngine is the main search engine
type RediSearchEngine struct {
	name   string
	index  *InvertedIndex
	schema map[string]*Field
	
	// Geo indices for each geo field
	geoIndices map[string]*GeoIndex // field name -> geo index
	
	// Autocomplete for suggestions
	autocomplete *Autocomplete
	
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
		geoIndices:      make(map[string]*GeoIndex),
		autocomplete:    NewAutocomplete(),
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
	Document   *Document
	Score      float64
	Fields     map[string]interface{}
	Highlights map[string]string // field -> highlighted value
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
	
	// Apply geo filter if specified
	if opts != nil && opts.GeoFilter != nil {
		docIDs = e.applyGeoFilter(docIDs, opts.GeoFilter)
	}
	
	// Fetch documents and calculate scores
	results := make([]*SearchResult, 0, len(docIDs))
	for _, docID := range docIDs {
		doc, ok := e.index.GetDocument(docID)
		if !ok {
			continue
		}
		
		score := e.calculateScore(doc, query)
		
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

// GeoFilterOptions holds geo filter options
type GeoFilterOptions struct {
	Field  string
	Lat    float64
	Lon    float64
	Radius float64
	Unit   string // m, km, mi, ft
}

// SearchOptions holds search options
type SearchOptions struct {
	Offset     int
	Limit      int
	SortBy     string
	SortDesc   bool
	WithScores bool
	WithPayloads bool
	GeoFilter  *GeoFilterOptions
	Filters   []FieldFilter
	// Highlight options
	Highlight         bool
	HighlightFields   []string
	HighlightOpenTag  string
	HighlightCloseTag string
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
	
	var docs []*Document
	
	// Handle wildcard query
	if req.Query == "*" {
		// Get all documents
		docs = e.index.GetAllDocuments()
	} else {
		// First, get matching documents
		parser := NewExpressionParser(req.Query)
		node, err := parser.Parse()
		if err != nil {
			return nil, err
		}
		
		docIDs := node.Evaluate(e.index)
		
		// Fetch documents
		docs = make([]*Document, 0, len(docIDs))
		for _, docID := range docIDs {
			doc, ok := e.index.GetDocument(docID)
			if ok {
				docs = append(docs, doc)
			}
		}
	}
	
	// Apply LOAD
	for _, load := range req.Load {
		_ = load // Fields already loaded in documents
	}
	
	// Apply GROUPBY
	groups := e.groupBy(docs, req.GroupBy, req.Reduce)
	
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
	Query   string
	Load    []string
	GroupBy []string          // Support multiple group by fields
	Having  *HavingClause     // HAVING clause for group filtering
	Reduce  []Reducer
	SortBy  string
	SortDesc bool
	Offset  int
	Limit   int
	Filter  string // FILTER expression
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

func (e *RediSearchEngine) groupBy(docs []*Document, groupByFields []string, reducers []Reducer) []*Group {
	groupMap := make(map[string][]*Document)
	
	for _, doc := range docs {
		// Build composite key from multiple group by fields
		var keyParts []string
		for _, field := range groupByFields {
			keyParts = append(keyParts, fmt.Sprintf("%v", doc.Fields[field]))
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
		
		// Store individual group by field values
		keyParts := strings.Split(key, "|$")
		for i, field := range groupByFields {
			if i < len(keyParts) {
				group.Fields[field] = keyParts[i]
			}
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

// filterGroups filters groups based on a filter expression
// Simple filter format: "field > 10", "field = value", "field < 100"
func (e *RediSearchEngine) filterGroups(groups []*Group, filter string) []*Group {
	// Parse simple filter expression
	// Support: field > value, field < value, field = value, field >= value, field <= value
	
	filter = strings.TrimSpace(filter)
	
	// Find operator
	operators := []string{">=", "<=", "=", ">", "<"}
	var op string
	var parts []string
	
	for _, candidate := range operators {
		if strings.Contains(filter, candidate) {
			parts = strings.SplitN(filter, candidate, 2)
			if len(parts) == 2 {
				op = candidate
				break
			}
		}
	}
	
	if op == "" {
		// No valid operator found, return all groups
		return groups
	}
	
	field := strings.TrimSpace(parts[0])
	valueStr := strings.TrimSpace(parts[1])
	
	// Try to parse as number
	var numValue float64
	var isNum bool
	if n, err := strconv.ParseFloat(valueStr, 64); err == nil {
		numValue = n
		isNum = true
	}
	
	var result []*Group
	for _, group := range groups {
		fieldValue, ok := group.Fields[field]
		if !ok {
			// Field not found, try group.By for grouping field
			if fmt.Sprintf("%v", group.By) == field {
				fieldValue = group.By
			} else {
				continue
			}
		}
		
		if e.matchesFilter(fieldValue, op, numValue, valueStr, isNum) {
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
