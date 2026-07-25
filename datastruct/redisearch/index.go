package redisearch

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// FieldType represents the type of a field in the index
type FieldType int

const (
	FieldTypeText FieldType = iota
	FieldTypeNumeric
	FieldTypeTag
	FieldTypeGeo
	FieldTypeVector
)

// Field represents a field definition in the index
type Field struct {
	Name      string // search/document field name (AS alias when set)
	Path      string // JSON path for ON JSON (empty → derive from Name)
	Type      FieldType
	Weight    float64
	Sortable  bool
	NoIndex   bool
	Stemming  bool
	Tokenizer Tokenizer
	Separator string // for TAG fields; defaults to "," when empty
}

// Document represents a document to be indexed
type Document struct {
	ID      string
	Fields  map[string]interface{}
	Score   float64
	Payload []byte
}

// IndexStats holds statistics about an index
type IndexStats struct {
	NumDocs       int64
	NumTerms      int64
	NumRecords    int64
	InvertedSize  int64
	OffsetVectors int64
}

// InvertedIndex manages the inverted index for full-text search
type InvertedIndex struct {
	terms     map[string]map[string][]int // term -> docID -> positions
	documents map[string]*Document
	fields    map[string]*Field

	tokenizer  Tokenizer
	stopFilter *StopWordFilter
	stemmer    *Stemmer

	mu sync.RWMutex
}

// NewInvertedIndex creates a new inverted index
func NewInvertedIndex() *InvertedIndex {
	return &InvertedIndex{
		terms:      make(map[string]map[string][]int),
		documents:  make(map[string]*Document),
		fields:     make(map[string]*Field),
		tokenizer:  &StandardTokenizer{},
		stopFilter: NewStopWordFilter(),
		stemmer:    &Stemmer{},
	}
}

// AddField adds a field definition to the index
func (idx *InvertedIndex) AddField(field *Field) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if field.Tokenizer == nil {
		field.Tokenizer = idx.tokenizer
	}
	idx.fields[field.Name] = field
}

// IndexDocument adds or updates a document in the index
func (idx *InvertedIndex) IndexDocument(doc *Document) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Remove old document if exists
	if _, exists := idx.documents[doc.ID]; exists {
		idx.removeDocumentInternal(doc.ID)
	}

	// Index new document
	idx.documents[doc.ID] = doc

	// Index each field
	for fieldName, value := range doc.Fields {
		field, ok := idx.fields[fieldName]
		if !ok || field.NoIndex {
			continue
		}

		switch field.Type {
		case FieldTypeText:
			idx.indexTextField(doc.ID, field, fmt.Sprintf("%v", value))
		case FieldTypeTag:
			idx.indexTagField(doc.ID, field, fmt.Sprintf("%v", value))
		case FieldTypeNumeric:
			// Numeric fields are stored but not inverted indexed
		}
	}

	return nil
}

// indexTextField indexes a text field
func (idx *InvertedIndex) indexTextField(docID string, field *Field, text string) {
	tokens := field.Tokenizer.Tokenize(text)
	tokens = idx.stopFilter.Filter(tokens)

	if field.Stemming {
		tokens = idx.stemmer.StemAll(tokens)
	}

	// Track positions
	positions := make(map[string][]int)
	for pos, token := range tokens {
		positions[token] = append(positions[token], pos)
	}

	// Add to inverted index with field prefix
	for term, posList := range positions {
		// Prefix with field name for field-specific search
		fieldTerm := field.Name + ":" + term

		if _, ok := idx.terms[fieldTerm]; !ok {
			idx.terms[fieldTerm] = make(map[string][]int)
		}
		idx.terms[fieldTerm][docID] = posList

		// Also add to global index
		if _, ok := idx.terms[term]; !ok {
			idx.terms[term] = make(map[string][]int)
		}
		idx.terms[term][docID] = posList
	}
}

// indexTagField indexes a tag field
func (idx *InvertedIndex) indexTagField(docID string, field *Field, value string) {
	// Tags are separated by the field's Separator (default ",").
	sep := field.Separator
	if sep == "" {
		sep = ","
	}
	tags := strings.Split(value, sep)
	for _, tag := range tags {
		tag = strings.TrimSpace(strings.ToLower(tag))
		if tag == "" {
			continue
		}

		// Store with tag prefix
		fieldTerm := field.Name + ":$" + tag
		if _, ok := idx.terms[fieldTerm]; !ok {
			idx.terms[fieldTerm] = make(map[string][]int)
		}
		idx.terms[fieldTerm][docID] = []int{0}
	}
}

// TagVals returns distinct tag values for a TAG field (sorted).
func (idx *InvertedIndex) TagVals(field string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	prefix := field + ":$"
	seen := make(map[string]struct{})
	for term := range idx.terms {
		if strings.HasPrefix(term, prefix) {
			seen[strings.TrimPrefix(term, prefix)] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for tag := range seen {
		out = append(out, tag)
	}
	sort.Strings(out)
	return out
}

// DeleteDocument removes a document from the index
func (idx *InvertedIndex) DeleteDocument(docID string) bool {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	return idx.removeDocumentInternal(docID)
}

func (idx *InvertedIndex) removeDocumentInternal(docID string) bool {
	_, exists := idx.documents[docID]
	if !exists {
		return false
	}

	// Remove from inverted index
	for term, docs := range idx.terms {
		delete(docs, docID)
		if len(docs) == 0 {
			delete(idx.terms, term)
		}
	}

	delete(idx.documents, docID)
	return true
}

// Search performs a full-text search
func (idx *InvertedIndex) Search(query string, field string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	tokens := idx.tokenizer.Tokenize(query)
	tokens = idx.stopFilter.Filter(tokens)

	if len(tokens) == 0 {
		return nil
	}

	// Get document sets for each term
	var docSets []map[string]bool
	for _, token := range tokens {
		term := token
		if field != "" {
			term = field + ":" + token
		}

		docs, ok := idx.terms[term]
		if !ok {
			// Try stemming
			stemmed := idx.stemmer.Stem(token)
			if stemmed != token {
				if d, found := idx.terms[stemmed]; found {
					docs = d
				} else if field != "" {
					docs = idx.terms[field+":"+stemmed]
				}
			}
		}

		docSet := make(map[string]bool)
		for docID := range docs {
			docSet[docID] = true
		}
		docSets = append(docSets, docSet)
	}

	// Intersect document sets (AND logic)
	if len(docSets) == 0 {
		return nil
	}

	result := docSets[0]
	for i := 1; i < len(docSets); i++ {
		newResult := make(map[string]bool)
		for docID := range result {
			if docSets[i][docID] {
				newResult[docID] = true
			}
		}
		result = newResult
		if len(result) == 0 {
			break
		}
	}

	// Convert to slice
	var docIDs []string
	for docID := range result {
		docIDs = append(docIDs, docID)
	}

	return docIDs
}

// SearchPhrase finds documents where terms appear within SLOP intervening words.
// When inOrder is true, term positions must be strictly increasing.
func (idx *InvertedIndex) SearchPhrase(terms []string, field string, slop int, inOrder bool) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(terms) == 0 {
		return nil
	}
	if slop < 0 {
		slop = 0
	}

	posMaps := make([]map[string][]int, len(terms))
	for i, token := range terms {
		term := token
		if field != "" {
			term = field + ":" + token
		}
		docs := idx.terms[term]
		if docs == nil {
			stemmed := idx.stemmer.Stem(token)
			if stemmed != token {
				if field != "" {
					docs = idx.terms[field+":"+stemmed]
				}
				if docs == nil {
					docs = idx.terms[stemmed]
				}
			}
		}
		if docs == nil {
			return nil
		}
		posMaps[i] = docs
	}

	// Intersect document IDs that contain every term.
	candidates := make(map[string]bool)
	for docID := range posMaps[0] {
		candidates[docID] = true
	}
	for i := 1; i < len(posMaps); i++ {
		for docID := range candidates {
			if _, ok := posMaps[i][docID]; !ok {
				delete(candidates, docID)
			}
		}
	}

	var docIDs []string
	for docID := range candidates {
		positions := make([][]int, len(terms))
		for i := range terms {
			positions[i] = posMaps[i][docID]
		}
		if phrasePositionsMatch(positions, slop, inOrder) {
			docIDs = append(docIDs, docID)
		}
	}
	return docIDs
}

// phrasePositionsMatch reports whether there is a sequence of positions, one per
// term, with at most `slop` intervening tokens between consecutive matches.
func phrasePositionsMatch(termPositions [][]int, slop int, inOrder bool) bool {
	return matchPhraseAt(termPositions, 0, -1, slop, inOrder)
}

func matchPhraseAt(termPositions [][]int, termIdx, prevPos, slop int, inOrder bool) bool {
	if termIdx >= len(termPositions) {
		return true
	}
	for _, pos := range termPositions[termIdx] {
		if termIdx > 0 {
			if inOrder {
				if pos <= prevPos {
					continue
				}
				if pos-prevPos-1 > slop {
					continue
				}
			} else {
				dist := pos - prevPos
				if dist < 0 {
					dist = -dist
				}
				if dist == 0 || dist-1 > slop {
					continue
				}
			}
		}
		if matchPhraseAt(termPositions, termIdx+1, pos, slop, inOrder) {
			return true
		}
	}
	return false
}

// SearchOr performs OR search
func (idx *InvertedIndex) SearchOr(query string, field string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	tokens := idx.tokenizer.Tokenize(query)
	tokens = idx.stopFilter.Filter(tokens)

	result := make(map[string]bool)

	for _, token := range tokens {
		term := token
		if field != "" {
			term = field + ":" + token
		}

		docs, ok := idx.terms[term]
		if !ok {
			// Try stemming
			stemmed := idx.stemmer.Stem(token)
			if stemmed != token {
				if d, found := idx.terms[stemmed]; found {
					docs = d
				}
			}
		}

		for docID := range docs {
			result[docID] = true
		}
	}

	var docIDs []string
	for docID := range result {
		docIDs = append(docIDs, docID)
	}

	return docIDs
}

// GetDocument retrieves a document by ID
func (idx *InvertedIndex) GetDocument(docID string) (*Document, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	doc, ok := idx.documents[docID]
	return doc, ok
}

// GetAllDocuments returns all documents
func (idx *InvertedIndex) GetAllDocuments() []*Document {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	docs := make([]*Document, 0, len(idx.documents))
	for _, doc := range idx.documents {
		docs = append(docs, doc)
	}

	// Sort by ID for consistency
	sort.Slice(docs, func(i, j int) bool {
		return docs[i].ID < docs[j].ID
	})

	return docs
}

// DocCount returns the number of documents
func (idx *InvertedIndex) DocCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.documents)
}

// TermCount returns the number of unique terms
func (idx *InvertedIndex) TermCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.terms)
}

// Clear removes all documents and terms
func (idx *InvertedIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.terms = make(map[string]map[string][]int)
	idx.documents = make(map[string]*Document)
}

// FuzzySearch performs fuzzy search with Levenshtein distance
func (idx *InvertedIndex) FuzzySearch(term string, field string, maxDist int) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	results := make(map[string]bool)

	// Search through all terms
	for dictTerm := range idx.terms {
		// Skip field-prefixed terms if no field specified
		if field == "" && strings.Contains(dictTerm, ":") {
			continue
		}

		// Check field match
		if field != "" {
			if !strings.HasPrefix(dictTerm, field+":") {
				continue
			}
			// Remove field prefix for comparison
			dictTerm = strings.TrimPrefix(dictTerm, field+":")
		}

		// Calculate Levenshtein distance
		dist := levenshteinDistance(term, dictTerm)
		if dist <= maxDist {
			// Add all documents for this term
			if field != "" {
				dictTerm = field + ":" + dictTerm
			}
			if docs, ok := idx.terms[dictTerm]; ok {
				for docID := range docs {
					results[docID] = true
				}
			}
		}
	}

	// Convert to slice
	var result []string
	for docID := range results {
		result = append(result, docID)
	}

	return result
}

// PrefixSearch searches for terms with given prefix
func (idx *InvertedIndex) PrefixSearch(prefix string, field string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	prefix = strings.ToLower(prefix)
	if field != "" {
		prefix = field + ":" + prefix
	}

	result := make(map[string]bool)
	for term, docs := range idx.terms {
		if strings.HasPrefix(term, prefix) {
			for docID := range docs {
				result[docID] = true
			}
		}
	}

	var docIDs []string
	for docID := range result {
		docIDs = append(docIDs, docID)
	}
	return docIDs
}

// TagSearch searches for exact tag match
func (idx *InvertedIndex) TagSearch(field, tag string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	tag = strings.ToLower(strings.TrimSpace(tag))
	fieldTerm := field + ":$" + tag

	docs, ok := idx.terms[fieldTerm]
	if !ok {
		return nil
	}

	var docIDs []string
	for docID := range docs {
		docIDs = append(docIDs, docID)
	}
	return docIDs
}

// NumericRangeSearch returns doc IDs whose stored numeric field value falls
// within [min, max] (inclusive by default; exclusive bounds via minEx/maxEx).
// Numeric fields are stored on the document but not inverted-indexed, so this
// scans all documents. ponytail: linear scan per range query; add a sorted
// numeric index if range throughput matters.
func (idx *InvertedIndex) NumericRangeSearch(field string, minInf, maxInf, minEx, maxEx bool, min, max float64) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var result []string
	for docID, doc := range idx.documents {
		raw, ok := doc.Fields[field]
		if !ok {
			continue
		}
		v, err := strconv.ParseFloat(fmt.Sprintf("%v", raw), 64)
		if err != nil {
			continue
		}
		if !minInf {
			if minEx {
				if v <= min {
					continue
				}
			} else if v < min {
				continue
			}
		}
		if !maxInf {
			if maxEx {
				if v >= max {
					continue
				}
			} else if v > max {
				continue
			}
		}
		result = append(result, docID)
	}
	return result
}
