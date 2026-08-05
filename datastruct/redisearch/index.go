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
	FieldTypeGeoShape
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

	// Redis 8.x field options. Parsed and stored here; behavioral wiring is
	// incremental (CASESENSITIVE / INDEXEMPTY take effect immediately; PHONETIC
	// / WITHSUFFIXTRIE / INDEXMISSING / SortableUNF are stored for FT.INFO and
	// later phases).
	Phonetic       string // dm:en | dm:fr | dm:pt | dm:es (TEXT)
	IndexMissing   bool   // index absent values; queryable via ismissing() (DIALECT 2+)
	IndexEmpty     bool   // index empty strings as a searchable token (TEXT, TAG)
	WithSuffixTrie bool   // maintain suffix trie for *suffix / *infix* queries
	CaseSensitive  bool   // TAG: do not lowercase tag values
	SortableUNF    bool   // SORTABLE UNF: sort column stores unnormalized values
	CoordinateSystem string // GEOSHAPE: "FLAT" | "SPHERICAL" (default SPHERICAL)

	// VectorConfig holds the parsed VECTOR field attributes (algorithm, dim,
	// distance metric, ...). Non-nil only for FieldTypeVector fields.
	VectorConfig *VectorFieldConfig
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

	// docLengths tracks the total token count per document (summed across TEXT
	// fields) for BM25 length normalization. totalLength is the corpus sum,
	// giving avgdl = totalLength / DocCount.
	docLengths   map[string]int
	totalLength  int

	tokenizer  Tokenizer
	stopFilter *StopWordFilter
	stemmer    *Stemmer

	// Index-level flags from FT.CREATE. NoOffsets drops position data so phrase
	// / SLOP / highlight queries can't match. NoFreqs collapses frequency to 1
	// for every term-doc pair. NoFields suppresses the field-prefixed copy of
	// each term so @field: scoping becomes a no-op.
	noOffsets bool
	noFreqs   bool
	noFields  bool

	mu sync.RWMutex
}

// NewInvertedIndex creates a new inverted index
func NewInvertedIndex() *InvertedIndex {
	return &InvertedIndex{
		terms:       make(map[string]map[string][]int),
		documents:   make(map[string]*Document),
		fields:      make(map[string]*Field),
		docLengths:  make(map[string]int),
		tokenizer:   &StandardTokenizer{},
		stopFilter:  NewStopWordFilter(),
		stemmer:     &Stemmer{},
	}
}

// SetIndexFlags applies FT.CREATE NOOFFSETS / NOFIELDS / NOFREQS behavior.
// Called once at engine creation; the flags are read by indexTextField.
func (idx *InvertedIndex) SetIndexFlags(noOffsets, noFields, noFreqs bool) {
	idx.noOffsets = noOffsets
	idx.noFields = noFields
	idx.noFreqs = noFreqs
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

	// INDEXEMPTY: for TEXT/TAG fields whose value is absent or empty, index a
	// searchable empty-token marker so the doc is findable. Redis semantics:
	// only fields declared INDEXEMPTY get this treatment.
	for fieldName, field := range idx.fields {
		if field.NoIndex || !field.IndexEmpty {
			continue
		}
		if field.Type != FieldTypeText && field.Type != FieldTypeTag {
			continue
		}
		raw, present := doc.Fields[fieldName]
		if present {
			if s := fmt.Sprintf("%v", raw); s != "" {
				continue // non-empty value handled above
			}
		}
		// Absent or empty: index the empty-token marker for this field.
		fieldTerm := fieldName + ":\x00empty"
		if _, ok := idx.terms[fieldTerm]; !ok {
			idx.terms[fieldTerm] = make(map[string][]int)
		}
		idx.terms[fieldTerm][doc.ID] = []int{0}
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

	// Account this field's token count toward the document's length (for BM25
	// normalization). We count surviving (post-stopword) tokens.
	idx.docLengths[docID] += len(tokens)
	idx.totalLength += len(tokens)

	// Positions are tracked for phrase/SLOP/highlight. NOOFFSETS drops them
	// entirely (single sentinel position); NOFREQS collapses frequency to 1.
	positions := make(map[string][]int)
	for pos, token := range tokens {
		if idx.noOffsets || idx.noFreqs {
			// No positional detail needed: a single slot suffices for "present".
			if len(positions[token]) == 0 {
				positions[token] = []int{0}
			}
			continue
		}
		positions[token] = append(positions[token], pos)
	}

	addTerm := func(key string, posList []int) {
		if _, ok := idx.terms[key]; !ok {
			idx.terms[key] = make(map[string][]int)
		}
		idx.terms[key][docID] = posList
	}

	for term, posList := range positions {
		// Global index copy (unscoped).
		addTerm(term, posList)
		// Field-prefixed copy unless NOFIELDS suppresses field attribution.
		if !idx.noFields {
			addTerm(field.Name + ":" + term, posList)
		}
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
		tag = strings.TrimSpace(tag)
		if field.CaseSensitive {
			// preserve original case
		} else {
			tag = strings.ToLower(tag)
		}
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

	// Reclaim the doc's length contribution from the corpus total.
	if dl, ok := idx.docLengths[docID]; ok {
		idx.totalLength -= dl
		if idx.totalLength < 0 {
			idx.totalLength = 0
		}
		delete(idx.docLengths, docID)
	}

	delete(idx.documents, docID)
	return true
}

// Search performs a full-text search
func (idx *InvertedIndex) Search(query string, field string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.noFields {
		field = "" // NOFIELDS disables field filtering
	}

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

	if idx.noFields {
		field = ""
	}

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

	if idx.noFields {
		field = ""
	}
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

// SuffixSearch returns doc IDs whose indexed terms end with suffix. Redis
// accelerates this with WITHSUFFIXTRIE; without the trie it brute-forces the
// term dictionary — which is what we do here. field scopes the search.
func (idx *InvertedIndex) SuffixSearch(suffix string, field string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.noFields {
		field = ""
	}
	suffix = strings.ToLower(suffix)

	result := make(map[string]bool)
	for dictTerm, docs := range idx.terms {
		ct := dictTerm
		if field != "" {
			if !strings.HasPrefix(dictTerm, field+":") {
				continue
			}
			ct = strings.TrimPrefix(dictTerm, field+":")
		} else if strings.Contains(ct, ":") {
			// Skip field-prefixed copies when unscoped.
			continue
		}
		if strings.HasSuffix(ct, suffix) {
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

// InfixSearch returns doc IDs whose indexed terms contain infix (substring).
// Like SuffixSearch this is a brute-force dictionary scan unless the field has
// WITHSUFFIXTRIE (the trie is a future optimization).
func (idx *InvertedIndex) InfixSearch(infix string, field string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.noFields {
		field = ""
	}
	infix = strings.ToLower(infix)

	result := make(map[string]bool)
	for dictTerm, docs := range idx.terms {
		ct := dictTerm
		if field != "" {
			if !strings.HasPrefix(dictTerm, field+":") {
				continue
			}
			ct = strings.TrimPrefix(dictTerm, field+":")
		} else if strings.Contains(ct, ":") {
			continue
		}
		if strings.Contains(ct, infix) {
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

// PrefixTermCount returns the number of distinct dictionary terms that begin
// with prefix (field-scoped when field != ""). Used by MAXEXPANSIONS validation
// so a runaway prefix query can be rejected before Evaluate fans out.
func (idx *InvertedIndex) PrefixTermCount(prefix, field string) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	prefix = strings.ToLower(prefix)
	if field != "" {
		prefix = field + ":" + prefix
	}
	n := 0
	for term := range idx.terms {
		if strings.HasPrefix(term, prefix) {
			n++
		}
	}
	return n
}

// FuzzyTermCount returns the number of distinct dictionary terms within
// maxDist (Levenshtein) of term. Used by MAXEXPANSIONS validation for fuzzy
// queries. Field-prefixed dictionary entries are skipped when field == "".
func (idx *InvertedIndex) FuzzyTermCount(term, field string, maxDist int) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	term = strings.ToLower(term)
	n := 0
	for dictTerm := range idx.terms {
		if field == "" && strings.Contains(dictTerm, ":") {
			continue
		}
		ct := dictTerm
		if field != "" {
			if !strings.HasPrefix(dictTerm, field+":") {
				continue
			}
			ct = strings.TrimPrefix(dictTerm, field+":")
		}
		if levenshteinDistance(term, ct) <= maxDist {
			n++
		}
	}
	return n
}

// SuffixTermCount returns the number of dictionary terms ending with suffix.
func (idx *InvertedIndex) SuffixTermCount(suffix, field string) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	suffix = strings.ToLower(suffix)
	n := 0
	for dictTerm := range idx.terms {
		ct := dictTerm
		if field != "" {
			if !strings.HasPrefix(dictTerm, field+":") {
				continue
			}
			ct = strings.TrimPrefix(dictTerm, field+":")
		} else if strings.Contains(ct, ":") {
			continue
		}
		if strings.HasSuffix(ct, suffix) {
			n++
		}
	}
	return n
}

// InfixTermCount returns the number of dictionary terms containing infix.
func (idx *InvertedIndex) InfixTermCount(infix, field string) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	infix = strings.ToLower(infix)
	n := 0
	for dictTerm := range idx.terms {
		ct := dictTerm
		if field != "" {
			if !strings.HasPrefix(dictTerm, field+":") {
				continue
			}
			ct = strings.TrimPrefix(dictTerm, field+":")
		} else if strings.Contains(ct, ":") {
			continue
		}
		if strings.Contains(ct, infix) {
			n++
		}
	}
	return n
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

// FieldPresentDocIDs returns IDs of documents that have any value stored for
// field. Used by GeoRangeNode.Evaluate: the InvertedIndex has no access to
// geoIndices (those live on RediSearchEngine), so Evaluate can only narrow to
// "field is present" and the actual radius test happens as a post-filter in
// RediSearchEngine.filterByGeoNodes.
func (idx *InvertedIndex) FieldPresentDocIDs(field string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var docIDs []string
	for id, doc := range idx.documents {
		if _, ok := doc.Fields[field]; ok {
			docIDs = append(docIDs, id)
		}
	}
	return docIDs
}

// NumericCompare returns doc IDs whose stored numeric Field value satisfies
// op against v (op ∈ {==,!=,>,>=,<,<=}). Docs where Field is absent or
// non-numeric are excluded, except for "!=" which includes them (a missing
// value is "not equal" to any number). Like NumericRangeSearch this is a linear
// scan — ponytail: add a sorted numeric index if comparison throughput matters.
func (idx *InvertedIndex) NumericCompare(field, op string, v float64) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var result []string
	for docID, doc := range idx.documents {
		raw, ok := doc.Fields[field]
		if !ok {
			if op == "!=" {
				result = append(result, docID)
			}
			continue
		}
		got, err := strconv.ParseFloat(fmt.Sprintf("%v", raw), 64)
		if err != nil {
			if op == "!=" {
				result = append(result, docID)
			}
			continue
		}
		if numericCompareMatch(got, op, v) {
			result = append(result, docID)
		}
	}
	return result
}

// numericCompareMatch applies op to (got, want) and returns the comparison result.
func numericCompareMatch(got float64, op string, want float64) bool {
	switch op {
	case "==":
		return got == want
	case "!=":
		return got != want
	case ">":
		return got > want
	case ">=":
		return got >= want
	case "<":
		return got < want
	case "<=":
		return got <= want
	default:
		return false
	}
}

// MissingDocIDs returns doc IDs that do NOT have any value stored for field.
// Used by ismissing(@field) — requires the field to be declared INDEXMISSING so
// callers can opt in (handled at the query-parser layer).
func (idx *InvertedIndex) MissingDocIDs(field string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var result []string
	for docID, doc := range idx.documents {
		if _, ok := doc.Fields[field]; !ok {
			result = append(result, docID)
		}
	}
	return result
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
