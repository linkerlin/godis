package redisearch

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// QueryNode represents a node in the query AST
type QueryNode interface {
	Evaluate(idx *InvertedIndex) []string
}

// TermNode represents a single term
type TermNode struct {
	Term  string
	Field string // empty for default field
}

// Evaluate evaluates a term node
func (n *TermNode) Evaluate(idx *InvertedIndex) []string {
	if n.Field != "" {
		return idx.Search(n.Term, n.Field)
	}
	return idx.Search(n.Term, "")
}

// AndNode represents an AND operation
type AndNode struct {
	Left  QueryNode
	Right QueryNode
}

// Evaluate evaluates an AND node
func (n *AndNode) Evaluate(idx *InvertedIndex) []string {
	left := n.Left.Evaluate(idx)
	right := n.Right.Evaluate(idx)
	
	// Build set from left
	leftSet := make(map[string]bool)
	for _, id := range left {
		leftSet[id] = true
	}
	
	// Intersect with right
	var result []string
	for _, id := range right {
		if leftSet[id] {
			result = append(result, id)
		}
	}
	
	return result
}

// OrNode represents an OR operation
type OrNode struct {
	Left  QueryNode
	Right QueryNode
}

// Evaluate evaluates an OR node
func (n *OrNode) Evaluate(idx *InvertedIndex) []string {
	left := n.Left.Evaluate(idx)
	right := n.Right.Evaluate(idx)
	
	// Build set
	resultSet := make(map[string]bool)
	for _, id := range left {
		resultSet[id] = true
	}
	for _, id := range right {
		resultSet[id] = true
	}
	
	// Convert to slice
	var result []string
	for id := range resultSet {
		result = append(result, id)
	}
	
	return result
}

// NotNode represents a NOT operation
type NotNode struct {
	Child QueryNode
}

// Evaluate evaluates a NOT node
func (n *NotNode) Evaluate(idx *InvertedIndex) []string {
	// Get all documents
	allDocs := idx.GetAllDocuments()
	
	// Get matching documents
	matching := n.Child.Evaluate(idx)
	matchingSet := make(map[string]bool)
	for _, id := range matching {
		matchingSet[id] = true
	}
	
	// Return non-matching
	var result []string
	for _, doc := range allDocs {
		if !matchingSet[doc.ID] {
			result = append(result, doc.ID)
		}
	}
	
	return result
}

// OptionalNode represents an optional term (increases score if matched but not required)
type OptionalNode struct {
	Child QueryNode
}

// Evaluate evaluates an optional node. Per Redis semantics an optional term
// does NOT filter the result set (a doc is never excluded for lacking it), so
// Evaluate returns every indexed document. The scoring pass uses
// CollectOptionalTerms to boost docs that actually contain the term.
func (n *OptionalNode) Evaluate(idx *InvertedIndex) []string {
	all := idx.GetAllDocuments()
	out := make([]string, 0, len(all))
	for _, d := range all {
		out = append(out, d.ID)
	}
	return out
}

// GetMatching returns the documents that actually match the optional term
func (n *OptionalNode) GetMatching(idx *InvertedIndex) []string {
	return n.Child.Evaluate(idx)
}

// PrefixNode represents a prefix search
type PrefixNode struct {
	Prefix string
	Field  string
}

// Evaluate evaluates a prefix node
func (n *PrefixNode) Evaluate(idx *InvertedIndex) []string {
	return idx.PrefixSearch(n.Prefix, n.Field)
}

// SuffixNode represents a suffix search (*suffix). Redis accelerates this with
// WITHSUFFIXTRIE; without it the engine brute-forces the term dictionary.
type SuffixNode struct {
	Suffix string
	Field  string
}

// Evaluate evaluates a suffix node
func (n *SuffixNode) Evaluate(idx *InvertedIndex) []string {
	return idx.SuffixSearch(n.Suffix, n.Field)
}

// InfixNode represents a substring/contains search (*infix*).
type InfixNode struct {
	Infix string
	Field string
}

// Evaluate evaluates an infix node
func (n *InfixNode) Evaluate(idx *InvertedIndex) []string {
	return idx.InfixSearch(n.Infix, n.Field)
}

// NumericCompareNode evaluates a DIALECT-2 comparison against a numeric field:
// @n == 5, @n != 5, @n > 5, @n >= 5, @n < 5, @n <= 5. The doc's stored value
// for Field is parsed as a float and tested with Op. Docs whose field is absent
// or non-numeric never match (except !=, which matches them).
type NumericCompareNode struct {
	Field string
	Op    string // == != > >= < <=
	Value float64
}

// Evaluate evaluates a numeric comparison node
func (n *NumericCompareNode) Evaluate(idx *InvertedIndex) []string {
	return idx.NumericCompare(n.Field, n.Op, n.Value)
}

// MissingNode evaluates ismissing(@field): docs that have no value for Field.
// Redis requires the field to be declared INDEXMISSING for the query to work;
// we return matching docs regardless (the declaration is informational here).
type MissingNode struct {
	Field string
}

// Evaluate evaluates an ismissing node
func (n *MissingNode) Evaluate(idx *InvertedIndex) []string {
	return idx.MissingDocIDs(n.Field)
}

// FuzzyNode represents a fuzzy (approximate) search
type FuzzyNode struct {
	Term    string
	Field   string
	MaxDist int // Maximum Levenshtein distance
}

// Evaluate evaluates a fuzzy node
func (n *FuzzyNode) Evaluate(idx *InvertedIndex) []string {
	return idx.FuzzySearch(n.Term, n.Field, n.MaxDist)
}

// PhraseNode represents a quoted multi-term phrase with optional SLOP proximity.
type PhraseNode struct {
	Terms   []string
	Field   string
	Slop    int
	InOrder bool // quoted phrases default to ordered matching
}

// Evaluate evaluates a phrase with position proximity.
func (n *PhraseNode) Evaluate(idx *InvertedIndex) []string {
	if len(n.Terms) == 0 {
		return nil
	}
	if len(n.Terms) == 1 {
		return (&TermNode{Term: n.Terms[0], Field: n.Field}).Evaluate(idx)
	}
	return idx.SearchPhrase(n.Terms, n.Field, n.Slop, n.InOrder)
}

// ApplyPhraseOpts walks the AST and applies FT.SEARCH SLOP / INORDER to PhraseNodes.
func ApplyPhraseOpts(node QueryNode, slop int, inOrder bool) {
	if node == nil {
		return
	}
	switch n := node.(type) {
	case *PhraseNode:
		n.Slop = slop
		if inOrder {
			n.InOrder = true
		}
	case *AndNode:
		ApplyPhraseOpts(n.Left, slop, inOrder)
		ApplyPhraseOpts(n.Right, slop, inOrder)
	case *OrNode:
		ApplyPhraseOpts(n.Left, slop, inOrder)
		ApplyPhraseOpts(n.Right, slop, inOrder)
	case *NotNode:
		ApplyPhraseOpts(n.Child, slop, inOrder)
	case *OptionalNode:
		ApplyPhraseOpts(n.Child, slop, inOrder)
	}
}

// fuzzyDistance returns the Levenshtein distance encoded by %markers around a
// term: %t% = 1, %%t%% = 2, %%%t%%% = 3 (Redis max). Returns 0 when the term is
// not a balanced fuzzy expression (so callers can treat it as a literal term).
func fuzzyDistance(term string) int {
	lead := 0
	for lead < len(term) && term[lead] == '%' {
		lead++
	}
	if lead == 0 || lead > 3 {
		return 0
	}
	trail := 0
	for i := len(term) - 1; i >= 0 && term[i] == '%'; i-- {
		trail++
	}
	if trail != lead {
		return 0
	}
	if len(term)-lead-trail <= 0 {
		return 0 // empty inner term
	}
	return lead
}

// CollectOptionalTerms walks the AST and returns the literal term tokens that
// appear under an OptionalNode (~term). The scorer uses these to boost docs
// that contain the optional term without requiring it for matching.
func CollectOptionalTerms(node QueryNode) []string {
	var out []string
	var walk func(n QueryNode)
	walk = func(n QueryNode) {
		if n == nil {
			return
		}
		switch nn := n.(type) {
		case *OptionalNode:
			out = append(out, termTokens(nn.Child)...)
		case *AndNode:
			walk(nn.Left)
			walk(nn.Right)
		case *OrNode:
			walk(nn.Left)
			walk(nn.Right)
		case *NotNode:
			walk(nn.Child)
		}
	}
	walk(node)
	return out
}

// termTokens returns the literal term strings carried by a node (TermNode and
// the terms inside a PhraseNode). Used by CollectOptionalTerms for scoring.
func termTokens(n QueryNode) []string {
	switch nn := n.(type) {
	case *TermNode:
		return []string{strings.ToLower(nn.Term)}
	case *PhraseNode:
		out := make([]string, 0, len(nn.Terms))
		for _, t := range nn.Terms {
			out = append(out, strings.ToLower(t))
		}
		return out
	case *AndNode, *OrNode:
		// Optional of a compound is unusual; collect both sides.
		switch c := n.(type) {
		case *AndNode:
			return append(termTokens(c.Left), termTokens(c.Right)...)
		case *OrNode:
			return append(termTokens(c.Left), termTokens(c.Right)...)
		}
	}
	return nil
}

// RequiresDialect2 reports whether the AST contains any DIALECT-2-only construct
// (comparison operators or ismissing). The engine uses this to reject such
// queries when the declared dialect is < 2, matching Redis behavior.
func RequiresDialect2(node QueryNode) bool {
	if node == nil {
		return false
	}
	switch n := node.(type) {
	case *NumericCompareNode, *MissingNode:
		return true
	case *AndNode:
		return RequiresDialect2(n.Left) || RequiresDialect2(n.Right)
	case *OrNode:
		return RequiresDialect2(n.Left) || RequiresDialect2(n.Right)
	case *NotNode:
		return RequiresDialect2(n.Child)
	case *OptionalNode:
		return RequiresDialect2(n.Child)
	}
	return false
}

// RequiresDialect3 reports whether the AST contains DIALECT-3-only constructs
// (GEOSHAPE spatial predicates). Rejected when Dialect < 3.
func RequiresDialect3(node QueryNode) bool {
	if node == nil {
		return false
	}
	switch n := node.(type) {
	case *GeoShapeNode:
		return true
	case *AndNode:
		return RequiresDialect3(n.Left) || RequiresDialect3(n.Right)
	case *OrNode:
		return RequiresDialect3(n.Left) || RequiresDialect3(n.Right)
	case *NotNode:
		return RequiresDialect3(n.Child)
	case *OptionalNode:
		return RequiresDialect3(n.Child)
	}
	return false
}

// ValidateExpansions walks the query AST and enforces FT.CONFIG MINPREFIX and
// MAXEXPANSIONS against the live index term dictionary:
//   - MINPREFIX: every PrefixNode whose prefix is shorter than minPrefix errors
//     ("Query prefix length is less than MINPREFIX").
//   - MAXEXPANSIONS: every PrefixNode/FuzzyNode whose dictionary expansion
//     exceeds maxExpansions errors ("Max terms expansion exceeded").
//
// minPrefix <= 0 or maxExpansions <= 0 disable the corresponding check. This
// mirrors RediSearch, which rejects oversized expansions rather than silently
// truncating. The validator is called from engine.Search after parsing.
func ValidateExpansions(node QueryNode, idx *InvertedIndex, minPrefix, maxExpansions int) error {
	if node == nil {
		return nil
	}
	switch n := node.(type) {
	case *PrefixNode:
		if minPrefix > 0 && len(n.Prefix) < minPrefix {
			return fmt.Errorf("Query prefix length is less than MINPREFIX")
		}
		if maxExpansions > 0 {
			if c := idx.PrefixTermCount(n.Prefix, n.Field); c > maxExpansions {
				return fmt.Errorf("Max terms expansion exceeded")
			}
		}
	case *FuzzyNode:
		if maxExpansions > 0 {
			if c := idx.FuzzyTermCount(n.Term, n.Field, n.MaxDist); c > maxExpansions {
				return fmt.Errorf("Max terms expansion exceeded")
			}
		}
	case *SuffixNode:
		if maxExpansions > 0 {
			if c := idx.SuffixTermCount(n.Suffix, n.Field); c > maxExpansions {
				return fmt.Errorf("Max terms expansion exceeded")
			}
		}
	case *InfixNode:
		if maxExpansions > 0 {
			if c := idx.InfixTermCount(n.Infix, n.Field); c > maxExpansions {
				return fmt.Errorf("Max terms expansion exceeded")
			}
		}
	case *AndNode:
		if err := ValidateExpansions(n.Left, idx, minPrefix, maxExpansions); err != nil {
			return err
		}
		return ValidateExpansions(n.Right, idx, minPrefix, maxExpansions)
	case *OrNode:
		if err := ValidateExpansions(n.Left, idx, minPrefix, maxExpansions); err != nil {
			return err
		}
		return ValidateExpansions(n.Right, idx, minPrefix, maxExpansions)
	case *NotNode:
		return ValidateExpansions(n.Child, idx, minPrefix, maxExpansions)
	case *OptionalNode:
		return ValidateExpansions(n.Child, idx, minPrefix, maxExpansions)
	}
	return nil
}

// ExpandInFields restricts unscoped terms/phrases to the given fields (OR across fields).
func ExpandInFields(node QueryNode, fields []string) QueryNode {
	if node == nil || len(fields) == 0 {
		return node
	}
	switch n := node.(type) {
	case *TermNode:
		if n.Field != "" {
			return n
		}
		return orFieldTerms(n.Term, fields, func(term, field string) QueryNode {
			return &TermNode{Term: term, Field: field}
		})
	case *PhraseNode:
		if n.Field != "" {
			return n
		}
		var result QueryNode
		for i, f := range fields {
			p := &PhraseNode{Terms: append([]string(nil), n.Terms...), Field: f, Slop: n.Slop, InOrder: n.InOrder}
			if i == 0 {
				result = p
			} else {
				result = &OrNode{Left: result, Right: p}
			}
		}
		return result
	case *PrefixNode:
		if n.Field != "" {
			return n
		}
		return orFieldTerms(n.Prefix, fields, func(prefix, field string) QueryNode {
			return &PrefixNode{Prefix: prefix, Field: field}
		})
	case *SuffixNode:
		if n.Field != "" {
			return n
		}
		return orFieldTerms(n.Suffix, fields, func(suffix, field string) QueryNode {
			return &SuffixNode{Suffix: suffix, Field: field}
		})
	case *InfixNode:
		if n.Field != "" {
			return n
		}
		return orFieldTerms(n.Infix, fields, func(infix, field string) QueryNode {
			return &InfixNode{Infix: infix, Field: field}
		})
	case *FuzzyNode:
		if n.Field != "" {
			return n
		}
		return orFieldTerms(n.Term, fields, func(term, field string) QueryNode {
			return &FuzzyNode{Term: term, Field: field, MaxDist: n.MaxDist}
		})
	case *AndNode:
		return &AndNode{Left: ExpandInFields(n.Left, fields), Right: ExpandInFields(n.Right, fields)}
	case *OrNode:
		return &OrNode{Left: ExpandInFields(n.Left, fields), Right: ExpandInFields(n.Right, fields)}
	case *NotNode:
		return &NotNode{Child: ExpandInFields(n.Child, fields)}
	case *OptionalNode:
		return &OptionalNode{Child: ExpandInFields(n.Child, fields)}
	default:
		return node
	}
}

func orFieldTerms(term string, fields []string, makeNode func(term, field string) QueryNode) QueryNode {
	var result QueryNode
	for i, f := range fields {
		n := makeNode(term, f)
		if i == 0 {
			result = n
		} else {
			result = &OrNode{Left: result, Right: n}
		}
	}
	return result
}

// TagNode represents a tag search
type TagNode struct {
	Field string
	Tag   string
}

// Evaluate evaluates a tag node
func (n *TagNode) Evaluate(idx *InvertedIndex) []string {
	return idx.TagSearch(n.Field, n.Tag)
}

// unescapeQuery reverses RediSearch query backslash escapes ("\X" -> "X").
// Groker escapes -, :, ., /, + in tag values; real RediSearch unescapes them
// before matching. Without this, "@collection:{ collection\-c }" would look for
// the literal "collection\-c" and never match the stored tag "collection-c".
func unescapeQuery(s string) string {
	if !strings.ContainsRune(s, '\\') {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			b.WriteByte(s[i+1])
			i++
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// parseTagList turns the contents of "@field:{ ... }" into a QueryNode.
// It supports multiple tags separated by "|" (RediSearch tag OR), trims
// surrounding whitespace, and unescapes backslash escapes.
func parseTagList(field, raw string) QueryNode {
	parts := strings.Split(raw, "|")
	nodes := make([]QueryNode, 0, len(parts))
	for _, t := range parts {
		t = strings.TrimSpace(unescapeQuery(t))
		if t == "" {
			continue
		}
		nodes = append(nodes, &TagNode{Field: field, Tag: t})
	}
	switch len(nodes) {
	case 0:
		return &TagNode{Field: field, Tag: strings.TrimSpace(unescapeQuery(raw))}
	case 1:
		return nodes[0]
	}
	root := nodes[0]
	for _, n := range nodes[1:] {
		root = &OrNode{Left: root, Right: n}
	}
	return root
}

// NumericRangeNode represents @field:[min max] numeric range search.
type NumericRangeNode struct {
	Field        string
	Min, Max     float64
	MinInf       bool // -inf / no lower bound
	MaxInf       bool // +inf / no upper bound
	MinExclusive bool // "(min"
	MaxExclusive bool // "max)"
}

// Evaluate evaluates a numeric range node by scanning indexed documents.
func (n *NumericRangeNode) Evaluate(idx *InvertedIndex) []string {
	return idx.NumericRangeSearch(n.Field, n.MinInf, n.MaxInf, n.MinExclusive, n.MaxExclusive, n.Min, n.Max)
}

// parseRangeBound parses one side of "[min max]": optional "(" (exclusive)
// and a number or "-inf"/"+inf". Returns (inf, exclusive, value).
func parseRangeBound(s string) (inf, exclusive bool, val float64) {
	if strings.HasPrefix(s, "(") {
		exclusive = true
		s = s[1:]
	}
	switch strings.ToLower(s) {
	case "-inf", "-infinity":
		return true, exclusive, 0
	case "inf", "+inf", "+infinity", "infinity":
		return true, exclusive, 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		// Unparseable bound: treat as no bound rather than failing the query.
		return true, exclusive, 0
	}
	return false, exclusive, v
}

// parseNumericRange turns the contents of "@field:[ ... ]" into a NumericRangeNode.
func parseNumericRange(field, raw string) QueryNode {
	parts := strings.Fields(raw)
	node := &NumericRangeNode{Field: field, MinInf: true, MaxInf: true}
	if len(parts) >= 1 {
		node.MinInf, node.MinExclusive, node.Min = parseRangeBound(parts[0])
	}
	if len(parts) >= 2 {
		node.MaxInf, node.MaxExclusive, node.Max = parseRangeBound(parts[1])
	}
	return node
}

// GeoRangeNode represents an inline @field:[lon lat radius unit] GEO range
// query. Evaluate only narrows to documents that have the field at all
// (InvertedIndex has no access to geo indices); the real radius test is
// applied by RediSearchEngine.filterByGeoNodes as a post-filter over Search's
// result set, mirroring how GeoFilterOptions/FieldFilter are applied.
type GeoRangeNode struct {
	Field  string
	Lon    float64
	Lat    float64
	Radius float64
	Unit   string // m, km, mi, ft
}

// Evaluate returns every document that has a value for the geo field; the
// actual radius filtering happens afterwards in RediSearchEngine.
func (n *GeoRangeNode) Evaluate(idx *InvertedIndex) []string {
	return idx.FieldPresentDocIDs(n.Field)
}

// collectGeoRangeNodes walks a query AST and collects every GeoRangeNode leaf
// so Search can apply each as an additional radius filter over the result set.
func collectGeoRangeNodes(node QueryNode) []*GeoRangeNode {
	var out []*GeoRangeNode
	var walk func(QueryNode)
	walk = func(n QueryNode) {
		switch v := n.(type) {
		case *GeoRangeNode:
			out = append(out, v)
		case *AndNode:
			walk(v.Left)
			walk(v.Right)
		case *OrNode:
			walk(v.Left)
			walk(v.Right)
		case *NotNode:
			walk(v.Child)
		case *OptionalNode:
			walk(v.Child)
		}
	}
	walk(node)
	return out
}

// ExpandSynonyms walks the query AST and replaces each TermNode with
// "term OR syn1 OR syn2 ..." using expand to look up synonyms (e.g. from
// FT.SYNADD groups). Terms are lowercased for case-insensitive matching.
// Phrase terms are left untouched: substituting synonyms mid-phrase would
// require a combinatorial expansion of exact phrases that isn't worth the
// complexity for Phase A.
func ExpandSynonyms(node QueryNode, expand func(string) []string) QueryNode {
	if node == nil || expand == nil {
		return node
	}
	switch n := node.(type) {
	case *TermNode:
		syns := expand(strings.ToLower(n.Term))
		if len(syns) == 0 {
			return n
		}
		var result QueryNode = n
		for _, s := range syns {
			result = &OrNode{Left: result, Right: &TermNode{Term: strings.ToLower(s), Field: n.Field}}
		}
		return result
	case *AndNode:
		return &AndNode{Left: ExpandSynonyms(n.Left, expand), Right: ExpandSynonyms(n.Right, expand)}
	case *OrNode:
		return &OrNode{Left: ExpandSynonyms(n.Left, expand), Right: ExpandSynonyms(n.Right, expand)}
	case *NotNode:
		return &NotNode{Child: ExpandSynonyms(n.Child, expand)}
	case *OptionalNode:
		return &OptionalNode{Child: ExpandSynonyms(n.Child, expand)}
	case *PhraseNode:
		return n
	default:
		return node
	}
}

// parseRangeOrGeo turns the contents of "@field:[ ... ]" into:
//   - a GeoShapeNode when it looks like "WITHIN|CONTAINS|INTERSECTS|DISJOINT $param"
//   - a GeoRangeNode when it looks like "lon lat radius unit"
//   - a numeric range otherwise.
func parseRangeOrGeo(field, raw string) QueryNode {
	parts := strings.Fields(raw)
	// GEOSHAPE predicate: "<op> $param" (DIALECT 3+).
	if len(parts) == 2 {
		switch strings.ToUpper(parts[0]) {
		case "WITHIN", "CONTAINS", "INTERSECTS", "DISJOINT":
			if strings.HasPrefix(parts[1], "$") {
				return &GeoShapeNode{Field: field, Op: strings.ToUpper(parts[0]), Param: parts[1]}
			}
		}
	}
	if len(parts) == 4 {
		switch strings.ToLower(parts[3]) {
		case "m", "km", "mi", "ft":
			lon, err1 := strconv.ParseFloat(parts[0], 64)
			lat, err2 := strconv.ParseFloat(parts[1], 64)
			radius, err3 := strconv.ParseFloat(parts[2], 64)
			if err1 == nil && err2 == nil && err3 == nil {
				return &GeoRangeNode{Field: field, Lon: lon, Lat: lat, Radius: radius, Unit: strings.ToLower(parts[3])}
			}
		}
	}
	return parseNumericRange(field, raw)
}

// GeoShapeNode evaluates a GEOSHAPE spatial predicate @field:[OP $param]. The
// param's WKT value is resolved from SearchOptions.Params at post-filter time
// (engine.filterByGeoshapeNodes). Evaluate narrows to docs that HAVE the field,
// mirroring the GeoRangeNode approach, and the engine applies the real spatial
// test as a post-filter.
type GeoShapeNode struct {
	Field string
	Op    string // WITHIN | CONTAINS | INTERSECTS | DISJOINT
	Param string // $-parameter name carrying the query WKT
}

// Evaluate returns doc IDs that have a value for the field (the engine's
// post-filter narrows by the actual spatial predicate).
func (n *GeoShapeNode) Evaluate(idx *InvertedIndex) []string {
	return idx.FieldPresentDocIDs(n.Field)
}

// QueryParser parses query strings into query nodes
type QueryParser struct {
	tokenizer *StandardTokenizer
}

// NewQueryParser creates a new query parser
func NewQueryParser() *QueryParser {
	return &QueryParser{
		tokenizer: &StandardTokenizer{},
	}
}

// Parse parses a query string into a QueryNode
func (p *QueryParser) Parse(query string) (QueryNode, error) {
	query = strings.TrimSpace(query)
	
	if query == "" {
		return nil, fmt.Errorf("empty query")
	}
	
	// Simple parsing for now
	// Handle field-specific queries: @title:hello
	// Handle OR: hello | world
	// Handle AND: hello world
	// Handle NOT: hello -world
	// Handle prefix: hel*
	// Handle tags: @tags:{red}
	
	// Check for OR
	if strings.Contains(query, "|") {
		parts := strings.Split(query, "|")
		if len(parts) == 2 {
			left, err := p.Parse(strings.TrimSpace(parts[0]))
			if err != nil {
				return nil, err
			}
			right, err := p.Parse(strings.TrimSpace(parts[1]))
			if err != nil {
				return nil, err
			}
			return &OrNode{Left: left, Right: right}, nil
		}
	}
	
	// Tokenize the query
	tokens := p.tokenizer.Tokenize(query)
	
	var nodes []QueryNode
	var optionalNodes []QueryNode
	negateNext := false
	optionalNext := false
	
	for _, token := range tokens {
		if token == "-" {
			negateNext = true
			continue
		}
		
		if token == "~" {
			optionalNext = true
			continue
		}
		
		// Check for field prefix
		field := ""
		term := token
		
		if strings.HasPrefix(token, "@") {
			colonIdx := strings.Index(token, ":")
			if colonIdx > 0 {
				field = token[1:colonIdx]
				term = token[colonIdx+1:]
			}
		}
		
		// Check for tag syntax @field:{tag}
		if strings.HasPrefix(term, "{") && strings.HasSuffix(term, "}") {
			tag := term[1 : len(term)-1]
			node := parseTagList(field, tag)
			if negateNext {
				node = &NotNode{Child: node}
				negateNext = false
			}
			nodes = append(nodes, node)
			continue
		}
		
		// Wildcard / fuzzy detection. Order matters: fuzzy (%term%) before
		// suffix (*term) before infix (*term*) before prefix (term*). Each
		// branch constructs the appropriate node and applies pending negate.
		makeNode := func(n QueryNode) QueryNode {
			if negateNext {
				negateNext = false
				return &NotNode{Child: n}
			}
			return n
		}

		// Fuzzy: %t% (dist 1), %%t%% (dist 2), %%%t%%% (dist 3, max).
		if dist := fuzzyDistance(term); dist > 0 {
			inner := strings.Trim(term, "%")
			nodes = append(nodes, makeNode(&FuzzyNode{Term: inner, Field: field, MaxDist: dist}))
			continue
		}

		// Infix: *infix* (contains). Must check before suffix (leading *).
		if len(term) > 2 && strings.HasPrefix(term, "*") && strings.HasSuffix(term, "*") {
			infix := term[1 : len(term)-1]
			nodes = append(nodes, makeNode(&InfixNode{Infix: infix, Field: field}))
			continue
		}

		// Suffix: *suffix.
		if strings.HasPrefix(term, "*") && !strings.HasSuffix(term, "*") && len(term) > 1 {
			suffix := term[1:]
			nodes = append(nodes, makeNode(&SuffixNode{Suffix: suffix, Field: field}))
			continue
		}

		// Prefix: prefix*.
		if strings.HasSuffix(term, "*") && !strings.HasPrefix(term, "*") && len(term) > 1 {
			prefix := term[:len(term)-1]
			nodes = append(nodes, makeNode(&PrefixNode{Prefix: prefix, Field: field}))
			continue
		}
		
		var node QueryNode
		if field != "" {
			node = &TermNode{Term: term, Field: field}
		} else {
			node = &TermNode{Term: term}
		}
		
		if negateNext {
			node = &NotNode{Child: node}
			negateNext = false
		}
		
		if optionalNext {
			optionalNodes = append(optionalNodes, &OptionalNode{Child: node})
			optionalNext = false
		} else {
			nodes = append(nodes, node)
		}
	}
	
	// Combine required nodes with AND
	if len(nodes) == 0 && len(optionalNodes) == 0 {
		return nil, fmt.Errorf("empty query after parsing")
	}
	
	// Add optional nodes to the main nodes (they don't filter but will be evaluated for scoring)
	nodes = append(nodes, optionalNodes...)
	
	if len(nodes) == 1 {
		return nodes[0], nil
	}
	
	result := &AndNode{Left: nodes[0], Right: nodes[1]}
	for i := 2; i < len(nodes); i++ {
		result = &AndNode{Left: result, Right: nodes[i]}
	}
	
	return result, nil
}

// ParseQueryWithFields parses a query supporting field-specific syntax
func ParseQueryWithFields(query string) ([]QueryCondition, error) {
	var conditions []QueryCondition
	
	// Simple tokenizer that handles quoted strings
	tokens := tokenizeWithQuotes(query)
	
	for _, token := range tokens {
		cond := QueryCondition{}
		
		// Check for field specification
		if strings.HasPrefix(token, "@") {
			colonIdx := strings.Index(token, ":")
			if colonIdx > 0 {
				cond.Field = token[1:colonIdx]
				token = token[colonIdx+1:]
			}
		}
		
		// Check for range
		if strings.HasPrefix(token, "[") && strings.HasSuffix(token, "]") {
			// Numeric range [min max]
			rangeContent := token[1 : len(token)-1]
			parts := strings.Fields(rangeContent)
			if len(parts) == 2 {
				min, _ := strconv.ParseFloat(parts[0], 64)
				max, _ := strconv.ParseFloat(parts[1], 64)
				cond.Min = &min
				cond.Max = &max
				cond.Type = RangeCondition
				conditions = append(conditions, cond)
				continue
			}
		}
		
		// Check for tag
		if strings.HasPrefix(token, "{") && strings.HasSuffix(token, "}") {
			cond.Type = TagCondition
			cond.Value = token[1 : len(token)-1]
			conditions = append(conditions, cond)
			continue
		}
		
		// Check for prefix
		if strings.HasSuffix(token, "*") {
			cond.Type = PrefixCondition
			cond.Value = token[:len(token)-1]
			conditions = append(conditions, cond)
			continue
		}
		
		// Default: term
		cond.Type = TermCondition
		cond.Value = token
		conditions = append(conditions, cond)
	}
	
	return conditions, nil
}

// QueryCondition represents a single query condition
type QueryCondition struct {
	Field string
	Type  ConditionType
	Value string
	Min   *float64
	Max   *float64
}

// ConditionType represents the type of condition
type ConditionType int

const (
	TermCondition ConditionType = iota
	PrefixCondition
	TagCondition
	RangeCondition
	FuzzyCondition
)

func tokenizeWithQuotes(input string) []string {
	var tokens []string
	var current strings.Builder
	inQuotes := false
	quoteChar := rune(0)
	
	for _, r := range input {
		if !inQuotes && (r == '"' || r == '\'') {
			inQuotes = true
			quoteChar = r
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		} else if inQuotes && r == quoteChar {
			inQuotes = false
			tokens = append(tokens, current.String())
			current.Reset()
		} else if !inQuotes && (r == ' ' || r == '\t') {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
		} else {
			current.WriteRune(r)
		}
	}
	
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	
	return tokens
}

// ExpressionParser for complex boolean expressions
type ExpressionParser struct {
	input   string
	pos     int
	dialect int // 0 or 1 = D1 precedence (| looser than space); >= 2 = D2 (| tighter)
}

// NewExpressionParser creates a new expression parser
func NewExpressionParser(input string) *ExpressionParser {
	return &ExpressionParser{input: input}
}

// SetDialect configures D1 vs D2 precedence. dialect >= 2 makes `|` bind
// tighter than whitespace-separated terms (Redis DIALECT 2 behavior).
func (p *ExpressionParser) SetDialect(d int) { p.dialect = d }

// Parse parses a boolean expression
// Supports: term, "phrase", @field:term, @field:{tag}, (expr), expr AND expr, expr OR expr, NOT expr
func (p *ExpressionParser) Parse() (QueryNode, error) {
	if p.dialect >= 2 {
		return p.parseAndD2()
	}
	return p.parseOr()
}

// parseAndD2 is the DIALECT 2 top level: whitespace separates OrExpr groups
// that are ANDed together, so `|` (inside parseOrD2) binds tighter than space.
// `a b | c` parses as `a (b | c)` under D2.
func (p *ExpressionParser) parseAndD2() (QueryNode, error) {
	left, err := p.parseOrD2()
	if err != nil {
		return nil, err
	}
	for p.matchKeyword("AND") || p.peekTerm() {
		right, err := p.parseOrD2()
		if err != nil {
			return nil, err
		}
		left = &AndNode{Left: left, Right: right}
	}
	return left, nil
}

// parseOrD2 collects | -joined primaries (tighter than the surrounding AND).
func (p *ExpressionParser) parseOrD2() (QueryNode, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.match("|") || p.matchKeyword("OR") {
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &OrNode{Left: left, Right: right}
	}
	return left, nil
}

func (p *ExpressionParser) parseOr() (QueryNode, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	
	for p.match("|") || p.matchKeyword("OR") {
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &OrNode{Left: left, Right: right}
	}
	
	return left, nil
}

func (p *ExpressionParser) parseAnd() (QueryNode, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	
	for p.matchKeyword("AND") || p.peekTerm() {
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &AndNode{Left: left, Right: right}
	}
	
	return left, nil
}

func (p *ExpressionParser) parseNot() (QueryNode, error) {
	if p.match("-") || p.matchKeyword("NOT") {
		child, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &NotNode{Child: child}, nil
	}
	// ~ prefix marks an optional term (boosts score, never filters).
	if p.match("~") {
		child, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &OptionalNode{Child: child}, nil
	}

	return p.parsePrimary()
}

func (p *ExpressionParser) parsePrimary() (QueryNode, error) {
	p.skipWhitespace()

	// ismissing(@field) — DIALECT 2 function query. Detect before '(' / '@'.
	if p.matchKeyword("ISMISSING") {
		p.skipWhitespace()
		if !p.match("(") {
			return nil, fmt.Errorf("expected '(' after ismissing")
		}
		p.skipWhitespace()
		if !p.match("@") {
			return nil, fmt.Errorf("ismissing expects a @field argument")
		}
		fieldStart := p.pos
		for p.pos < len(p.input) && (isAlphaNum(rune(p.input[p.pos])) || p.input[p.pos] == '_') {
			p.pos++
		}
		field := p.input[fieldStart:p.pos]
		p.skipWhitespace()
		if !p.match(")") {
			return nil, fmt.Errorf("expected ')' to close ismissing")
		}
		return &MissingNode{Field: field}, nil
	}

	if p.match("(") {
		node, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		if !p.match(")") {
			return nil, fmt.Errorf("expected ')'")
		}
		return node, nil
	}
	
	// Parse field specification: @field  or  @f1|f2|f3 (DIALECT 2 multi-field).
	fields := []string{}
	if p.match("@") {
		specStart := p.pos
		for p.pos < len(p.input) && (isAlphaNum(rune(p.input[p.pos])) || p.input[p.pos] == '_' || p.input[p.pos] == '|') {
			p.pos++
		}
		spec := p.input[specStart:p.pos]
		if spec == "" {
			return nil, fmt.Errorf("expected field name after '@'")
		}
		fields = strings.Split(spec, "|")

		// DIALECT 2 comparison operators: @n == 5, @n != 5, @n > 5, ...
		// These appear WITHOUT a colon and only over a single numeric field.
		if op := p.matchCompareOp(); op != "" {
			if len(fields) > 1 {
				return nil, fmt.Errorf("comparison operator cannot apply to multiple fields")
			}
			p.skipWhitespace()
			numStart := p.pos
			for p.pos < len(p.input) && !isWhitespace(rune(p.input[p.pos])) && p.input[p.pos] != ')' && p.input[p.pos] != '|' {
				p.pos++
			}
			numStr := strings.TrimSpace(p.input[numStart:p.pos])
			v, perr := strconv.ParseFloat(numStr, 64)
			if perr != nil {
				return nil, fmt.Errorf("invalid numeric value '%s' in comparison", numStr)
			}
			return &NumericCompareNode{Field: fields[0], Op: op, Value: v}, nil
		}

		if !p.match(":") {
			return nil, fmt.Errorf("expected ':' after field name")
		}
	}

	primaryField := ""
	if len(fields) > 0 {
		primaryField = fields[0]
	}

	atom, err := p.parseAtom(primaryField)
	if err != nil {
		return nil, err
	}
	// Multi-field: OR-expand the atom across the remaining fields.
	for fi := 1; fi < len(fields); fi++ {
		cloned := withField(atom, fields[fi])
		if cloned == nil {
			// Atom type doesn't support field reassignment (e.g. tag/range);
			// fall back to the first-field atom only.
			break
		}
		atom = &OrNode{Left: atom, Right: cloned}
	}
	return atom, nil
}

// matchCompareOp returns the comparison operator at the current position (one
// of ==, !=, >=, <=, >, <) or "" when none matches. The two-char ops are tried
// first so ">=" isn't misread as ">".
func (p *ExpressionParser) matchCompareOp() string {
	p.skipWhitespace()
	for _, op := range []string{"==", "!=", ">=", "<=", ">", "<"} {
		if strings.HasPrefix(p.input[p.pos:], op) {
			p.pos += len(op)
			return op
		}
	}
	return ""
}

// parseAtom parses the value side following a field specifier (or an unscoped
// term): tag list {...}, numeric/geo range [...], phrase "...", or a bare
// term/prefix/fuzzy token. field is the field scope ("" for unscoped).
func (p *ExpressionParser) parseAtom(field string) (QueryNode, error) {
	p.skipWhitespace()

	if p.match("{") {
		// Tag list: @field:{ tag | tag }
		tagStart := p.pos
		for p.pos < len(p.input) && p.input[p.pos] != '}' {
			p.pos++
		}
		raw := p.input[tagStart:p.pos]
		p.match("}")
		return parseTagList(field, raw), nil
	}

	if p.match("[") {
		// Numeric range: @field:[min max] (with optional "(" exclusive and -inf/+inf)
		// or GEO range: @field:[lon lat radius unit]
		rangeStart := p.pos
		for p.pos < len(p.input) && p.input[p.pos] != ']' {
			p.pos++
		}
		raw := p.input[rangeStart:p.pos]
		p.match("]")
		return parseRangeOrGeo(field, strings.TrimSpace(raw)), nil
	}

	if p.match("\"") {
		// Phrase — tokenize into PhraseNode (ordered; SLOP applied later via opts)
		phraseStart := p.pos
		for p.pos < len(p.input) && p.input[p.pos] != '"' {
			p.pos++
		}
		phrase := p.input[phraseStart:p.pos]
		p.match("\"")
		tok := &StandardTokenizer{}
		terms := tok.Tokenize(phrase)
		if len(terms) == 0 {
			return &TermNode{Term: phrase, Field: field}, nil
		}
		if len(terms) == 1 {
			return &TermNode{Term: terms[0], Field: field}, nil
		}
		return &PhraseNode{Terms: terms, Field: field, Slop: 0, InOrder: true}, nil
	}

	// Simple term or prefix
	termStart := p.pos
	for p.pos < len(p.input) && !isWhitespace(rune(p.input[p.pos])) && p.input[p.pos] != ')' && p.input[p.pos] != '|' {
		p.pos++
	}

	if termStart == p.pos {
		return nil, fmt.Errorf("expected term at position %d", p.pos)
	}

	term := p.input[termStart:p.pos]

	// Wildcard / fuzzy detection (kept in sync with the fallback QueryParser).
	if dist := fuzzyDistance(term); dist > 0 {
		return &FuzzyNode{Term: strings.Trim(term, "%"), Field: field, MaxDist: dist}, nil
	}
	if len(term) > 2 && strings.HasPrefix(term, "*") && strings.HasSuffix(term, "*") {
		return &InfixNode{Infix: term[1 : len(term)-1], Field: field}, nil
	}
	if strings.HasPrefix(term, "*") && !strings.HasSuffix(term, "*") && len(term) > 1 {
		return &SuffixNode{Suffix: term[1:], Field: field}, nil
	}
	if strings.HasSuffix(term, "*") && !strings.HasPrefix(term, "*") && len(term) > 1 {
		return &PrefixNode{Prefix: term[:len(term)-1], Field: field}, nil
	}

	return &TermNode{Term: term, Field: field}, nil
}

// withField returns a shallow copy of node with its Field replaced, or nil if
// the node type doesn't carry a reassignable Field (used for @f1|f2 expansion).
func withField(node QueryNode, field string) QueryNode {
	switch n := node.(type) {
	case *TermNode:
		cp := *n
		cp.Field = field
		return &cp
	case *PrefixNode:
		cp := *n
		cp.Field = field
		return &cp
	case *SuffixNode:
		cp := *n
		cp.Field = field
		return &cp
	case *InfixNode:
		cp := *n
		cp.Field = field
		return &cp
	case *FuzzyNode:
		cp := *n
		cp.Field = field
		return &cp
	case *PhraseNode:
		cp := *n
		cp.Field = field
		return &cp
	}
	return nil
}

func (p *ExpressionParser) match(s string) bool {
	p.skipWhitespace()
	if strings.HasPrefix(p.input[p.pos:], s) {
		p.pos += len(s)
		return true
	}
	return false
}

func (p *ExpressionParser) matchKeyword(kw string) bool {
	p.skipWhitespace()
	if !strings.HasPrefix(strings.ToUpper(p.input[p.pos:]), kw) {
		return false
	}
	
	// Ensure it's a whole word
	end := p.pos + len(kw)
	if end < len(p.input) && isAlphaNum(rune(p.input[end])) {
		return false
	}
	
	p.pos = end
	return true
}

func (p *ExpressionParser) peekTerm() bool {
	p.skipWhitespace()
	return p.pos < len(p.input) && p.input[p.pos] != ')' && p.input[p.pos] != '|'
}

func (p *ExpressionParser) skipWhitespace() {
	for p.pos < len(p.input) && isWhitespace(rune(p.input[p.pos])) {
		p.pos++
	}
}

func isWhitespace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r'
}

func isAlphaNum(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_'
}

func (p *ExpressionParser) remaining() string {
	if p.pos >= len(p.input) {
		return ""
	}
	return p.input[p.pos:]
}

// ExplainNode renders a parsed query AST as a RediSearch-style execution plan
// tree, e.g.:
//
//	INTERSECT {
//	  TERM{hello}
//	  TERM{world}
//	}
//
// Used by FT.EXPLAIN so the output reflects the real query plan rather than a
// hand-rolled approximation.
func ExplainNode(node QueryNode) string {
	var b strings.Builder
	explainNode(&b, node, 0)
	return b.String()
}

func explainNode(b *strings.Builder, node QueryNode, depth int) {
	indent := strings.Repeat("  ", depth)
	switch n := node.(type) {
	case nil:
		b.WriteString(indent + "EMPTY\n")
	case *TermNode:
		b.WriteString(indent + "TERM{" + n.Term + "}\n")
	case *PhraseNode:
		b.WriteString(indent + "PHRASE{" + strings.Join(n.Terms, " ") + "}\n")
	case *PrefixNode:
		b.WriteString(indent + "PREFIX{" + n.Prefix + "}\n")
	case *SuffixNode:
		b.WriteString(indent + "SUFFIX{" + n.Suffix + "}\n")
	case *InfixNode:
		b.WriteString(indent + "INFIX{" + n.Infix + "}\n")
	case *FuzzyNode:
		b.WriteString(indent + "FUZZY{" + n.Term + "} (DIST " + strconv.Itoa(n.MaxDist) + ")\n")
	case *TagNode:
		b.WriteString(indent + "TAG{" + n.Field + ":" + n.Tag + "}\n")
	case *NumericRangeNode:
		min, max := n.Min, n.Max
		if n.MinInf {
			min = math.Inf(-1)
		}
		if n.MaxInf {
			max = math.Inf(1)
		}
		b.WriteString(indent + "RANGE{" + n.Field + " [" + formatRangeBound(min, n.MinExclusive) + " " + formatRangeBound(max, n.MaxExclusive) + "]}\n")
	case *NumericCompareNode:
		b.WriteString(indent + "COMPARE{" + n.Field + " " + n.Op + " " + strconv.FormatFloat(n.Value, 'f', -1, 64) + "}\n")
	case *GeoRangeNode:
		b.WriteString(indent + "GEORANGE{" + n.Field + " " + strconv.FormatFloat(n.Lon, 'f', -1, 64) + " " + strconv.FormatFloat(n.Lat, 'f', -1, 64) + " " + strconv.FormatFloat(n.Radius, 'f', -1, 64) + " " + n.Unit + "}\n")
	case *GeoShapeNode:
		b.WriteString(indent + "GEOSHAPE{" + n.Field + " " + n.Op + " " + n.Param + "}\n")
	case *MissingNode:
		b.WriteString(indent + "MISSING{" + n.Field + "}\n")
	case *AndNode:
		b.WriteString(indent + "INTERSECT {\n")
		explainNode(b, n.Left, depth+1)
		explainNode(b, n.Right, depth+1)
		b.WriteString(indent + "}\n")
	case *OrNode:
		b.WriteString(indent + "UNION {\n")
		explainNode(b, n.Left, depth+1)
		explainNode(b, n.Right, depth+1)
		b.WriteString(indent + "}\n")
	case *NotNode:
		b.WriteString(indent + "NOT {\n")
		explainNode(b, n.Child, depth+1)
		b.WriteString(indent + "}\n")
	case *OptionalNode:
		b.WriteString(indent + "OPTIONAL {\n")
		explainNode(b, n.Child, depth+1)
		b.WriteString(indent + "}\n")
	default:
		b.WriteString(indent + "NODE\n")
	}
}

func formatRangeBound(v float64, exclusive bool) string {
	prefix := ""
	if exclusive {
		prefix = "("
	}
	if math.IsInf(v, 1) {
		return prefix + "+inf"
	}
	if math.IsInf(v, -1) {
		return prefix + "-inf"
	}
	return prefix + strconv.FormatFloat(v, 'f', -1, 64)
}
