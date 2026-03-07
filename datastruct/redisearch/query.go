package redisearch

import (
	"fmt"
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

// PrefixNode represents a prefix search
type PrefixNode struct {
	Prefix string
	Field  string
}

// Evaluate evaluates a prefix node
func (n *PrefixNode) Evaluate(idx *InvertedIndex) []string {
	return idx.PrefixSearch(n.Prefix, n.Field)
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
	negateNext := false
	
	for _, token := range tokens {
		if token == "-" {
			negateNext = true
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
			node := &TagNode{Field: field, Tag: tag}
			if negateNext {
				node = &TagNode{Field: field, Tag: tag} // TODO: proper NOT handling
				negateNext = false
			}
			nodes = append(nodes, node)
			continue
		}
		
		// Check for prefix search
		if strings.HasSuffix(term, "*") {
			prefix := term[:len(term)-1]
			node := &PrefixNode{Prefix: prefix, Field: field}
			if negateNext {
				node = &PrefixNode{Prefix: prefix, Field: field}
				negateNext = false
			}
			nodes = append(nodes, node)
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
		
		nodes = append(nodes, node)
	}
	
	// Combine nodes with AND
	if len(nodes) == 0 {
		return nil, fmt.Errorf("empty query after parsing")
	}
	
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
	input string
	pos   int
}

// NewExpressionParser creates a new expression parser
func NewExpressionParser(input string) *ExpressionParser {
	return &ExpressionParser{input: input}
}

// Parse parses a boolean expression
// Supports: term, "phrase", @field:term, @field:{tag}, (expr), expr AND expr, expr OR expr, NOT expr
func (p *ExpressionParser) Parse() (QueryNode, error) {
	return p.parseOr()
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
	
	return p.parsePrimary()
}

func (p *ExpressionParser) parsePrimary() (QueryNode, error) {
	p.skipWhitespace()
	
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
	
	// Parse field specification
	field := ""
	if p.match("@") {
		fieldStart := p.pos
		for p.pos < len(p.input) && (isAlphaNum(rune(p.input[p.pos])) || p.input[p.pos] == '_') {
			p.pos++
		}
		field = p.input[fieldStart:p.pos]
		if !p.match(":") {
			return nil, fmt.Errorf("expected ':' after field name")
		}
	}
	
	// Parse term or phrase
	p.skipWhitespace()
	
	if p.match("{") {
		// Tag
		tagStart := p.pos
		for p.pos < len(p.input) && p.input[p.pos] != '}' {
			p.pos++
		}
		tag := p.input[tagStart:p.pos]
		p.match("}")
		return &TagNode{Field: field, Tag: tag}, nil
	}
	
	if p.match("\"") {
		// Phrase
		phraseStart := p.pos
		for p.pos < len(p.input) && p.input[p.pos] != '"' {
			p.pos++
		}
		phrase := p.input[phraseStart:p.pos]
		p.match("\"")
		return &TermNode{Term: phrase, Field: field}, nil
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
	
	// Check for prefix
	if strings.HasSuffix(term, "*") && len(term) > 1 {
		return &PrefixNode{Prefix: term[:len(term)-1], Field: field}, nil
	}
	
	return &TermNode{Term: term, Field: field}, nil
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
