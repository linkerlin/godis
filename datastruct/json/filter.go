package json

// JSONPath filter expressions — Redis 8.10 batch 1 (R5-20).
//
// Supported inside `[?( ... )]`: comparisons ==, !=, <, <=, >, >= against any
// literal (number, 'string', "string", true, false, null, and JSON
// array/object literals compared by deep equality), ! negation, && and ||,
// and parentheses. `@` refers to the element under test and may carry a
// relative path (`.a.b[0]`).
//
// ponytail: the filter segment must be the LAST path component (a filter
// mid-path is rejected, not silently mis-evaluated); nested brackets inside
// the predicate are not supported by the segment splitter. Batch 2 (in/nin)
// and batch 3 (functions) extend evalCompare/scanOperand.

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrJSONPathSyntax marks malformed filter expressions so command handlers
// can reply with an error instead of the legacy path-not-found null.
var ErrJSONPathSyntax = errors.New("ERR Path syntax error")

// IsFilterPart reports whether a parsed path component is a filter segment.
func IsFilterPart(part string) bool {
	return strings.HasPrefix(part, "?(") && strings.HasSuffix(part, ")")
}

// PathHasFilter reports whether any component of path is a filter segment
// (used to skip the `$`-path single-value wrapping: a filter already yields
// an array).
func PathHasFilter(path string) bool {
	for _, part := range parsePath(path) {
		if IsFilterPart(part) {
			return true
		}
	}
	return false
}

// ApplyFilter evaluates pred against each element of arr and returns the
// matching elements as a new array.
func ApplyFilter(arr []interface{}, pred string) ([]interface{}, error) {
	expr := strings.TrimSpace(pred)
	if !strings.HasPrefix(expr, "?(") || !strings.HasSuffix(expr, ")") {
		return nil, ErrJSONPathSyntax
	}
	toks, err := tokenize(expr[2 : len(expr)-1])
	if err != nil {
		return nil, err
	}
	p := &predParser{toks: toks}
	node, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	if p.pos != len(p.toks) {
		return nil, ErrJSONPathSyntax
	}
	out := make([]interface{}, 0, len(arr))
	for _, elem := range arr {
		if evalNode(node, elem) {
			out = append(out, elem)
		}
	}
	return out, nil
}

// ---- tokens ----

type tokenKind int

const (
	tkLParen tokenKind = iota
	tkRParen
	tkNot
	tkAnd
	tkOr
	tkCmp // == != < <= > >=
	tkAt  // @ plus relative path, .tok carries the path
	tkLit // literal, .tok carries parsed JSON value
)

type token struct {
	kind tokenKind
	tok  string // operator text or @-relative path
	val  interface{}
}

func tokenize(s string) ([]token, error) {
	var toks []token
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t':
			i++
		case c == '(':
			toks = append(toks, token{kind: tkLParen})
			i++
		case c == ')':
			toks = append(toks, token{kind: tkRParen})
			i++
		case c == '!':
			if i+1 < len(s) && s[i+1] == '=' {
				toks = append(toks, token{kind: tkCmp, tok: "!="})
				i += 2
			} else {
				toks = append(toks, token{kind: tkNot})
				i++
			}
		case c == '&' && i+1 < len(s) && s[i+1] == '&':
			toks = append(toks, token{kind: tkAnd})
			i += 2
		case c == '|' && i+1 < len(s) && s[i+1] == '|':
			toks = append(toks, token{kind: tkOr})
			i += 2
		case c == '=' && i+1 < len(s) && s[i+1] == '=':
			toks = append(toks, token{kind: tkCmp, tok: "=="})
			i += 2
		case c == '<' && i+1 < len(s) && s[i+1] == '=':
			toks = append(toks, token{kind: tkCmp, tok: "<="})
			i += 2
		case c == '>' && i+1 < len(s) && s[i+1] == '=':
			toks = append(toks, token{kind: tkCmp, tok: ">="})
			i += 2
		case c == '<':
			toks = append(toks, token{kind: tkCmp, tok: "<"})
			i++
		case c == '>':
			toks = append(toks, token{kind: tkCmp, tok: ">"})
			i++
		case c == '@':
			j := i + 1
			for j < len(s) {
				ch := s[j]
				if ch == '.' || ch == '[' || ch == ']' ||
					(ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
					(ch >= '0' && ch <= '9') || ch == '_' || ch == '-' {
					j++
				} else {
					break
				}
			}
			toks = append(toks, token{kind: tkAt, tok: s[i+1 : j]})
			i = j
		case c == '\'' || c == '"':
			q := c
			j := i + 1
			for j < len(s) && s[j] != q {
				j++
			}
			if j >= len(s) {
				return nil, ErrJSONPathSyntax
			}
			toks = append(toks, token{kind: tkLit, val: s[i+1 : j]})
			i = j + 1
		default:
			// number / true / false / null / JSON composite literal
			j := i
			depth := 0
			for j < len(s) {
				ch := s[j]
				if ch == '{' || ch == '[' {
					depth++
				} else if ch == ')' || ch == '&' || ch == '|' {
					break
				} else if ch == ']' {
					if depth == 0 {
						break
					}
					depth--
				} else if ch == ' ' && depth == 0 {
					break
				}
				j++
			}
			if j == i {
				return nil, ErrJSONPathSyntax
			}
			raw := s[i:j]
			var v interface{}
			if err := json.Unmarshal([]byte(raw), &v); err != nil {
				return nil, ErrJSONPathSyntax
			}
			toks = append(toks, token{kind: tkLit, val: v})
			i = j
		}
	}
	return toks, nil
}

// ---- parser (precedence: || < && < ! < comparison) ----

type predNode struct {
	op    string // "or","and","not", or comparison operator, "" = operand-only
	left  *predNode
	right *predNode
	self  *token // operand token when op == ""
}

type predParser struct {
	toks []token
	pos  int
}

func (p *predParser) peek() *token {
	if p.pos < len(p.toks) {
		return &p.toks[p.pos]
	}
	return nil
}

func (p *predParser) parseOr() (*predNode, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t == nil || t.kind != tkOr {
			return left, nil
		}
		p.pos++
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &predNode{op: "or", left: left, right: right}
	}
}

func (p *predParser) parseAnd() (*predNode, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t == nil || t.kind != tkAnd {
			return left, nil
		}
		p.pos++
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &predNode{op: "and", left: left, right: right}
	}
}

func (p *predParser) parseUnary() (*predNode, error) {
	t := p.peek()
	if t != nil && t.kind == tkNot {
		p.pos++
		inner, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &predNode{op: "not", left: inner}, nil
	}
	return p.parseComparison()
}

func (p *predParser) parseComparison() (*predNode, error) {
	t := p.peek()
	if t == nil {
		return nil, ErrJSONPathSyntax
	}
	if t.kind == tkLParen {
		p.pos++
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		closing := p.peek()
		if closing == nil || closing.kind != tkRParen {
			return nil, ErrJSONPathSyntax
		}
		p.pos++
		return inner, nil
	}
	if t.kind != tkAt && t.kind != tkLit {
		return nil, ErrJSONPathSyntax
	}
	p.pos++
	next := p.peek()
	if next == nil || next.kind != tkCmp {
		// Bare operand: truthiness (non-null, non-false, non-zero).
		return &predNode{self: t}, nil
	}
	p.pos++
	rhs := p.peek()
	if rhs == nil || (rhs.kind != tkAt && rhs.kind != tkLit) {
		return nil, ErrJSONPathSyntax
	}
	p.pos++
	return &predNode{op: next.tok, left: &predNode{self: t}, right: &predNode{self: rhs}}, nil
}

// ---- evaluation ----

func evalNode(n *predNode, elem interface{}) bool {
	switch n.op {
	case "or":
		return evalNode(n.left, elem) || evalNode(n.right, elem)
	case "and":
		return evalNode(n.left, elem) && evalNode(n.right, elem)
	case "not":
		return !evalNode(n.left, elem)
	case "":
		v := operandValue(n.self, elem)
		return truthy(v)
	default:
		l := operandValue(n.left.self, elem)
		r := operandValue(n.right.self, elem)
		ok, err := compare(l, r, n.op)
		return err == nil && ok
	}
}

// operandValue resolves an operand token against the current element.
func operandValue(t *token, elem interface{}) interface{} {
	if t.kind == tkLit {
		return t.val
	}
	return walkRelative(elem, t.tok)
}

// walkRelative resolves a relative path like ".a.b[0]" ("" = the element
// itself) from start.
func walkRelative(start interface{}, rel string) interface{} {
	current := start
	for _, part := range parsePath("@" + rel) {
		if part == "@" {
			continue
		}
		switch c := current.(type) {
		case map[string]interface{}:
			v, ok := c[part]
			if !ok {
				return nil
			}
			current = v
		case []interface{}:
			idx, err := strconv.Atoi(part)
			if err != nil || idx < 0 || idx >= len(c) {
				return nil
			}
			current = c[idx]
		default:
			return nil
		}
	}
	return current
}

func truthy(v interface{}) bool {
	switch t := v.(type) {
	case nil:
		return false
	case bool:
		return t
	case float64:
		return t != 0
	case string:
		return t != ""
	default:
		return true
	}
}

// compare applies a comparison operator; numbers compare across int/float.
func compare(l, r interface{}, op string) (bool, error) {
	if op == "==" || op == "!=" {
		eq := deepEqual(l, r)
		if op == "==" {
			return eq, nil
		}
		return !eq, nil
	}
	lf, lok := toFloat(l)
	rf, rok := toFloat(r)
	if lok && rok {
		switch op {
		case "<":
			return lf < rf, nil
		case "<=":
			return lf <= rf, nil
		case ">":
			return lf > rf, nil
		case ">=":
			return lf >= rf, nil
		}
	}
	ls, lok2 := l.(string)
	rs, rok2 := r.(string)
	if lok2 && rok2 {
		switch op {
		case "<":
			return ls < rs, nil
		case "<=":
			return ls <= rs, nil
		case ">":
			return ls > rs, nil
		case ">=":
			return ls >= rs, nil
		}
	}
	return false, fmt.Errorf("incomparable operands")
}

func toFloat(v interface{}) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	}
	return 0, false
}

// deepEqual compares JSON-decoded values (composites by reflect; scalars
// with number/string/bool/null equivalence).
func deepEqual(l, r interface{}) bool {
	if lf, ok := toFloat(l); ok {
		if rf, ok2 := toFloat(r); ok2 {
			return lf == rf
		}
		return false
	}
	if _, ok := l.(string); ok {
		ls, ok1 := l.(string)
		rs, ok2 := r.(string)
		return ok1 && ok2 && ls == rs
	}
	if lb, ok := l.(bool); ok {
		rb, ok2 := r.(bool)
		return ok2 && lb == rb
	}
	if l == nil || r == nil {
		return l == nil && r == nil
	}
	return fmtEqual(l, r)
}

func fmtEqual(l, r interface{}) bool {
	lb, err1 := json.Marshal(l)
	rb, err2 := json.Marshal(r)
	// ponytail: marshal-based deep equality — key order from encoding/json
	// is deterministic for map[string]interface{}, so this is exact.
	return err1 == nil && err2 == nil && string(lb) == string(rb)
}
