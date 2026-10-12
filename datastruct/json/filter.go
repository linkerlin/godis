package json

// JSONPath filter expressions — Redis 8.10 batches 1–3a (R5-20).
//
// Supported inside `[?( ... )]`:
//   - comparisons ==, !=, <, <=, >, >= against any literal (number, string,
//     bool, null, JSON array/object by deep equality)
//   - membership: `lhs in [...]`, `lhs nin [...]` (array literal or an
//     @-path resolving to an array); a missing left side matches neither
//   - arithmetic on numbers: binary + - * / % and unary -/+ (a non-numeric
//     operand or division by zero evaluates to Nothing — condition false,
//     never an error)
//   - size/sizeof: length of a string/array/object, optionally followed by
//     a comparison (default ==); empty: true when the length is 0
//   - ! negation, && and ||, parentheses; `@` relative paths (`.a.b[0]`);
//     bare operands test truthiness
//
// ponytail: comparisons with a missing (nil) operand keep the batch-1
// convention (@.missing == null is true); functions (match/search/concat/
// min/…) and top-level projection expressions are batch 3 proper.

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
	tkCmp   // == != < <= > >= in nin
	tkArith // + - * / %
	tkAt    // @ plus relative path, .tok carries the path
	tkLit   // literal, .val carries the parsed JSON value
	tkSize  // size / sizeof
	tkEmpty // empty
)

type token struct {
	kind tokenKind
	tok  string // operator text or @-relative path
	val  interface{}
}

var wordOps = map[string]tokenKind{
	"in":     tkCmp,
	"nin":    tkCmp,
	"size":   tkSize,
	"sizeof": tkSize,
	"empty":  tkEmpty,
}

func isWordStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isWordChar(c byte) bool {
	return isWordStart(c) || (c >= '0' && c <= '9') || c == '_' || c == '.'
}

func isArithChar(c byte) bool {
	return c == '+' || c == '-' || c == '*' || c == '/' || c == '%'
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
		case c == '+' || c == '-' || c == '*' || c == '/' || c == '%':
			toks = append(toks, token{kind: tkArith, tok: string(c)})
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
		case isWordStart(c):
			// Bare word: in/nin/size/sizeof/empty operator, or a JSON
			// literal (true/false/null).
			j := i
			for j < len(s) && isWordChar(s[j]) {
				j++
			}
			word := s[i:j]
			if kind, ok := wordOps[word]; ok {
				toks = append(toks, token{kind: kind, tok: word})
			} else {
				var v interface{}
				if err := json.Unmarshal([]byte(word), &v); err != nil {
					return nil, ErrJSONPathSyntax
				}
				toks = append(toks, token{kind: tkLit, val: v})
			}
			i = j
		default:
			// number / JSON composite literal
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
				} else if depth == 0 && isArithChar(ch) {
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

// ---- parser (precedence: || < && < ! < comparison < additive < multiplicative) ----

type predNode struct {
	op    string // "or","and","not", comparison, arithmetic, "" = operand-only
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

	lhs, err := p.parseArith()
	if err != nil {
		return nil, err
	}

	next := p.peek()
	switch {
	case next == nil:
		return lhs, nil // bare operand: truthiness
	case next.kind == tkCmp:
		p.pos++
		rhs, err := p.parseArith()
		if err != nil {
			return nil, err
		}
		return &predNode{op: next.tok, left: lhs, right: rhs}, nil
	case next.kind == tkSize:
		p.pos++
		sizeNode := &predNode{op: "size ==", left: lhs}
		if cmp := p.peek(); cmp != nil && cmp.kind == tkCmp {
			p.pos++
			rhs, err := p.parseArith()
			if err != nil {
				return nil, err
			}
			sizeNode.right = rhs
			sizeNode.op = "size " + cmp.tok
		} else {
			// `size n` defaults to ==; the comparand is mandatory.
			rhs, err := p.parseArith()
			if err != nil {
				return nil, err
			}
			sizeNode.right = rhs
		}
		return sizeNode, nil
	case next.kind == tkEmpty:
		p.pos++
		return &predNode{op: "empty", left: lhs}, nil
	default:
		return lhs, nil
	}
}

// parseArith: additive := multiplicative (('+'|'-') multiplicative)*
func (p *predParser) parseArith() (*predNode, error) {
	left, err := p.parseMul()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t == nil || t.kind != tkArith || (t.tok != "+" && t.tok != "-") {
			return left, nil
		}
		p.pos++
		right, err := p.parseMul()
		if err != nil {
			return nil, err
		}
		left = &predNode{op: t.tok, left: left, right: right}
	}
}

// parseMul: multiplicative := operand (('*'|'/'|'%') operand)*
func (p *predParser) parseMul() (*predNode, error) {
	left, err := p.parseOperand()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		if t == nil || t.kind != tkArith || (t.tok != "*" && t.tok != "/" && t.tok != "%") {
			return left, nil
		}
		p.pos++
		right, err := p.parseOperand()
		if err != nil {
			return nil, err
		}
		left = &predNode{op: t.tok, left: left, right: right}
	}
}

func (p *predParser) parseOperand() (*predNode, error) {
	t := p.peek()
	if t == nil {
		return nil, ErrJSONPathSyntax
	}
	switch t.kind {
	case tkAt, tkLit:
		p.pos++
		return &predNode{self: t}, nil
	case tkArith:
		if t.tok == "-" || t.tok == "+" {
			p.pos++
			inner, err := p.parseOperand()
			if err != nil {
				return nil, err
			}
			if t.tok == "-" {
				return &predNode{op: "u-", left: inner}, nil
			}
			return inner, nil
		}
	}
	return nil, ErrJSONPathSyntax
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
		return truthy(nodeValue(n, elem))
	case "in", "nin":
		l := nodeValue(n.left, elem)
		if l == nil {
			return false // missing operand matches neither in nor nin
		}
		arr, ok := nodeValue(n.right, elem).([]interface{})
		if !ok {
			return false
		}
		found := false
		for _, cand := range arr {
			if deepEqual(l, cand) {
				found = true
				break
			}
		}
		if n.op == "in" {
			return found
		}
		return !found
	case "empty":
		v := nodeValue(n.left, elem)
		length, ok := lengthOf(v)
		return ok && length == 0
	default:
		if strings.HasPrefix(n.op, "size") {
			if n.right == nil {
				return false // `size` without a comparand
			}
			length, ok := lengthOf(nodeValue(n.left, elem))
			if !ok {
				return false
			}
			res, err := compare(float64(length), nodeValue(n.right, elem),
				strings.TrimSpace(n.op[len("size"):]))
			return err == nil && res
		}
		l := nodeValue(n.left, elem)
		r := nodeValue(n.right, elem)
		ok, err := compare(l, r, n.op)
		return err == nil && ok
	}
}

// nodeValue evaluates a value node (operand, arithmetic, unary minus).
func nodeValue(n *predNode, elem interface{}) interface{} {
	if n.op == "" {
		if n.self.kind == tkLit {
			return n.self.val
		}
		return walkRelative(elem, n.self.tok)
	}
	if n.op == "u-" {
		f, ok := toFloat(nodeValue(n.left, elem))
		if !ok {
			return nil // Nothing
		}
		return -f
	}
	lf, lok := toFloat(nodeValue(n.left, elem))
	rf, rok := toFloat(nodeValue(n.right, elem))
	if !lok || !rok {
		return nil // Nothing: non-numeric operand
	}
	switch n.op {
	case "+":
		return lf + rf
	case "-":
		return lf - rf
	case "*":
		return lf * rf
	case "/":
		if rf == 0 {
			return nil
		}
		return lf / rf
	case "%":
		if rf == 0 {
			return nil
		}
		return math.Mod(lf, rf)
	}
	return nil
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

// lengthOf returns the length of a string/array/object (ok=false for other
// types, including nil).
func lengthOf(v interface{}) (int, bool) {
	switch t := v.(type) {
	case string:
		return len([]rune(t)), true
	case []interface{}:
		return len(t), true
	case map[string]interface{}:
		return len(t), true
	}
	return 0, false
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

// deepEqual compares JSON-decoded values (composites by marshal; scalars
// with number/string/bool/null equivalence).
func deepEqual(l, r interface{}) bool {
	if lf, ok := toFloat(l); ok {
		if rf, ok2 := toFloat(r); ok2 {
			return lf == rf
		}
		return false
	}
	if ls, ok1 := l.(string); ok1 {
		rs, ok2 := r.(string)
		return ok2 && ls == rs
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
