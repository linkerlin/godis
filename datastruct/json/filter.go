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
	"regexp"
	"sort"
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
	tkCmp    // == != < <= > >= in nin
	tkArith  // + - * / %
	tkAt     // @ plus relative path, .tok carries the path
	tkLit    // literal, .val carries the parsed JSON value
	tkSize   // size / sizeof
	tkEmpty  // empty
	tkFirst  // first (postfix value transform)
	tkLast   // last (postfix value transform)
	tkFnWord // concat / match / search (postfix, one required argument)
	tkFnCall // prefix function call fn(...)
)

type token struct {
	kind tokenKind
	tok  string // operator text or @-relative path
	val  interface{}
}

var wordOps = map[string]tokenKind{
	"in":       tkCmp,
	"nin":      tkCmp,
	"size":     tkSize,
	"sizeof":   tkSize,
	"empty":    tkEmpty,
	"first":    tkFirst,
	"last":     tkLast,
	"concat":   tkFnWord,
	"match":    tkFnWord,
	"search":   tkFnWord,
	"subsetof": tkFnWord,
	"anyof":    tkFnWord,
	"noneof":   tkFnWord,
	"append":   tkFnWord,
	"index":    tkFnWord,
}

var prefixFnNames = map[string]bool{
	"length":  true,
	"abs":     true,
	"ceiling": true,
	"floor":   true,
	"first":   true,
	"last":    true,
	"min":     true,
	"max":     true,
	"avg":     true,
	"sum":     true,
	"stddev":  true,
	"keys":    true,
	"count":   true,
	"value":   true,
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
				if ch == '.' || ch == '[' || ch == ']' || ch == '~' ||
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
			// Bare word: prefix function call fn(...), in/nin/size/sizeof/empty
			// operator, or a JSON literal (true/false/null).
			j := i
			for j < len(s) && isWordChar(s[j]) {
				j++
			}
			word := s[i:j]
			k := j
			for k < len(s) && (s[k] == ' ' || s[k] == '\t') {
				k++
			}
			if k < len(s) && s[k] == '(' && prefixFnNames[word] {
				toks = append(toks, token{kind: tkFnCall, tok: word})
			} else if kind, ok := wordOps[word]; ok {
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
				if ch == '\'' || ch == '"' {
					// Quoted string inside a composite literal: skip whole.
					q := ch
					j++
					for j < len(s) && s[j] != q {
						j++
					}
					if j >= len(s) {
						return nil, ErrJSONPathSyntax
					}
				} else if ch == '{' || ch == '[' {
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
		saved := p.pos
		p.pos++
		if inner, err := p.parseOr(); err == nil {
			if closing := p.peek(); closing != nil && closing.kind == tkRParen {
				p.pos++
				next := p.peek()
				if next == nil || next.kind == tkAnd || next.kind == tkOr || next.kind == tkRParen {
					return inner, nil
				}
			}
		}
		p.pos = saved
	}

	lhs, err := p.parseArith()
	if err != nil {
		return nil, err
	}

	for {
		next := p.peek()
		switch {
		case next == nil:
			return lhs, nil // bare operand: truthiness
		case next.kind == tkFirst, next.kind == tkLast:
			// Postfix value transform (`@.colors first == "black"`); the
			// comparison (if any) applies to the transformed value.
			p.pos++
			lhs = &predNode{op: "fn:" + next.tok, left: lhs}
		case next.kind == tkFnWord:
			// Postfix call with one argument: `@.id concat ":" == "bike:1:"`.
			p.pos++
			arg, err := p.parseArith()
			if err != nil {
				return nil, err
			}
			lhs = &predNode{op: "fn:" + next.tok, left: lhs, right: arg}
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
	case tkLParen:
		p.pos++
		inner, err := p.parseArith()
		if err != nil {
			return nil, err
		}
		closing := p.peek()
		if closing == nil || closing.kind != tkRParen {
			return nil, ErrJSONPathSyntax
		}
		p.pos++
		return inner, nil
	case tkFnCall:
		fn := t.tok
		p.pos++
		open := p.peek()
		if open == nil || open.kind != tkLParen {
			return nil, ErrJSONPathSyntax
		}
		p.pos++
		arg, err := p.parseArith()
		if err != nil {
			return nil, err
		}
		closing := p.peek()
		if closing == nil || closing.kind != tkRParen {
			return nil, ErrJSONPathSyntax
		}
		p.pos++
		return &predNode{
			op:   "fn:" + fn,
			left: arg,
		}, nil
	case tkAt:
		p.pos++
		// Method call form: `@.recv.fn()` or `@.recv.fn(arg)` — the
		// function name is the last dot-segment of the @-path.
		if call := p.peek(); call != nil && call.kind == tkLParen {
			dot := strings.LastIndex(t.tok, ".")
			if dot <= 0 {
				return nil, ErrJSONPathSyntax
			}
			fn, recv := t.tok[dot+1:], t.tok[:dot]
			p.pos++ // consume '('
			var arg *predNode
			if argTok := p.peek(); argTok != nil && argTok.kind != tkRParen {
				var err error
				arg, err = p.parseArith()
				if err != nil {
					return nil, err
				}
			}
			closing := p.peek()
			if closing == nil || closing.kind != tkRParen {
				return nil, ErrJSONPathSyntax
			}
			p.pos++
			return &predNode{
				op:    "fn:" + fn,
				left:  &predNode{self: &token{kind: tkAt, tok: recv}},
				right: arg,
			}, nil
		}
		return &predNode{self: t}, nil
	case tkLit:
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
		// A bare function node (e.g. `@.name.match("x")` as the whole
		// predicate) evaluates to the function result's truthiness.
		if strings.HasPrefix(n.op, "fn:") {
			return truthy(nodeValue(n, elem))
		}
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
	if fn, ok := strings.CutPrefix(n.op, "fn:"); ok {
		return applyFunction(fn, nodeValue(n.left, elem), n.right, elem)
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

// applyFunction evaluates one JSONPath function over recv (8.10 batch 3):
// length/abs/ceiling/floor on number/string/array/object; first/last on
// arrays (postfix word form routes here too); min/max/avg/sum/stddev on
// arrays of numbers; keys on objects; match/search (regexp, anchored vs
// partial) and concat on strings. Unknown or mistyped calls yield Nothing.
func applyFunction(fn string, recv interface{}, argNode *predNode, elem interface{}) interface{} {
	numsOf := func(v interface{}) ([]float64, bool) {
		arr, ok := v.([]interface{})
		if !ok {
			return nil, false
		}
		nums := make([]float64, 0, len(arr))
		for _, e := range arr {
			f, ok := toFloat(e)
			if !ok {
				return nil, false
			}
			nums = append(nums, f)
		}
		return nums, true
	}

	switch fn {
	case "length":
		if n, ok := lengthOf(recv); ok {
			return float64(n)
		}
		return nil
	case "abs", "ceiling", "floor":
		f, ok := toFloat(recv)
		if !ok {
			return nil
		}
		switch fn {
		case "abs":
			return math.Abs(f)
		case "ceiling":
			return math.Ceil(f)
		default:
			return math.Floor(f)
		}
	case "first", "last":
		arr, ok := recv.([]interface{})
		if !ok || len(arr) == 0 {
			return nil
		}
		if fn == "first" {
			return arr[0]
		}
		return arr[len(arr)-1]
	case "min", "max", "avg", "sum", "stddev":
		nums, ok := numsOf(recv)
		if !ok || len(nums) == 0 {
			return nil
		}
		switch fn {
		case "min":
			m := nums[0]
			for _, x := range nums[1:] {
				if x < m {
					m = x
				}
			}
			return m
		case "max":
			m := nums[0]
			for _, x := range nums[1:] {
				if x > m {
					m = x
				}
			}
			return m
		case "sum":
			s := 0.0
			for _, x := range nums {
				s += x
			}
			return s
		case "avg":
			s := 0.0
			for _, x := range nums {
				s += x
			}
			return s / float64(len(nums))
		default: // stddev (sample)
			m := 0.0
			for _, x := range nums {
				m += x
			}
			m /= float64(len(nums))
			var sq float64
			for _, x := range nums {
				sq += (x - m) * (x - m)
			}
			if len(nums) < 2 {
				return nil
			}
			return math.Sqrt(sq / float64(len(nums)-1))
		}
	case "keys":
		obj, ok := recv.(map[string]interface{})
		if !ok {
			return nil
		}
		keys := make([]interface{}, 0, len(obj))
		for k := range obj {
			keys = append(keys, k)
		}
		sortStrings(keys)
		return keys
	case "match", "search":
		s, ok := recv.(string)
		if !ok || argNode == nil {
			return nil
		}
		pattern, ok := nodeValue(argNode, elem).(string)
		if !ok {
			return nil
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil
		}
		if fn == "match" {
			// Whole-string (anchored) match.
			anchored, aerr := regexp.Compile(`^(?:` + pattern + `)$`)
			if aerr != nil {
				return nil
			}
			return anchored.MatchString(s)
		}
		return re.MatchString(s)
	case "concat":
		s, ok := recv.(string)
		if !ok || argNode == nil {
			return nil
		}
		arg, ok := nodeValue(argNode, elem).(string)
		if !ok {
			return nil
		}
		return s + arg
	case "subsetof", "anyof", "noneof":
		// Set relations on array/nodelist (8.10): subset = every element
		// present; anyof = at least one shared; noneof = no shared element.
		arr, ok := recv.([]interface{})
		if !ok || argNode == nil {
			return nil
		}
		set, ok := nodeValue(argNode, elem).([]interface{})
		if !ok {
			return nil
		}
		contains := func(v interface{}) bool {
			for _, s := range set {
				if deepEqual(v, s) {
					return true
				}
			}
			return false
		}
		matched := 0
		for _, v := range arr {
			if contains(v) {
				matched++
			}
		}
		switch fn {
		case "subsetof":
			return matched == len(arr)
		case "anyof":
			return matched > 0
		default: // noneof
			return matched == 0
		}
	case "index":
		// Array access by position; out-of-range / non-numeric → Nothing.
		arr, ok := recv.([]interface{})
		if !ok || argNode == nil {
			return nil
		}
		idx, ok := toFloat(nodeValue(argNode, elem))
		if !ok || idx < 0 || idx >= float64(len(arr)) {
			return nil
		}
		return arr[int(idx)]
	case "append":
		// Query-time projection: recv array + one appended value
		// (read-only; the stored document is untouched).
		arr, ok := recv.([]interface{})
		if !ok || argNode == nil {
			return nil
		}
		out := append(append([]interface{}(nil), arr...), nodeValue(argNode, elem))
		return out
	case "count":
		arr, ok := recv.([]interface{})
		if !ok {
			return nil
		}
		return float64(len(arr))
	case "value":
		return recv
	}
	return nil
}

func sortStrings(keys []interface{}) {
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].(string) < keys[j].(string)
	})
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
			if part == "~" {
				keys := make([]interface{}, 0, len(c))
				for k := range c {
					keys = append(keys, k)
				}
				sortStrings(keys)
				current = keys
				continue
			}
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
