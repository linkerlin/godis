package redisearch

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ApplyClause represents a minimal FT.AGGREGATE `APPLY <expr> AS <name>`
// clause. PreGroup records whether the clause appeared before the (single)
// GROUPBY stage in the original command, which decides whether it runs
// against per-document rows or against the post-GROUPBY result rows.
type ApplyClause struct {
	Expr     string
	As       string
	PreGroup bool
}

// EvalApplyExpr evaluates an FT.AGGREGATE expression (used by both APPLY and
// FILTER) against a row's field map. The grammar is a superset of Redis's
// aggregation expression language:
//
//	@field refs, numeric/string literals, parentheses
//	arithmetic: + - * / % ^ (power, right-assoc)
//	comparison: == != < <= > >=   (return bool)
//	logical:    && || !           (return bool)
//	functions:  log log2 exp sqrt abs ceil floor
//	             upper lower strlen startswith contains substr format split
//	             exists case
//
// `+` falls back to string concatenation when either operand is non-numeric.
func EvalApplyExpr(expr string, fields map[string]interface{}) (interface{}, error) {
	tokens, err := applyTokenize(strings.TrimSpace(expr))
	if err != nil {
		return nil, err
	}
	p := &applyParser{tokens: tokens, fields: fields}
	v, err := p.parseBoolOr()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != applyTokEOF {
		return nil, errors.New("unexpected trailing tokens in expression")
	}
	return applyValueToInterface(v), nil
}

// EvalFilterExpr evaluates an expression and reports its boolean truth value.
// Non-bool results are coerced: numbers are true when nonzero, strings when
// non-empty, nil otherwise. Used by FT.AGGREGATE FILTER.
func EvalFilterExpr(expr string, fields map[string]interface{}) (bool, error) {
	v, err := EvalApplyExpr(expr, fields)
	if err != nil {
		return false, err
	}
	switch x := v.(type) {
	case bool:
		return x, nil
	case float64:
		return x != 0, nil
	case string:
		return x != "", nil
	case nil:
		return false, nil
	default:
		return true, nil
	}
}

// passthroughGroups turns each matching document into its own result row.
func passthroughGroups(docs []*Document) []*Group {
	groups := make([]*Group, 0, len(docs))
	for _, doc := range docs {
		fields := make(map[string]interface{}, len(doc.Fields))
		for k, v := range doc.Fields {
			fields[k] = v
		}
		groups = append(groups, &Group{Fields: fields})
	}
	return groups
}

// applyPreGroupClauses evaluates APPLY clauses that appeared before GROUPBY
// against each document's own fields.
func applyPreGroupClauses(docs []*Document, applies []ApplyClause) []*Document {
	if len(applies) == 0 {
		return docs
	}
	out := make([]*Document, len(docs))
	for i, doc := range docs {
		fields := make(map[string]interface{}, len(doc.Fields)+len(applies))
		for k, v := range doc.Fields {
			fields[k] = v
		}
		for _, ac := range applies {
			if val, err := EvalApplyExpr(ac.Expr, fields); err == nil {
				fields[ac.As] = val
			}
		}
		out[i] = &Document{ID: doc.ID, Fields: fields, Score: doc.Score, Payload: doc.Payload}
	}
	return out
}

// applyPostGroupClauses evaluates APPLY clauses that appeared after GROUPBY
// against each result row (group), adding the computed field in place.
func applyPostGroupClauses(groups []*Group, applies []ApplyClause) {
	for _, g := range groups {
		for _, ac := range applies {
			if val, err := EvalApplyExpr(ac.Expr, g.Fields); err == nil {
				g.Fields[ac.As] = val
			}
		}
	}
}

// ---- token types ----

type applyTokenKind int

const (
	applyTokEOF applyTokenKind = iota
	applyTokNumber
	applyTokString
	applyTokIdent  // function name or @field-stripped identifier
	applyTokField  // @field reference
	applyTokPlus
	applyTokMinus
	applyTokStar
	applyTokSlash
	applyTokPercent
	applyTokCaret
	applyTokLParen
	applyTokRParen
	applyTokComma
	applyTokEq    // ==
	applyTokNe    // !=
	applyTokLt    // <
	applyTokLe    // <=
	applyTokGt    // >
	applyTokGe    // >=
	applyTokAnd   // &&
	applyTokOr    // ||
	applyTokNot   // !
	applyTokAssign // = (single, tolerated as ==)
)

type applyToken struct {
	kind applyTokenKind
	text string
}

func applyTokenize(expr string) ([]applyToken, error) {
	var tokens []applyToken
	i, n := 0, len(expr)
	for i < n {
		c := expr[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '+':
			tokens = append(tokens, applyToken{kind: applyTokPlus})
			i++
		case c == '-':
			tokens = append(tokens, applyToken{kind: applyTokMinus})
			i++
		case c == '*' && i+1 < n && expr[i+1] == '*':
			// ** as an alias for power
			tokens = append(tokens, applyToken{kind: applyTokCaret})
			i += 2
		case c == '*':
			tokens = append(tokens, applyToken{kind: applyTokStar})
			i++
		case c == '/':
			tokens = append(tokens, applyToken{kind: applyTokSlash})
			i++
		case c == '%':
			tokens = append(tokens, applyToken{kind: applyTokPercent})
			i++
		case c == '^':
			tokens = append(tokens, applyToken{kind: applyTokCaret})
			i++
		case c == '(':
			tokens = append(tokens, applyToken{kind: applyTokLParen})
			i++
		case c == ')':
			tokens = append(tokens, applyToken{kind: applyTokRParen})
			i++
		case c == ',':
			tokens = append(tokens, applyToken{kind: applyTokComma})
			i++
		case c == '=' && i+1 < n && expr[i+1] == '=':
			tokens = append(tokens, applyToken{kind: applyTokEq})
			i += 2
		case c == '=':
			tokens = append(tokens, applyToken{kind: applyTokAssign})
			i++
		case c == '!' && i+1 < n && expr[i+1] == '=':
			tokens = append(tokens, applyToken{kind: applyTokNe})
			i += 2
		case c == '!':
			tokens = append(tokens, applyToken{kind: applyTokNot})
			i++
		case c == '<' && i+1 < n && expr[i+1] == '=':
			tokens = append(tokens, applyToken{kind: applyTokLe})
			i += 2
		case c == '<':
			tokens = append(tokens, applyToken{kind: applyTokLt})
			i++
		case c == '>' && i+1 < n && expr[i+1] == '=':
			tokens = append(tokens, applyToken{kind: applyTokGe})
			i += 2
		case c == '>':
			tokens = append(tokens, applyToken{kind: applyTokGt})
			i++
		case c == '&' && i+1 < n && expr[i+1] == '&':
			tokens = append(tokens, applyToken{kind: applyTokAnd})
			i += 2
		case c == '|' && i+1 < n && expr[i+1] == '|':
			tokens = append(tokens, applyToken{kind: applyTokOr})
			i += 2
		case c == '"':
			j := i + 1
			for j < n && expr[j] != '"' {
				j++
			}
			if j >= n {
				return nil, errors.New("unterminated string literal in expression")
			}
			tokens = append(tokens, applyToken{kind: applyTokString, text: expr[i+1 : j]})
			i = j + 1
		case c == '\'':
			// Single-quoted string literal (Redis accepts both ' and ").
			j := i + 1
			for j < n && expr[j] != '\'' {
				j++
			}
			if j >= n {
				return nil, errors.New("unterminated string literal in expression")
			}
			tokens = append(tokens, applyToken{kind: applyTokString, text: expr[i+1 : j]})
			i = j + 1
		case c == '@':
			j := i + 1
			for j < n && isApplyIdentChar(expr[j]) {
				j++
			}
			if j == i+1 {
				return nil, errors.New("invalid field reference in expression")
			}
			tokens = append(tokens, applyToken{kind: applyTokField, text: expr[i+1 : j]})
			i = j
		case c >= '0' && c <= '9' || c == '.':
			j := i + 1
			for j < n && ((expr[j] >= '0' && expr[j] <= '9') || expr[j] == '.') {
				j++
			}
			tokens = append(tokens, applyToken{kind: applyTokNumber, text: expr[i:j]})
			i = j
		case isApplyIdentStart(c):
			j := i + 1
			for j < n && isApplyIdentChar(expr[j]) {
				j++
			}
			tokens = append(tokens, applyToken{kind: applyTokIdent, text: expr[i:j]})
			i = j
		default:
			return nil, fmt.Errorf("unexpected character %q in expression", c)
		}
	}
	tokens = append(tokens, applyToken{kind: applyTokEOF})
	return tokens, nil
}

func isApplyIdentStart(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isApplyIdentChar(c byte) bool {
	return isApplyIdentStart(c) || (c >= '0' && c <= '9')
}

// ---- value ----

// applyValue is a tagged union of numeric, string, or bool intermediate result.
type applyValue struct {
	isNum bool
	isStr bool
	isBool bool
	num   float64
	str   string
	b     bool
}

type applyParser struct {
	tokens []applyToken
	pos    int
	fields map[string]interface{}
}

func (p *applyParser) peek() applyToken { return p.tokens[p.pos] }

func (p *applyParser) next() applyToken {
	t := p.tokens[p.pos]
	p.pos++
	return t
}

// parseBoolOr := parseBoolAnd ('||' parseBoolAnd)*
func (p *applyParser) parseBoolOr() (applyValue, error) {
	left, err := p.parseBoolAnd()
	if err != nil {
		return applyValue{}, err
	}
	for p.peek().kind == applyTokOr {
		p.next()
		right, err := p.parseBoolAnd()
		if err != nil {
			return applyValue{}, err
		}
		lb, _ := applyTruthy(left)
		rb, _ := applyTruthy(right)
		left = applyBoolValue(lb || rb)
	}
	return left, nil
}

// parseBoolAnd := parseComparison ('&&' parseComparison)*
func (p *applyParser) parseBoolAnd() (applyValue, error) {
	left, err := p.parseComparison()
	if err != nil {
		return applyValue{}, err
	}
	for p.peek().kind == applyTokAnd {
		p.next()
		right, err := p.parseComparison()
		if err != nil {
			return applyValue{}, err
		}
		lb, _ := applyTruthy(left)
		rb, _ := applyTruthy(right)
		left = applyBoolValue(lb && rb)
	}
	return left, nil
}

// parseComparison := parseAddSub ((== | != | < | <= | > | >=) parseAddSub)*
func (p *applyParser) parseComparison() (applyValue, error) {
	left, err := p.parseAddSub()
	if err != nil {
		return applyValue{}, err
	}
	for {
		k := p.peek().kind
		if k != applyTokEq && k != applyTokAssign && k != applyTokNe && k != applyTokLt && k != applyTokLe && k != applyTokGt && k != applyTokGe {
			break
		}
		p.next()
		right, err := p.parseAddSub()
		if err != nil {
			return applyValue{}, err
		}
		res, err := applyCompare(left, k, right)
		if err != nil {
			return applyValue{}, err
		}
		left = applyBoolValue(res)
	}
	return left, nil
}

// parseAddSub := parseMulDiv (('+' | '-') parseMulDiv)*
func (p *applyParser) parseAddSub() (applyValue, error) {
	left, err := p.parseMulDiv()
	if err != nil {
		return applyValue{}, err
	}
	for p.peek().kind == applyTokPlus || p.peek().kind == applyTokMinus {
		op := p.next().kind
		right, err := p.parseMulDiv()
		if err != nil {
			return applyValue{}, err
		}
		left, err = applyBinaryArith(left, op, right)
		if err != nil {
			return applyValue{}, err
		}
	}
	return left, nil
}

// parseMulDiv := parsePower (('*' | '/' | '%') parsePower)*
func (p *applyParser) parseMulDiv() (applyValue, error) {
	left, err := p.parsePower()
	if err != nil {
		return applyValue{}, err
	}
	for p.peek().kind == applyTokStar || p.peek().kind == applyTokSlash || p.peek().kind == applyTokPercent {
		op := p.next().kind
		right, err := p.parsePower()
		if err != nil {
			return applyValue{}, err
		}
		left, err = applyBinaryArith(left, op, right)
		if err != nil {
			return applyValue{}, err
		}
	}
	return left, nil
}

// parsePower := parseUnary ('^' parsePower)?   (right-associative)
func (p *applyParser) parsePower() (applyValue, error) {
	base, err := p.parseUnary()
	if err != nil {
		return applyValue{}, err
	}
	if p.peek().kind == applyTokCaret {
		p.next()
		exp, err := p.parsePower()
		if err != nil {
			return applyValue{}, err
		}
		if !base.isNum || !exp.isNum {
			return applyValue{}, errors.New("^ requires numeric operands")
		}
		return applyValue{isNum: true, num: math.Pow(base.num, exp.num)}, nil
	}
	return base, nil
}

// parseUnary := ('-' | '!') parseUnary | parsePrimary
func (p *applyParser) parseUnary() (applyValue, error) {
	if p.peek().kind == applyTokMinus {
		p.next()
		v, err := p.parseUnary()
		if err != nil {
			return applyValue{}, err
		}
		if !v.isNum {
			return applyValue{}, errors.New("unary minus on non-numeric value")
		}
		return applyValue{isNum: true, num: -v.num}, nil
	}
	if p.peek().kind == applyTokNot {
		p.next()
		v, err := p.parseUnary()
		if err != nil {
			return applyValue{}, err
		}
		tb, _ := applyTruthy(v)
		return applyBoolValue(!tb), nil
	}
	return p.parsePrimary()
}

// parsePrimary := NUMBER | STRING | FIELD | '(' boolOr ')' | func '(' args ')' | ident
func (p *applyParser) parsePrimary() (applyValue, error) {
	tok := p.next()
	switch tok.kind {
	case applyTokNumber:
		f, err := strconv.ParseFloat(tok.text, 64)
		if err != nil {
			return applyValue{}, err
		}
		return applyValue{isNum: true, num: f}, nil
	case applyTokString:
		return applyValue{isStr: true, str: tok.text}, nil
	case applyTokField:
		raw, ok := p.fields[tok.text]
		if !ok {
			return applyValue{isNum: true, num: 0}, nil
		}
		return applyValueFromInterface(raw), nil
	case applyTokLParen:
		v, err := p.parseBoolOr()
		if err != nil {
			return applyValue{}, err
		}
		if p.peek().kind != applyTokRParen {
			return applyValue{}, errors.New("expected ')' in expression")
		}
		p.next()
		return v, nil
	case applyTokIdent:
		// Identifier: true/false keywords, a function call (ident followed by
		// '('), or — when bare — a string literal value (Redis FILTER treats
		// unquoted alphanumerics like @cat == active as the string "active").
		name := strings.ToLower(tok.text)
		if name == "true" {
			return applyBoolValue(true), nil
		}
		if name == "false" {
			return applyBoolValue(false), nil
		}
		if p.peek().kind != applyTokLParen {
			// Bare identifier not used as a function: treat as a string literal
			// so comparisons like "@status == active" work without quotes.
			return applyValue{isStr: true, str: tok.text}, nil
		}
		p.next() // consume '('
		var args []applyValue
		if p.peek().kind != applyTokRParen {
			for {
				a, err := p.parseBoolOr()
				if err != nil {
					return applyValue{}, err
				}
				args = append(args, a)
				if p.peek().kind == applyTokComma {
					p.next()
					continue
				}
				break
			}
		}
		if p.peek().kind != applyTokRParen {
			return applyValue{}, fmt.Errorf("expected ')' to close function %q", tok.text)
		}
		p.next()
		return applyFunction(name, args)
	default:
		return applyValue{}, errors.New("unexpected token in expression")
	}
}

// applyValueFromInterface converts a stored field value into an applyValue.
func applyValueFromInterface(raw interface{}) applyValue {
	switch v := raw.(type) {
	case bool:
		return applyBoolValue(v)
	case float64:
		return applyValue{isNum: true, num: v}
	case float32:
		return applyValue{isNum: true, num: float64(v)}
	case int:
		return applyValue{isNum: true, num: float64(v)}
	case int64:
		return applyValue{isNum: true, num: float64(v)}
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return applyValue{isNum: true, num: f}
		}
		return applyValue{isStr: true, str: v}
	default:
		s := fmt.Sprintf("%v", v)
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return applyValue{isNum: true, num: f}
		}
		return applyValue{isStr: true, str: s}
	}
}

func applyBoolValue(b bool) applyValue { return applyValue{isBool: true, b: b} }

// applyValueToInterface unwraps an applyValue for return to callers.
func applyValueToInterface(v applyValue) interface{} {
	if v.isBool {
		return v.b
	}
	if v.isNum {
		return v.num
	}
	if v.isStr {
		return v.str
	}
	return nil
}

// applyTruthy coerces a value to bool (FILTER semantics).
func applyTruthy(v applyValue) (bool, error) {
	if v.isBool {
		return v.b, nil
	}
	if v.isNum {
		return v.num != 0, nil
	}
	if v.isStr {
		return v.str != "", nil
	}
	return false, nil
}

func applyBinaryArith(left applyValue, op applyTokenKind, right applyValue) (applyValue, error) {
	if op == applyTokPlus && (!left.isNum || !right.isNum) {
		// String concatenation fallback when either side is non-numeric.
		return applyValue{isStr: true, str: applyValueToString(left) + applyValueToString(right)}, nil
	}
	if !left.isNum || !right.isNum {
		return applyValue{}, errors.New("non-numeric operand for arithmetic operator")
	}
	switch op {
	case applyTokPlus:
		return applyValue{isNum: true, num: left.num + right.num}, nil
	case applyTokMinus:
		return applyValue{isNum: true, num: left.num - right.num}, nil
	case applyTokStar:
		return applyValue{isNum: true, num: left.num * right.num}, nil
	case applyTokSlash:
		if right.num == 0 {
			return applyValue{}, errors.New("division by zero")
		}
		return applyValue{isNum: true, num: left.num / right.num}, nil
	case applyTokPercent:
		if right.num == 0 {
			return applyValue{}, errors.New("modulo by zero")
		}
		return applyValue{isNum: true, num: math.Mod(left.num, right.num)}, nil
	default:
		return applyValue{}, errors.New("unsupported arithmetic operator")
	}
}

// applyCompare evaluates a comparison operator. Numeric vs numeric compares by
// value; otherwise both sides stringify and compare lexicographically.
func applyCompare(left applyValue, op applyTokenKind, right applyValue) (bool, error) {
	if left.isNum && right.isNum {
		switch op {
		case applyTokEq, applyTokAssign:
			return left.num == right.num, nil
		case applyTokNe:
			return left.num != right.num, nil
		case applyTokLt:
			return left.num < right.num, nil
		case applyTokLe:
			return left.num <= right.num, nil
		case applyTokGt:
			return left.num > right.num, nil
		case applyTokGe:
			return left.num >= right.num, nil
		}
	}
	ls := applyValueToString(left)
	rs := applyValueToString(right)
	switch op {
	case applyTokEq, applyTokAssign:
		return ls == rs, nil
	case applyTokNe:
		return ls != rs, nil
	case applyTokLt:
		return ls < rs, nil
	case applyTokLe:
		return ls <= rs, nil
	case applyTokGt:
		return ls > rs, nil
	case applyTokGe:
		return ls >= rs, nil
	}
	return false, errors.New("unsupported comparison operator")
}

func applyValueToString(v applyValue) string {
	if v.isBool {
		if v.b {
			return "true"
		}
		return "false"
	}
	if v.isNum {
		return strconv.FormatFloat(v.num, 'f', -1, 64)
	}
	return v.str
}

// applyFunction dispatches a built-in function call.
func applyFunction(name string, args []applyValue) (applyValue, error) {
	switch name {
	// Numeric functions.
	case "log":
		if len(args) != 1 || !args[0].isNum {
			return applyValue{}, errors.New("log() expects one numeric argument")
		}
		return applyValue{isNum: true, num: math.Log(args[0].num)}, nil
	case "log2":
		if len(args) != 1 || !args[0].isNum {
			return applyValue{}, errors.New("log2() expects one numeric argument")
		}
		return applyValue{isNum: true, num: math.Log2(args[0].num)}, nil
	case "exp":
		if len(args) != 1 || !args[0].isNum {
			return applyValue{}, errors.New("exp() expects one numeric argument")
		}
		return applyValue{isNum: true, num: math.Exp(args[0].num)}, nil
	case "sqrt":
		if len(args) != 1 || !args[0].isNum {
			return applyValue{}, errors.New("sqrt() expects one numeric argument")
		}
		return applyValue{isNum: true, num: math.Sqrt(args[0].num)}, nil
	case "abs":
		if len(args) != 1 || !args[0].isNum {
			return applyValue{}, errors.New("abs() expects one numeric argument")
		}
		return applyValue{isNum: true, num: math.Abs(args[0].num)}, nil
	case "ceil":
		if len(args) != 1 || !args[0].isNum {
			return applyValue{}, errors.New("ceil() expects one numeric argument")
		}
		return applyValue{isNum: true, num: math.Ceil(args[0].num)}, nil
	case "floor":
		if len(args) != 1 || !args[0].isNum {
			return applyValue{}, errors.New("floor() expects one numeric argument")
		}
		return applyValue{isNum: true, num: math.Floor(args[0].num)}, nil
	// String functions.
	case "upper":
		if len(args) != 1 {
			return applyValue{}, errors.New("upper() expects one argument")
		}
		return applyValue{isStr: true, str: strings.ToUpper(applyValueToString(args[0]))}, nil
	case "lower":
		if len(args) != 1 {
			return applyValue{}, errors.New("lower() expects one argument")
		}
		return applyValue{isStr: true, str: strings.ToLower(applyValueToString(args[0]))}, nil
	case "strlen":
		if len(args) != 1 {
			return applyValue{}, errors.New("strlen() expects one argument")
		}
		return applyValue{isNum: true, num: float64(len(applyValueToString(args[0])))}, nil
	case "startswith":
		if len(args) != 2 {
			return applyValue{}, errors.New("startswith() expects two arguments")
		}
		s := applyValueToString(args[0])
		prefix := applyValueToString(args[1])
		return applyBoolValue(strings.HasPrefix(s, prefix)), nil
	case "contains":
		if len(args) != 2 {
			return applyValue{}, errors.New("contains() expects two arguments")
		}
		s := applyValueToString(args[0])
		sub := applyValueToString(args[1])
		if sub == "" {
			return applyValue{isNum: true, num: float64(len(s) + 1)}, nil
		}
		return applyValue{isNum: true, num: float64(strings.Count(s, sub))}, nil
	case "substr":
		if len(args) != 3 {
			return applyValue{}, errors.New("substr() expects three arguments")
		}
		if !args[1].isNum || !args[2].isNum {
			return applyValue{}, errors.New("substr() offset/count must be numeric")
		}
		s := applyValueToString(args[0])
		offset := int(args[1].num)
		count := int(args[2].num)
		return applyValue{isStr: true, str: applySubstr(s, offset, count)}, nil
	case "format":
		if len(args) < 1 {
			return applyValue{}, errors.New("format() expects a format string")
		}
		fmtStr := applyValueToString(args[0])
		rest := args[1:]
		// Redis format only supports %s. Replace each %s with the next argument.
		out := applyFormat(fmtStr, rest)
		return applyValue{isStr: true, str: out}, nil
	case "split":
		if len(args) < 1 {
			return applyValue{}, errors.New("split() expects a string argument")
		}
		// Returns the first token (single-value approximation). Redis returns an
		// array; our APPLY model is scalar, so the first token is returned.
		// ponytail: full array support awaits multi-value pipeline rows.
		s := applyValueToString(args[0])
		sep := ","
		if len(args) >= 2 {
			sep = applyValueToString(args[1])
		}
		parts := strings.Split(s, sep)
		if len(parts) > 0 {
			return applyValue{isStr: true, str: strings.TrimSpace(parts[0])}, nil
		}
		return applyValue{isStr: true, str: ""}, nil
	// Boolean / control.
	case "exists":
		if len(args) != 1 {
			return applyValue{}, errors.New("exists() expects one argument")
		}
		// exists() of a field reference: a missing field parses as numeric 0, so
		// we treat "0 from a literal" the same as absent — close enough for the
		// common "@field" case where absence surfaces as 0.
		if args[0].isNum && args[0].num == 0 {
			return applyBoolValue(false), nil
		}
		return applyBoolValue(true), nil
	case "case":
		if len(args) != 3 {
			return applyValue{}, errors.New("case() expects (cond, ifTrue, ifFalse)")
		}
		cond, _ := applyTruthy(args[0])
		if cond {
			return args[1], nil
		}
		return args[2], nil
	}
	return applyValue{}, fmt.Errorf("unknown function %q", name)
}

// applySubstr implements substr(s, offset, count) with Redis semantics: a
// negative offset counts from the end; count == -1 means "to the end".
func applySubstr(s string, offset, count int) string {
	runes := []rune(s)
	if offset < 0 {
		offset = len(runes) + offset
		if offset < 0 {
			offset = 0
		}
	}
	if offset > len(runes) {
		offset = len(runes)
	}
	end := offset + count
	if count < 0 || end > len(runes) {
		end = len(runes)
	}
	if end < offset {
		end = offset
	}
	return string(runes[offset:end])
}

// applyFormat replaces each %s in fmtStr with successive args (Redis only
// supports %s in format()). A literal %% becomes %.
func applyFormat(fmtStr string, args []applyValue) string {
	var b strings.Builder
	ai := 0
	for i := 0; i < len(fmtStr); i++ {
		if fmtStr[i] == '%' && i+1 < len(fmtStr) {
			if fmtStr[i+1] == '%' {
				b.WriteByte('%')
				i++
				continue
			}
			if fmtStr[i+1] == 's' {
				if ai < len(args) {
					b.WriteString(applyValueToString(args[ai]))
					ai++
				}
				i++
				continue
			}
		}
		b.WriteByte(fmtStr[i])
	}
	return b.String()
}
