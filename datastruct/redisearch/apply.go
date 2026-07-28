package redisearch

import (
	"errors"
	"fmt"
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

// EvalApplyExpr evaluates a minimal APPLY expression against a row's field
// map. Supported syntax: @field references, numeric literals, the binary
// operators + - * / with standard precedence (* and / bind tighter), unary
// minus, and parentheses. When either operand of `+` is non-numeric the
// operator falls back to string concatenation.
func EvalApplyExpr(expr string, fields map[string]interface{}) (interface{}, error) {
	tokens, err := applyTokenize(strings.TrimSpace(expr))
	if err != nil {
		return nil, err
	}
	p := &applyParser{tokens: tokens, fields: fields}
	v, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != applyTokEOF {
		return nil, errors.New("unexpected trailing tokens in APPLY expression")
	}
	if v.isNum {
		return v.num, nil
	}
	return v.str, nil
}

// passthroughGroups turns each matching document into its own result row.
// Used by Aggregate when neither GROUPBY nor REDUCE is present, matching
// RediSearch's behavior of returning one row per document in that case.
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
// against each document's own fields, returning new documents carrying the
// computed fields so that a following GROUPBY/REDUCE can reference them.
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

type applyTokenKind int

const (
	applyTokNumber applyTokenKind = iota
	applyTokField
	applyTokPlus
	applyTokMinus
	applyTokStar
	applyTokSlash
	applyTokLParen
	applyTokRParen
	applyTokEOF
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
		case c == ' ' || c == '\t':
			i++
		case c == '+':
			tokens = append(tokens, applyToken{kind: applyTokPlus})
			i++
		case c == '-':
			tokens = append(tokens, applyToken{kind: applyTokMinus})
			i++
		case c == '*':
			tokens = append(tokens, applyToken{kind: applyTokStar})
			i++
		case c == '/':
			tokens = append(tokens, applyToken{kind: applyTokSlash})
			i++
		case c == '(':
			tokens = append(tokens, applyToken{kind: applyTokLParen})
			i++
		case c == ')':
			tokens = append(tokens, applyToken{kind: applyTokRParen})
			i++
		case c == '@':
			j := i + 1
			for j < n && (isApplyIdentChar(expr[j])) {
				j++
			}
			if j == i+1 {
				return nil, errors.New("invalid field reference in APPLY expression")
			}
			tokens = append(tokens, applyToken{kind: applyTokField, text: expr[i+1 : j]})
			i = j
		case c >= '0' && c <= '9', c == '.':
			j := i + 1
			for j < n && ((expr[j] >= '0' && expr[j] <= '9') || expr[j] == '.') {
				j++
			}
			tokens = append(tokens, applyToken{kind: applyTokNumber, text: expr[i:j]})
			i = j
		default:
			return nil, fmt.Errorf("unexpected character %q in APPLY expression", c)
		}
	}
	tokens = append(tokens, applyToken{kind: applyTokEOF})
	return tokens, nil
}

func isApplyIdentChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// applyValue is a tagged union of a numeric or string intermediate result.
type applyValue struct {
	isNum bool
	num   float64
	str   string
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

// parseExpr := term (('+' | '-') term)*
func (p *applyParser) parseExpr() (applyValue, error) {
	left, err := p.parseTerm()
	if err != nil {
		return applyValue{}, err
	}
	for p.peek().kind == applyTokPlus || p.peek().kind == applyTokMinus {
		op := p.next().kind
		right, err := p.parseTerm()
		if err != nil {
			return applyValue{}, err
		}
		left, err = applyBinaryOp(left, op, right)
		if err != nil {
			return applyValue{}, err
		}
	}
	return left, nil
}

// parseTerm := factor (('*' | '/') factor)*
func (p *applyParser) parseTerm() (applyValue, error) {
	left, err := p.parseFactor()
	if err != nil {
		return applyValue{}, err
	}
	for p.peek().kind == applyTokStar || p.peek().kind == applyTokSlash {
		op := p.next().kind
		right, err := p.parseFactor()
		if err != nil {
			return applyValue{}, err
		}
		left, err = applyBinaryOp(left, op, right)
		if err != nil {
			return applyValue{}, err
		}
	}
	return left, nil
}

// parseFactor := NUMBER | FIELD | '(' expr ')' | '-' factor
func (p *applyParser) parseFactor() (applyValue, error) {
	tok := p.next()
	switch tok.kind {
	case applyTokNumber:
		f, err := strconv.ParseFloat(tok.text, 64)
		if err != nil {
			return applyValue{}, err
		}
		return applyValue{isNum: true, num: f}, nil
	case applyTokField:
		raw, ok := p.fields[tok.text]
		if !ok {
			return applyValue{isNum: true, num: 0}, nil
		}
		return applyValueFromInterface(raw), nil
	case applyTokMinus:
		v, err := p.parseFactor()
		if err != nil {
			return applyValue{}, err
		}
		if !v.isNum {
			return applyValue{}, errors.New("unary minus on non-numeric value in APPLY expression")
		}
		return applyValue{isNum: true, num: -v.num}, nil
	case applyTokLParen:
		v, err := p.parseExpr()
		if err != nil {
			return applyValue{}, err
		}
		if p.peek().kind != applyTokRParen {
			return applyValue{}, errors.New("expected ')' in APPLY expression")
		}
		p.next()
		return v, nil
	default:
		return applyValue{}, errors.New("unexpected token in APPLY expression")
	}
}

func applyValueFromInterface(raw interface{}) applyValue {
	switch v := raw.(type) {
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
		return applyValue{str: v}
	default:
		s := fmt.Sprintf("%v", v)
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return applyValue{isNum: true, num: f}
		}
		return applyValue{str: s}
	}
}

func applyBinaryOp(left applyValue, op applyTokenKind, right applyValue) (applyValue, error) {
	if op == applyTokPlus && (!left.isNum || !right.isNum) {
		return applyValue{str: applyValueToString(left) + applyValueToString(right)}, nil
	}
	if !left.isNum || !right.isNum {
		return applyValue{}, errors.New("non-numeric operand for arithmetic operator in APPLY expression")
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
			return applyValue{}, errors.New("division by zero in APPLY expression")
		}
		return applyValue{isNum: true, num: left.num / right.num}, nil
	default:
		return applyValue{}, errors.New("unsupported operator in APPLY expression")
	}
}

func applyValueToString(v applyValue) string {
	if v.isNum {
		return strconv.FormatFloat(v.num, 'f', -1, 64)
	}
	return v.str
}
