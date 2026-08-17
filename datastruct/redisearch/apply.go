package redisearch

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
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

// PropNotLoadedError is Redis SEARCH_PROP_NOT_FOUND: APPLY/FILTER referenced a
// property that is neither LOADed nor SORTABLE (nor produced by an earlier
// pipeline stage). GROUPBY/REDUCE may still read full document fields.
type PropNotLoadedError struct {
	Name string
}

func (e *PropNotLoadedError) Error() string {
	return fmt.Sprintf("SEARCH_PROP_NOT_FOUND Property not loaded nor in pipeline: `%s`", e.Name)
}

// ValueNotFoundError is Redis SEARCH_VALUE_NOT_FOUND: a pipeline property was
// referenced but has no value on this row (use exists() to guard).
type ValueNotFoundError struct {
	Name string
}

func (e *ValueNotFoundError) Error() string {
	return fmt.Sprintf("SEARCH_VALUE_NOT_FOUND Could not find the value for a parameter name, consider using EXISTS if applicable for %s", e.Name)
}

func isValueNotFound(err error) bool {
	var vnf *ValueNotFoundError
	return errors.As(err, &vnf)
}

// CollectApplyFieldRefs returns distinct @field names referenced in an
// APPLY/FILTER expression (order of first appearance).
func CollectApplyFieldRefs(expr string) ([]string, error) {
	tokens, err := applyTokenize(expr)
	if err != nil {
		return nil, err
	}
	var refs []string
	seen := make(map[string]bool)
	for _, tok := range tokens {
		if tok.kind != applyTokField || seen[tok.text] {
			continue
		}
		seen[tok.text] = true
		refs = append(refs, tok.text)
	}
	return refs, nil
}

// EnsurePipelineProps reports PropNotLoadedError if expr references a field
// absent from props (pipeline-available names). props["*"] means LOAD * (any
// field except __key, which still requires explicit LOAD).
func EnsurePipelineProps(expr string, props map[string]bool) error {
	refs, err := CollectApplyFieldRefs(expr)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		if !pipelineHas(props, ref) {
			return &PropNotLoadedError{Name: ref}
		}
	}
	return nil
}

// EvalApplyExpr evaluates an FT.AGGREGATE expression (used by both APPLY and
// FILTER) against a row's field map. The grammar is a superset of Redis's
// aggregation expression language:
//
//	@field refs, numeric/string literals, parentheses
//	arithmetic: + - * / % ^ (power, right-assoc)
//	comparison: == != < <= > >=   (return bool)
//	logical:    && || !           (return bool)
//	functions:  log log2 exp sqrt abs ceil floor to_str to_number
//	             upper lower strlen startswith contains substr format split
//	             (split: charset sep/strip; format PARSE_ARGS; substr byte offsets)
//	             matched_terms timefmt parsetime (strftime subset) day hour minute month dayofweek
//	             dayofmonth dayofyear year monthofyear geodistance exists case
//	Missing @field → SEARCH_VALUE_NOT_FOUND (exists() ok; &&/||/case short-circuit).
//	Bad timefmt/parsetime → Null (not ERR). Non-numeric args to numeric funcs yield NaN (Redis). Unknown funcs → SEARCH_EXPR.
func EvalApplyExpr(expr string, fields map[string]interface{}) (interface{}, error) {
	return EvalApplyExprWithQuery(expr, fields, nil)
}

// EvalApplyExprWithQuery is EvalApplyExpr plus optional query terms for
// matched_terms() (terms that appear both in the query and the row's fields).
func EvalApplyExprWithQuery(expr string, fields map[string]interface{}, queryTerms []string) (interface{}, error) {
	tokens, err := applyTokenize(strings.TrimSpace(expr))
	if err != nil {
		return nil, err
	}
	p := &applyParser{tokens: tokens, fields: fields, queryTerms: queryTerms}
	v, err := p.parseBoolOr()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != applyTokEOF {
		return nil, errors.New("unexpected trailing tokens in expression")
	}
	// Field-sourced Null → SEARCH_VALUE_NOT_FOUND. Function Null (e.g. bad
	// parsetime/timefmt) is a valid empty result, matching Redis Query Engine.
	if err := v.missingErr(); err != nil {
		return nil, err
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
		return !math.IsNaN(x) && x != 0, nil
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
// against each document's pipeline fields. props is updated with each AS name.
// queryTerms feeds matched_terms().
func applyPreGroupClauses(docs []*Document, applies []ApplyClause, queryTerms []string, props map[string]bool) ([]*Document, error) {
	if len(applies) == 0 {
		return docs, nil
	}
	// Validate @refs once up front (Redis errors even when the result set is empty).
	for _, ac := range applies {
		if err := EnsurePipelineProps(ac.Expr, props); err != nil {
			return nil, err
		}
		props[ac.As] = true
	}
	// Re-run with a fresh progressive props for evaluation? props already has all AS.
	// Evaluation still needs intermediate AS on the field map per doc.
	out := make([]*Document, len(docs))
	for i, doc := range docs {
		fields := make(map[string]interface{}, len(doc.Fields)+len(applies))
		for k, v := range doc.Fields {
			fields[k] = v
		}
		for _, ac := range applies {
			val, err := EvalApplyExprWithQuery(ac.Expr, fields, queryTerms)
			if err != nil {
				return nil, err
			}
			fields[ac.As] = val
		}
		out[i] = &Document{ID: doc.ID, Fields: fields, Score: doc.Score, Payload: doc.Payload}
	}
	return out, nil
}

// applyPostGroupClauses evaluates APPLY clauses that appeared after GROUPBY
// against each result row (group), adding the computed field in place.
func applyPostGroupClauses(groups []*Group, applies []ApplyClause, queryTerms []string, props map[string]bool) error {
	if len(applies) == 0 {
		return nil
	}
	for _, ac := range applies {
		if err := EnsurePipelineProps(ac.Expr, props); err != nil {
			return err
		}
		for _, g := range groups {
			val, err := EvalApplyExprWithQuery(ac.Expr, g.Fields, queryTerms)
			if err != nil {
				return err
			}
			g.Fields[ac.As] = val
		}
		props[ac.As] = true
	}
	return nil
}

// ---- token types ----

type applyTokenKind int

const (
	applyTokEOF applyTokenKind = iota
	applyTokNumber
	applyTokString
	applyTokIdent // function name or @field-stripped identifier
	applyTokField // @field reference
	applyTokPlus
	applyTokMinus
	applyTokStar
	applyTokSlash
	applyTokPercent
	applyTokCaret
	applyTokLParen
	applyTokRParen
	applyTokComma
	applyTokEq     // ==
	applyTokNe     // !=
	applyTokLt     // <
	applyTokLe     // <=
	applyTokGt     // >
	applyTokGe     // >=
	applyTokAnd    // &&
	applyTokOr     // ||
	applyTokNot    // !
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

// applyValue is a tagged union of numeric, string, bool, null, or string-list.
type applyValue struct {
	isNum     bool
	isStr     bool
	isBool    bool
	isMulti   bool
	isNull    bool   // missing pipeline field (Redis RSValue_Null)
	nullField string // for SEARCH_VALUE_NOT_FOUND wording
	num       float64
	str       string
	b         bool
	multi     []string
}

func (v applyValue) missingErr() error {
	if !v.isNull || v.nullField == "" {
		// Bare Null (no field name) is a function result, not a missing @ref.
		return nil
	}
	return &ValueNotFoundError{Name: v.nullField}
}

type applyParser struct {
	tokens     []applyToken
	pos        int
	fields     map[string]interface{}
	queryTerms []string // for matched_terms(); may be nil
	lax        int      // >0: short-circuit dead branch — null soft, no VALUE_NOT_FOUND
}

func (p *applyParser) peek() applyToken { return p.tokens[p.pos] }

func (p *applyParser) next() applyToken {
	t := p.tokens[p.pos]
	p.pos++
	return t
}

// parseBoolOr := parseBoolAnd ('||' parseBoolAnd)*
// Redis short-circuits: when left is true, RHS is evaluated in lax mode.
func (p *applyParser) parseBoolOr() (applyValue, error) {
	left, err := p.parseBoolAnd()
	if err != nil {
		return applyValue{}, err
	}
	for p.peek().kind == applyTokOr {
		p.next()
		lb, err := applyTruthy(left)
		if err != nil {
			return applyValue{}, err
		}
		if lb {
			p.lax++
			_, err := p.parseBoolAnd()
			p.lax--
			if err != nil {
				return applyValue{}, err
			}
			left = applyBoolValue(true)
			continue
		}
		right, err := p.parseBoolAnd()
		if err != nil {
			return applyValue{}, err
		}
		rb, err := applyTruthy(right)
		if err != nil {
			return applyValue{}, err
		}
		left = applyBoolValue(rb)
	}
	return left, nil
}

// parseBoolAnd := parseComparison ('&&' parseComparison)*
// Redis short-circuits: when left is false, RHS is evaluated in lax mode.
func (p *applyParser) parseBoolAnd() (applyValue, error) {
	left, err := p.parseComparison()
	if err != nil {
		return applyValue{}, err
	}
	for p.peek().kind == applyTokAnd {
		p.next()
		lb, err := applyTruthy(left)
		if err != nil {
			return applyValue{}, err
		}
		if !lb {
			p.lax++
			_, err := p.parseComparison()
			p.lax--
			if err != nil {
				return applyValue{}, err
			}
			left = applyBoolValue(false)
			continue
		}
		right, err := p.parseComparison()
		if err != nil {
			return applyValue{}, err
		}
		rb, err := applyTruthy(right)
		if err != nil {
			return applyValue{}, err
		}
		left = applyBoolValue(rb)
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
		left = res
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
		if base.isNull {
			return base, nil
		}
		if exp.isNull {
			return exp, nil
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
		if v.isNull {
			return v, nil
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
		tb, err := applyTruthy(v)
		if err != nil {
			return applyValue{}, err
		}
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
		if !ok || raw == nil {
			return applyValue{isNull: true, nullField: tok.text}, nil
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
		if name == "case" {
			return p.parseCaseArgs()
		}
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
		return applyFunction(p, name, args)
	default:
		return applyValue{}, errors.New("unexpected token in expression")
	}
}

// parseCaseArgs evaluates case(cond, then, else) with Redis short-circuit:
// the untaken branch runs in lax mode (null soft) so VALUE_NOT_FOUND is ignored.
func (p *applyParser) parseCaseArgs() (applyValue, error) {
	cond, err := p.parseBoolOr()
	if err != nil {
		return applyValue{}, err
	}
	if p.peek().kind != applyTokComma {
		return applyValue{}, errors.New("case() expects (cond, ifTrue, ifFalse)")
	}
	p.next()
	ok, err := applyTruthy(cond)
	if err != nil {
		return applyValue{}, err
	}
	if ok {
		thenVal, err := p.parseBoolOr()
		if err != nil {
			return applyValue{}, err
		}
		if p.peek().kind != applyTokComma {
			return applyValue{}, errors.New("case() expects (cond, ifTrue, ifFalse)")
		}
		p.next()
		p.lax++
		_, err = p.parseBoolOr()
		p.lax--
		if err != nil {
			return applyValue{}, err
		}
		if p.peek().kind != applyTokRParen {
			return applyValue{}, errors.New("expected ')' to close function \"case\"")
		}
		p.next()
		return thenVal, nil
	}
	p.lax++
	_, err = p.parseBoolOr()
	p.lax--
	if err != nil {
		return applyValue{}, err
	}
	if p.peek().kind != applyTokComma {
		return applyValue{}, errors.New("case() expects (cond, ifTrue, ifFalse)")
	}
	p.next()
	elseVal, err := p.parseBoolOr()
	if err != nil {
		return applyValue{}, err
	}
	if p.peek().kind != applyTokRParen {
		return applyValue{}, errors.New("expected ')' to close function \"case\"")
	}
	p.next()
	return elseVal, nil
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
	case []string:
		return applyValue{isMulti: true, multi: append([]string(nil), v...)}
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
		if math.IsNaN(v.num) {
			return "nan" // Redis Query Engine wire form
		}
		return v.num
	}
	if v.isMulti {
		return append([]string(nil), v.multi...)
	}
	if v.isStr {
		return v.str
	}
	return nil
}

// applyTruthy coerces a value to bool (FILTER semantics). Null → VALUE_NOT_FOUND.
func applyTruthy(v applyValue) (bool, error) {
	if err := v.missingErr(); err != nil {
		return false, err
	}
	if v.isBool {
		return v.b, nil
	}
	if v.isNum {
		return v.num != 0, nil
	}
	if v.isMulti {
		return len(v.multi) > 0, nil
	}
	if v.isStr {
		return v.str != "", nil
	}
	return false, nil
}

func applyBinaryArith(left applyValue, op applyTokenKind, right applyValue) (applyValue, error) {
	if left.isNull {
		return left, nil
	}
	if right.isNull {
		return right, nil
	}
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

// applyCompare evaluates a comparison operator. Null operands propagate (Redis
// Null); numeric vs numeric compares by value; otherwise stringify.
func applyCompare(left applyValue, op applyTokenKind, right applyValue) (applyValue, error) {
	if left.isNull {
		return left, nil
	}
	if right.isNull {
		return right, nil
	}
	var res bool
	if left.isNum && right.isNum {
		switch op {
		case applyTokEq, applyTokAssign:
			res = left.num == right.num
		case applyTokNe:
			res = left.num != right.num
		case applyTokLt:
			res = left.num < right.num
		case applyTokLe:
			res = left.num <= right.num
		case applyTokGt:
			res = left.num > right.num
		case applyTokGe:
			res = left.num >= right.num
		default:
			return applyValue{}, errors.New("unsupported comparison operator")
		}
		return applyBoolValue(res), nil
	}
	ls := applyValueToString(left)
	rs := applyValueToString(right)
	switch op {
	case applyTokEq, applyTokAssign:
		res = ls == rs
	case applyTokNe:
		res = ls != rs
	case applyTokLt:
		res = ls < rs
	case applyTokLe:
		res = ls <= rs
	case applyTokGt:
		res = ls > rs
	case applyTokGe:
		res = ls >= rs
	default:
		return applyValue{}, errors.New("unsupported comparison operator")
	}
	return applyBoolValue(res), nil
}

func applyValueToString(v applyValue) string {
	if v.isBool {
		if v.b {
			return "true"
		}
		return "false"
	}
	if v.isNum {
		if math.IsNaN(v.num) {
			return "nan" // Redis Query Engine lowercase
		}
		return strconv.FormatFloat(v.num, 'f', -1, 64)
	}
	if v.isMulti {
		return strings.Join(v.multi, ",")
	}
	return v.str
}

// applyFunction dispatches a built-in function call.
func applyFunction(p *applyParser, name string, args []applyValue) (applyValue, error) {
	// Redis: missing @field is Null. exists() accepts Null; other funcs propagate
	// Null so &&/||/case can short-circuit after the RHS is fully parsed.
	// Top-level Null → SEARCH_VALUE_NOT_FOUND (EvalApplyExpr / applyTruthy / arith).
	if name != "exists" {
		for _, a := range args {
			if a.isNull {
				return a, nil
			}
		}
	}
	switch name {
	case "matched_terms":
		// Redis returns query terms that also appear in the row's text fields
		// (order preserved; multi-value array on the wire). Optional max_terms
		// defaults to 100. Stemmer variants (+redi…) are intentionally omitted.
		if len(args) > 1 {
			return applyValue{}, errors.New("matched_terms() expects at most one numeric argument")
		}
		maxTerms := 100
		if len(args) == 1 {
			if !args[0].isNum || args[0].num < 0 {
				return applyValue{}, errors.New("matched_terms() max_terms must be a non-negative number")
			}
			maxTerms = int(args[0].num)
		}
		if p == nil || len(p.queryTerms) == 0 || maxTerms == 0 {
			return applyValue{isMulti: true, multi: nil}, nil
		}
		tok := &StandardTokenizer{}
		fieldBlob := strings.Builder{}
		for _, v := range p.fields {
			fieldBlob.WriteString(fmt.Sprintf("%v ", v))
		}
		present := make(map[string]struct{})
		for _, t := range tok.Tokenize(fieldBlob.String()) {
			present[t] = struct{}{}
		}
		var matched []string
		seen := make(map[string]struct{})
		for _, term := range p.queryTerms {
			if len(matched) >= maxTerms {
				break
			}
			term = strings.ToLower(strings.TrimSpace(term))
			if term == "" {
				continue
			}
			if _, ok := present[term]; !ok {
				continue
			}
			if _, dup := seen[term]; dup {
				continue
			}
			seen[term] = struct{}{}
			matched = append(matched, term)
		}
		return applyValue{isMulti: true, multi: matched}, nil
	// Numeric functions. Non-numeric args → NaN (Redis Query Engine).
	case "log":
		if len(args) != 1 {
			return applyValue{}, errors.New("log() expects one numeric argument")
		}
		if !args[0].isNum {
			return applyValue{isNum: true, num: math.NaN()}, nil
		}
		return applyValue{isNum: true, num: math.Log(args[0].num)}, nil
	case "log2":
		if len(args) != 1 {
			return applyValue{}, errors.New("log2() expects one numeric argument")
		}
		if !args[0].isNum {
			return applyValue{isNum: true, num: math.NaN()}, nil
		}
		return applyValue{isNum: true, num: math.Log2(args[0].num)}, nil
	case "exp":
		if len(args) != 1 {
			return applyValue{}, errors.New("exp() expects one numeric argument")
		}
		if !args[0].isNum {
			return applyValue{isNum: true, num: math.NaN()}, nil
		}
		return applyValue{isNum: true, num: math.Exp(args[0].num)}, nil
	case "sqrt":
		if len(args) != 1 {
			return applyValue{}, errors.New("sqrt() expects one numeric argument")
		}
		if !args[0].isNum {
			return applyValue{isNum: true, num: math.NaN()}, nil
		}
		return applyValue{isNum: true, num: math.Sqrt(args[0].num)}, nil
	case "abs":
		if len(args) != 1 {
			return applyValue{}, errors.New("abs() expects one numeric argument")
		}
		if !args[0].isNum {
			return applyValue{isNum: true, num: math.NaN()}, nil
		}
		return applyValue{isNum: true, num: math.Abs(args[0].num)}, nil
	case "ceil":
		if len(args) != 1 {
			return applyValue{}, errors.New("ceil() expects one numeric argument")
		}
		if !args[0].isNum {
			return applyValue{isNum: true, num: math.NaN()}, nil
		}
		return applyValue{isNum: true, num: math.Ceil(args[0].num)}, nil
	case "floor":
		if len(args) != 1 {
			return applyValue{}, errors.New("floor() expects one numeric argument")
		}
		if !args[0].isNum {
			return applyValue{isNum: true, num: math.NaN()}, nil
		}
		return applyValue{isNum: true, num: math.Floor(args[0].num)}, nil
	// Time functions use Unix seconds and UTC, matching Redis Query Engine.
	case "timefmt":
		if len(args) < 1 || len(args) > 2 || !args[0].isNum {
			return applyValue{}, errors.New("timefmt() expects a timestamp and optional format")
		}
		tm := time.Unix(int64(args[0].num), 0).UTC()
		if len(args) == 1 {
			return applyValue{isStr: true, str: tm.Format("2006-01-02T15:04:05Z")}, nil
		}
		s, ok := strftimeFormat(tm, applyValueToString(args[1]))
		if !ok {
			return applyValue{isNull: true}, nil // Redis: bad format → Null
		}
		return applyValue{isStr: true, str: s}, nil
	case "parsetime":
		if len(args) != 2 {
			return applyValue{}, errors.New("parsetime() expects a time string and format")
		}
		tm, err := time.ParseInLocation(
			strftimeToGoLayout(applyValueToString(args[1])),
			applyValueToString(args[0]),
			time.UTC,
		)
		if err != nil {
			return applyValue{isNull: true}, nil // Redis: parse fail → Null
		}
		return applyValue{isNum: true, num: float64(tm.Unix())}, nil
	case "day", "hour", "minute", "month", "dayofweek", "dayofmonth", "dayofyear", "year", "monthofyear":
		if len(args) != 1 || !args[0].isNum {
			return applyValue{}, fmt.Errorf("%s() expects one numeric timestamp", name)
		}
		tm := time.Unix(int64(args[0].num), 0).UTC()
		switch name {
		case "day":
			tm = time.Date(tm.Year(), tm.Month(), tm.Day(), 0, 0, 0, 0, time.UTC)
			return applyValue{isNum: true, num: float64(tm.Unix())}, nil
		case "hour":
			return applyValue{isNum: true, num: float64(tm.Truncate(time.Hour).Unix())}, nil
		case "minute":
			return applyValue{isNum: true, num: float64(tm.Truncate(time.Minute).Unix())}, nil
		case "month":
			tm = time.Date(tm.Year(), tm.Month(), 1, 0, 0, 0, 0, time.UTC)
			return applyValue{isNum: true, num: float64(tm.Unix())}, nil
		case "dayofweek":
			return applyValue{isNum: true, num: float64(tm.Weekday())}, nil
		case "dayofmonth":
			return applyValue{isNum: true, num: float64(tm.Day())}, nil
		case "dayofyear":
			return applyValue{isNum: true, num: float64(tm.YearDay() - 1)}, nil
		case "year":
			return applyValue{isNum: true, num: float64(tm.Year())}, nil
		default:
			return applyValue{isNum: true, num: float64(tm.Month() - 1)}, nil
		}
	case "geodistance":
		lon1, lat1, lon2, lat2, err := applyGeoCoordinates(args)
		if err != nil {
			return applyValue{}, err
		}
		const earthRadiusMeters = 6372797.560856
		toRadians := math.Pi / 180
		lat1 *= toRadians
		lat2 *= toRadians
		dLat := lat2 - lat1
		dLon := (lon2 - lon1) * toRadians
		a := math.Sin(dLat/2)*math.Sin(dLat/2) +
			math.Cos(lat1)*math.Cos(lat2)*math.Sin(dLon/2)*math.Sin(dLon/2)
		return applyValue{isNum: true, num: earthRadiusMeters * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))}, nil
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
		out, err := applyFormat(fmtStr, args[1:])
		if err != nil {
			return applyValue{}, err
		}
		return applyValue{isStr: true, str: out}, nil
	case "split":
		if len(args) < 1 || len(args) > 3 {
			return applyValue{}, errors.New("split() expects 1 to 3 arguments")
		}
		// Redis: sep/strip are character sets (strpbrk / strchr); empty tokens dropped.
		s := applyValueToString(args[0])
		sep := ","
		strip := " "
		if len(args) >= 2 {
			sep = applyValueToString(args[1])
		}
		if len(args) == 3 {
			strip = applyValueToString(args[2])
		}
		return applyValue{isMulti: true, multi: applySplit(s, sep, strip)}, nil
	// Boolean / control.
	case "exists":
		if len(args) != 1 {
			return applyValue{}, errors.New("exists() expects one argument")
		}
		// Redis: only Null/missing is false; literal 0 / "" / present-zero are true.
		if args[0].isNull {
			return applyValue{isNum: true, num: 0}, nil
		}
		return applyValue{isNum: true, num: 1}, nil
	case "case":
		// Prefer parseCaseArgs (short-circuit); this path is unreachable normally.
		if len(args) != 3 {
			return applyValue{}, errors.New("case() expects (cond, ifTrue, ifFalse)")
		}
		cond, err := applyTruthy(args[0])
		if err != nil {
			return applyValue{}, err
		}
		if cond {
			return args[1], nil
		}
		return args[2], nil
	case "to_str":
		if len(args) != 1 {
			return applyValue{}, errors.New("to_str() expects one argument")
		}
		return applyValue{isStr: true, str: applyValueToString(args[0])}, nil
	case "to_number":
		if len(args) != 1 {
			return applyValue{}, errors.New("to_number() expects one argument")
		}
		if args[0].isNum {
			return applyValue{isNum: true, num: args[0].num}, nil
		}
		s := applyValueToString(args[0])
		n, err := strconv.ParseFloat(s, 64)
		if err != nil {
			// Redis 8.x: SEARCH_PARSE_ARGS to_number: cannot convert string '…'
			return applyValue{}, fmt.Errorf("SEARCH_PARSE_ARGS to_number: cannot convert string '%s'", s)
		}
		return applyValue{isNum: true, num: n}, nil
	}
	return applyValue{}, fmt.Errorf("SEARCH_EXPR Unknown function name '%s'", name)
}

func applyParseLonLat(v applyValue) (float64, float64, error) {
	parts := strings.Split(applyValueToString(v), ",")
	if len(parts) != 2 {
		return 0, 0, errors.New("geodistance() expects lon,lat coordinates")
	}
	lon, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	if err != nil {
		return 0, 0, errors.New("geodistance() expects numeric longitude")
	}
	lat, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return 0, 0, errors.New("geodistance() expects numeric latitude")
	}
	return lon, lat, nil
}

func applyGeoCoordinates(args []applyValue) (float64, float64, float64, float64, error) {
	if len(args) == 4 && args[0].isNum && args[1].isNum && args[2].isNum && args[3].isNum {
		return args[0].num, args[1].num, args[2].num, args[3].num, nil
	}
	// Redis 3-arg: geodistance(lon,lat,point) or geodistance(point,lon,lat).
	if len(args) == 3 {
		if args[0].isNum && args[1].isNum {
			lon2, lat2, err := applyParseLonLat(args[2])
			return args[0].num, args[1].num, lon2, lat2, err
		}
		if args[1].isNum && args[2].isNum {
			lon1, lat1, err := applyParseLonLat(args[0])
			return lon1, lat1, args[1].num, args[2].num, err
		}
		return 0, 0, 0, 0, errors.New("geodistance() expects two coordinates, three mixed, or four numbers")
	}
	if len(args) == 2 {
		lon1, lat1, err := applyParseLonLat(args[0])
		if err != nil {
			return 0, 0, 0, 0, err
		}
		lon2, lat2, err := applyParseLonLat(args[1])
		return lon1, lat1, lon2, lat2, err
	}
	return 0, 0, 0, 0, errors.New("geodistance() expects two coordinates, three mixed, or four numbers")
}

// strftimeFormat renders tm with a Redis Query Engine strftime subset.
// ok=false when the format contains an unsupported directive (Redis → Null).
func strftimeFormat(tm time.Time, format string) (string, bool) {
	var b strings.Builder
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			b.WriteByte(format[i])
			continue
		}
		if i+1 >= len(format) {
			return "", false
		}
		i++
		switch format[i] {
		case '%':
			b.WriteByte('%')
		case 'Y':
			b.WriteString(tm.Format("2006"))
		case 'y':
			b.WriteString(tm.Format("06"))
		case 'm':
			b.WriteString(tm.Format("01"))
		case 'd':
			b.WriteString(tm.Format("02"))
		case 'e': // space-padded day 1-31
			b.WriteString(tm.Format("_2"))
		case 'H':
			b.WriteString(tm.Format("15"))
		case 'M':
			b.WriteString(tm.Format("04"))
		case 'S':
			b.WriteString(tm.Format("05"))
		case 'I':
			b.WriteString(tm.Format("03"))
		case 'p':
			b.WriteString(tm.Format("PM"))
		case 'a':
			b.WriteString(tm.Format("Mon"))
		case 'A':
			b.WriteString(tm.Format("Monday"))
		case 'b':
			b.WriteString(tm.Format("Jan"))
		case 'B':
			b.WriteString(tm.Format("January"))
		case 'F': // %Y-%m-%d
			b.WriteString(tm.Format("2006-01-02"))
		case 'T': // %H:%M:%S
			b.WriteString(tm.Format("15:04:05"))
		case 'R': // %H:%M
			b.WriteString(tm.Format("15:04"))
		case 'z':
			b.WriteString(tm.Format("-0700"))
		case 'w': // weekday 0=Sunday … 6=Saturday
			b.WriteString(strconv.Itoa(int(tm.Weekday())))
		case 'j': // day of year 001-366
			b.WriteString(tm.Format("002"))
		default:
			return "", false
		}
	}
	return b.String(), true
}

// strftimeToGoLayout maps a Redis strftime subset to a Go time.Parse layout
// (used by parsetime). Unknown directives are left as-is and typically fail Parse.
func strftimeToGoLayout(format string) string {
	var b strings.Builder
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			b.WriteByte(format[i])
			continue
		}
		if i+1 >= len(format) {
			b.WriteByte('%')
			break
		}
		i++
		switch format[i] {
		case '%':
			b.WriteByte('%')
		case 'Y':
			b.WriteString("2006")
		case 'y':
			b.WriteString("06")
		case 'm':
			b.WriteString("01")
		case 'd':
			b.WriteString("02")
		case 'e':
			b.WriteString("_2")
		case 'H':
			b.WriteString("15")
		case 'M':
			b.WriteString("04")
		case 'S':
			b.WriteString("05")
		case 'I':
			b.WriteString("03")
		case 'p':
			b.WriteString("PM")
		case 'a':
			b.WriteString("Mon")
		case 'A':
			b.WriteString("Monday")
		case 'b':
			b.WriteString("Jan")
		case 'B':
			b.WriteString("January")
		case 'F':
			b.WriteString("2006-01-02")
		case 'T':
			b.WriteString("15:04:05")
		case 'R':
			b.WriteString("15:04")
		case 'z':
			b.WriteString("-0700")
		default:
			// Keep unknown as literal so Parse fails → Null (Redis).
			b.WriteByte('%')
			b.WriteByte(format[i])
		}
	}
	return b.String()
}

// applySubstr implements substr(s, offset, count) with Redis byte semantics:
// negative offset counts from the end; count < 0 means "to the end".
func applySubstr(s string, offset, count int) string {
	n := len(s) // byte length (Redis Query Engine)
	if offset < 0 {
		offset = n + offset
		if offset < 0 {
			offset = 0
		}
	}
	if offset > n {
		offset = n
	}
	end := offset + count
	if count < 0 || end > n {
		end = n
	}
	if end < offset {
		end = offset
	}
	return s[offset:end]
}

// applySplit splits s on any rune in sepChars, trims leading/trailing runes in
// stripChars from each token, drops empty tokens, and caps at 1024 parts
// (Redis RediSearch stringfunc_split).
func applySplit(s, sepChars, stripChars string) []string {
	if s == "" {
		return nil
	}
	if sepChars == "" {
		// No separators → one token (still strip ends).
		tok := applyStripCharset(s, stripChars)
		if tok == "" {
			return nil
		}
		return []string{tok}
	}
	parts := make([]string, 0, 8)
	start := 0
	for i := 0; i < len(s) && len(parts) < 1024; i++ {
		if strings.ContainsRune(sepChars, rune(s[i])) {
			tok := applyStripCharset(s[start:i], stripChars)
			if tok != "" {
				parts = append(parts, tok)
			}
			start = i + 1
		}
	}
	if len(parts) < 1024 && start <= len(s) {
		tok := applyStripCharset(s[start:], stripChars)
		if tok != "" {
			parts = append(parts, tok)
		}
	}
	return parts
}

func applyStripCharset(s, cset string) string {
	if s == "" {
		return ""
	}
	start, end := 0, len(s)
	for start < end && strings.ContainsRune(cset, rune(s[start])) {
		start++
	}
	for end > start && strings.ContainsRune(cset, rune(s[end-1])) {
		end--
	}
	return s[start:end]
}

// applyFormat replaces each %s in fmtStr with successive args (Redis only
// supports %s). A literal %% becomes %. Error wording matches Redis PARSE_ARGS.
func applyFormat(fmtStr string, args []applyValue) (string, error) {
	var b strings.Builder
	ai := 0
	for i := 0; i < len(fmtStr); i++ {
		if fmtStr[i] != '%' {
			b.WriteByte(fmtStr[i])
			continue
		}
		if i+1 >= len(fmtStr) {
			return "", errors.New("SEARCH_PARSE_ARGS Bad format string!")
		}
		switch fmtStr[i+1] {
		case '%':
			b.WriteByte('%')
			i++
		case 's':
			if ai >= len(args) {
				return "", errors.New("SEARCH_PARSE_ARGS Not enough arguments for format")
			}
			b.WriteString(applyValueToString(args[ai]))
			ai++
			i++
		default:
			return "", errors.New("SEARCH_PARSE_ARGS Unknown format specifier passed")
		}
	}
	return b.String(), nil
}
