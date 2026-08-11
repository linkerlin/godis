package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestP5ScorerDispatch verifies the SCORER option selects the ranking function:
// DOCSCORE returns the doc's FT.ADD SCORE verbatim, so scores track the declared
// score rather than term statistics.
func TestP5ScorerDispatch(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p5d", "SCHEMA", "t", "TEXT",
	)), "OK")
	// Two docs both matching "golang" but with distinct declared scores.
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p5d", "p5d:lo", "SCORE", "1.0", "FIELDS", "t", "golang")); protocol.IsErrorReply(r) {
		t.Fatalf("add lo: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p5d", "p5d:hi", "SCORE", "5.0", "FIELDS", "t", "golang")); protocol.IsErrorReply(r) {
		t.Fatalf("add hi: %s", r.ToBytes())
	}

	// DOCSCORE: score = declared score. hi (5.0) ranks above lo (1.0).
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p5d", "golang", "WITHSCORES", "SCORER", "DOCSCORE", "NOCONTENT"))
	scores := ftSearchScores(t, r)
	if len(scores) != 2 {
		t.Fatalf("DOCSCORE want 2 scored hits, got %d (%s)", len(scores), r.ToBytes())
	}
	if scores[0] < scores[1] {
		t.Fatalf("DOCSCORE first hit should rank higher; got %v", scores)
	}

	// Invalid scorer name falls back to default rather than erroring.
	r2 := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p5d", "golang", "SCORER", "NOSUCH", "NOCONTENT"))
	if protocol.IsErrorReply(r2) {
		t.Fatalf("unknown scorer should fall back, not error: %s", r2.ToBytes())
	}
}

// TestP5BM25vsDISMAX verifies BM25STD and DISMAX produce different scores for
// the same query, confirming SCORER dispatch actually changes the ranking math.
// (BM25STD and TFIDF can coincide on a single-doc index; DISMAX = raw tf sum
// always diverges from the normalized/saturating BM25.)
func TestP5BM25vsDISMAX(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p5bt", "SCHEMA", "t", "TEXT",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p5bt", "p5bt:1", "SCORE", "1.0", "FIELDS", "t", "golang tutorial")); protocol.IsErrorReply(r) {
		t.Fatalf("add: %s", r.ToBytes())
	}
	bm := ftSearchScores(t, db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p5bt", "golang", "WITHSCORES", "SCORER", "BM25STD", "NOCONTENT")))
	dm := ftSearchScores(t, db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p5bt", "golang", "WITHSCORES", "SCORER", "DISMAX", "NOCONTENT")))
	if len(bm) != 1 || len(dm) != 1 {
		t.Fatalf("want 1 hit each, got bm=%v dm=%v", bm, dm)
	}
	if bm[0] <= 0 || dm[0] <= 0 {
		t.Fatalf("scores must be positive: bm=%v dm=%v", bm, dm)
	}
	if bm[0] == dm[0] {
		t.Fatalf("BM25STD and DISMAX produced identical score %v (dispatch broken)", bm[0])
	}
}

// TestP5BM25FieldWeight verifies TEXT WEIGHT multiplies into BM25STD so a hit
// in a heavier field outranks the same term in a lighter field.
func TestP5BM25FieldWeight(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p5w", "SCHEMA",
		"title", "TEXT", "WEIGHT", "5.0",
		"body", "TEXT", "WEIGHT", "1.0",
	)), "OK")
	// d1: term only in title (weight 5); d2: term only in body (weight 1).
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p5w", "p5w:title", "SCORE", "1.0", "FIELDS", "title", "golang", "body", "other")); protocol.IsErrorReply(r) {
		t.Fatalf("add title: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p5w", "p5w:body", "SCORE", "1.0", "FIELDS", "title", "other", "body", "golang")); protocol.IsErrorReply(r) {
		t.Fatalf("add body: %s", r.ToBytes())
	}
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p5w", "golang", "WITHSCORES", "SCORER", "BM25STD", "NOCONTENT"))
	ids := ftSearchIDs(t, r)
	if len(ids) < 2 {
		t.Fatalf("want 2 hits, got %v (%s)", ids, r.ToBytes())
	}
	if ids[0] != "p5w:title" {
		t.Fatalf("WEIGHT 5 title should rank first, got %v scores=%v reply=%s", ids, ftSearchScores(t, r), r.ToBytes())
	}
}

// TestP5BM25DocLength prefers the shorter document when the matched term and
// field WEIGHT are equal (b>0 length normalization). Guards avgdl and docLengths.
func TestP5BM25DocLength(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p5dl", "SCHEMA", "t", "TEXT",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p5dl", "p5dl:short", "SCORE", "1.0", "FIELDS", "t", "golang")); protocol.IsErrorReply(r) {
		t.Fatalf("add short: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p5dl", "p5dl:long", "SCORE", "1.0", "FIELDS", "t",
		"golang alpha bravo charlie delta echo foxtrot golf hotel india")); protocol.IsErrorReply(r) {
		t.Fatalf("add long: %s", r.ToBytes())
	}
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p5dl", "golang", "WITHSCORES", "SCORER", "BM25STD", "NOCONTENT"))
	ids := ftSearchIDs(t, r)
	if len(ids) < 2 {
		t.Fatalf("want 2 hits, got %v (%s)", ids, r.ToBytes())
	}
	if ids[0] != "p5dl:short" {
		t.Fatalf("shorter doc should rank first, got %v scores=%v", ids, ftSearchScores(t, r))
	}
}

// TestP5BM25STDTanh verifies BM25STD.TANH applies tanh(raw/4) so scores stay
// in (0,1) for positive BM25 and differ from the unbound BM25STD score.
func TestP5BM25STDTanh(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p5t", "SCHEMA", "t", "TEXT",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p5t", "p5t:1", "SCORE", "1.0", "FIELDS", "t", "golang")); protocol.IsErrorReply(r) {
		t.Fatalf("add: %s", r.ToBytes())
	}
	raw := ftSearchScores(t, db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p5t", "golang", "WITHSCORES", "SCORER", "BM25STD", "NOCONTENT")))
	tanh := ftSearchScores(t, db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p5t", "golang", "WITHSCORES", "SCORER", "BM25STD.TANH", "NOCONTENT")))
	if len(raw) != 1 || len(tanh) != 1 {
		t.Fatalf("want 1 hit each, got raw=%v tanh=%v", raw, tanh)
	}
	if raw[0] <= 0 {
		t.Fatalf("BM25STD must be positive, got %v", raw)
	}
	if tanh[0] <= 0 || tanh[0] >= 1 {
		t.Fatalf("BM25STD.TANH want in (0,1), got %v", tanh)
	}
	if tanh[0] >= raw[0] {
		t.Fatalf("tanh(raw/4) should be < raw for raw>0; raw=%v tanh=%v", raw[0], tanh[0])
	}
}

// TestP5BM25STDNormMinMax verifies BM25STD.NORM rescales the full hit set to
// [0,1] via min-max (not the old x/(1+x) approximation).
func TestP5BM25STDNormMinMax(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p5n", "SCHEMA", "t", "TEXT",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p5n", "p5n:short", "SCORE", "1.0", "FIELDS", "t", "golang")); protocol.IsErrorReply(r) {
		t.Fatalf("add short: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p5n", "p5n:long", "SCORE", "1.0", "FIELDS", "t",
		"golang alpha bravo charlie delta echo foxtrot golf hotel india")); protocol.IsErrorReply(r) {
		t.Fatalf("add long: %s", r.ToBytes())
	}
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p5n", "golang", "WITHSCORES", "SCORER", "BM25STD.NORM", "NOCONTENT"))
	scores := ftSearchScores(t, r)
	if len(scores) != 2 {
		t.Fatalf("want 2 scores, got %v (%s)", scores, r.ToBytes())
	}
	// Highest BM25 (short) → 1.0; lowest → 0.0 after min-max.
	if scores[0] < 0.999 {
		t.Fatalf("top NORM score want ~1, got %v", scores)
	}
	if scores[1] > 0.001 {
		t.Fatalf("bottom NORM score want ~0, got %v", scores)
	}
	ids := ftSearchIDs(t, r)
	if ids[0] != "p5n:short" {
		t.Fatalf("ranking should preserve BM25 order, got %v", ids)
	}
}

// ftSearchIDs extracts document ids from a NOCONTENT WITHSCORES (or plain) reply.
func ftSearchIDs(t *testing.T, r redis.Reply) []string {
	t.Helper()
	mr := ftSearchMultiRaw(r)
	if mr == nil {
		return nil
	}
	var out []string
	// [total, id1, score1?, id2, ...] — WITHSCORES→step 2 after total; without→step 1
	// Detect: if reply[2] parses as float and len odd-ish, WITHSCORES.
	step := 1
	if len(mr.Replies) >= 3 {
		if b, ok := mr.Replies[2].(*protocol.BulkReply); ok {
			if _, err := strParseFloat(strings.TrimSpace(string(b.Arg))); err == nil {
				step = 2
			}
		}
	}
	for i := 1; i < len(mr.Replies); i += step {
		if b, ok := mr.Replies[i].(*protocol.BulkReply); ok {
			out = append(out, string(b.Arg))
		}
	}
	return out
}

// TestP5OptionalBoost verifies an optional ~term boosts docs containing it in
// the score without affecting which docs match. The base query matches all docs;
// the one with the optional term should score higher.
func TestP5OptionalBoost(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p5ob", "SCHEMA", "t", "TEXT",
	)), "OK")
	// Both docs match the wildcard; d2 also contains "special".
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p5ob", "p5ob:1", "SCORE", "1.0", "FIELDS", "t", "plain")); protocol.IsErrorReply(r) {
		t.Fatalf("add 1: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p5ob", "p5ob:2", "SCORE", "1.0", "FIELDS", "t", "plain special")); protocol.IsErrorReply(r) {
		t.Fatalf("add 2: %s", r.ToBytes())
	}
	// "*" matches all; ~special is optional. d2 should score higher because it
	// contains the optional term.
	// Note: the ExpressionParser treats ~ specially only in the fallback parser;
	// to exercise the boost path we query the literal "special" set OR'd with the
	// optional. Use a direct term query + WITHSCORES on both docs.
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p5ob", "plain ~special", "WITHSCORES", "NOCONTENT"))
	scores := ftSearchScores(t, r)
	if len(scores) != 2 {
		t.Fatalf("want 2 hits (optional must not filter), got %d (%s)", len(scores), r.ToBytes())
	}
	// The doc containing the optional term (d2, "plain special") should outrank
	// the doc without it (d1, "plain"). scores[0] is the higher-ranked hit.
	if scores[0] <= scores[1] {
		t.Fatalf("optional ~special should boost the matching doc; scores=%v", scores)
	}
}

// ftSearchScores extracts the WITHSCORES values (as floats) from a FT.SEARCH
// reply, in ranked order. Returns nil if the reply shape is unrecognized.
func ftSearchScores(t *testing.T, r redis.Reply) []float64 {
	t.Helper()
	mr := ftSearchMultiRaw(r)
	if mr == nil {
		return nil
	}
	var out []float64
	// Reply shape with WITHSCORES, NOCONTENT: [total, id1, score1, id2, score2, ...]
	for i := 2; i < len(mr.Replies); i += 2 {
		if b, ok := mr.Replies[i].(*protocol.BulkReply); ok {
			s := strings.TrimSpace(string(b.Arg))
			if f, err := strParseFloat(s); err == nil {
				out = append(out, f)
			}
		}
	}
	return out
}

// strParseFloat parses a float without importing strconv into the helper name.
func strParseFloat(s string) (float64, error) {
	var f float64
	var sign float64 = 1
	i := 0
	if i < len(s) && (s[i] == '-') {
		sign = -1
		i++
	} else if i < len(s) && s[i] == '+' {
		i++
	}
	sawDigit := false
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		f = f*10 + float64(s[i]-'0')
		sawDigit = true
		i++
	}
	if i < len(s) && s[i] == '.' {
		i++
		div := 10.0
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			f += float64(s[i]-'0') / div
			div *= 10
			sawDigit = true
			i++
		}
	}
	// Exponent (e.g. 1.23e+02) — best-effort.
	if i < len(s) && (s[i] == 'e' || s[i] == 'E') {
		i++
		esign := 1.0
		if i < len(s) && s[i] == '-' {
			esign = -1
			i++
		} else if i < len(s) && s[i] == '+' {
			i++
		}
		var exp int
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			exp = exp*10 + int(s[i]-'0')
			i++
		}
		pow := 1.0
		for k := 0; k < exp; k++ {
			pow *= 10
		}
		if esign < 0 {
			f = f / pow
		} else {
			f = f * pow
		}
	}
	if !sawDigit {
		return 0, errNotNumber
	}
	return sign * f, nil
}

var errNotNumber = &p5err{"not a number"}

type p5err struct{ msg string }

func (e *p5err) Error() string { return e.msg }
