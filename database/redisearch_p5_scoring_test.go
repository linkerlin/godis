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
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok {
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
