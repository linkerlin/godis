package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestP3aNumericCompare verifies DIALECT 2 comparison operators on a NUMERIC
// field: ==, !=, >, >=, <, <=. Each op is exercised against a small doc set.
func TestP3aNumericCompare(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p3a", "SCHEMA", "n", "NUMERIC",
	)), "OK")
	for _, kv := range []struct{ id string; n string }{
		{"p3a:1", "1"}, {"p3a:5", "5"}, {"p3a:10", "10"}, {"p3a:20", "20"},
	} {
		if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p3a", kv.id, "FIELDS", "n", kv.n)); protocol.IsErrorReply(r) {
			t.Fatalf("add %s: %s", kv.id, r.ToBytes())
		}
	}
	cases := []struct {
		op   string
		val  string
		want int64
	}{
		{"==", "5", 1},   // only p3a:5
		{"!=", "5", 3},   // 1,10,20
		{">", "5", 2},    // 10,20
		{">=", "5", 3},   // 5,10,20
		{"<", "10", 2},   // 1,5
		{"<=", "10", 3},  // 1,5,10
	}
	for _, c := range cases {
		r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p3a", "@n "+c.op+" "+c.val, "NOCONTENT", "DIALECT", "2"))
		if !searchTotalIs(t, r, c.want) {
			t.Fatalf("op %s %s: want %d, got %s", c.op, c.val, c.want, r.ToBytes())
		}
	}
}

// TestP3aCompareRequiresDialect2 verifies comparison ops are rejected when the
// declared dialect is < 2.
func TestP3aCompareRequiresDialect2(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p3ad", "SCHEMA", "n", "NUMERIC",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p3ad", "d1", "FIELDS", "n", "5")); protocol.IsErrorReply(r) {
		t.Fatalf("add: %s", r.ToBytes())
	}
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p3ad", "@n == 5", "NOCONTENT", "DIALECT", "1"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "DIALECT 2") {
		t.Fatalf("comparison in DIALECT 1 should error: %s", r.ToBytes())
	}
}

// TestP3cMultiFieldScope verifies @f1|f2:term matches the term in either field.
func TestP3cMultiFieldScope(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p3c", "SCHEMA", "title", "TEXT", "body", "TEXT",
	)), "OK")
	// d1 has "golang" in title; d2 has "golang" in body; d3 has neither.
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p3c", "d1", "FIELDS", "title", "golang", "body", "intro")); protocol.IsErrorReply(r) {
		t.Fatalf("add d1: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p3c", "d2", "FIELDS", "title", "hello", "body", "golang rules")); protocol.IsErrorReply(r) {
		t.Fatalf("add d2: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p3c", "d3", "FIELDS", "title", "hello", "body", "world")); protocol.IsErrorReply(r) {
		t.Fatalf("add d3: %s", r.ToBytes())
	}
	// @title|body:golang should match d1 and d2.
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p3c", "@title|body:golang", "NOCONTENT", "DIALECT", "2"))
	if !searchTotalIs(t, r, 2) {
		t.Fatalf("@title|body:golang should match 2 docs: %s", r.ToBytes())
	}
}

// TestP3bIsMissing verifies ismissing(@field) returns docs where the field is
// absent.
func TestP3bIsMissing(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p3b", "SCHEMA", "opt", "TEXT", "INDEXMISSING",
	)), "OK")
	// d1 has "opt"; d2 omits it entirely.
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p3b", "d1", "FIELDS", "opt", "yes")); protocol.IsErrorReply(r) {
		t.Fatalf("add d1: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p3b", "d2", "FIELDS", "other", "x")); protocol.IsErrorReply(r) {
		t.Fatalf("add d2: %s", r.ToBytes())
	}
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p3b", "ismissing(@opt)", "NOCONTENT", "DIALECT", "2"))
	if !searchTotalIs(t, r, 1) {
		t.Fatalf("ismissing(@opt) should match 1 doc (d2): %s", r.ToBytes())
	}
}

// TestP3dOrPrecedenceD1vsD2 verifies the | vs space precedence flip.
// Query "alpha beta | gamma" over docs {alpha+beta}, {alpha+gamma}, {gamma-only}:
//   D1: | looser  -> (alpha beta) | gamma  — ab (both), ag (gamma), g (gamma) = 3
//   D2: | tighter -> alpha (beta | gamma)  — ab, ag; g excluded (no alpha)    = 2
// Discriminator: gamma-only "g" is matched by D1 but excluded by D2.
func TestP3dOrPrecedenceD1vsD2(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p3d", "SCHEMA", "t", "TEXT", "NOSTEM",
	)), "OK")
	for _, kv := range []struct{ id, body string }{
		{"p3d:ab", "alpha beta"},
		{"p3d:ag", "alpha gamma"},
		{"p3d:g", "gamma"},
	} {
		if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p3d", kv.id, "FIELDS", "t", kv.body)); protocol.IsErrorReply(r) {
			t.Fatalf("add %s: %s", kv.id, r.ToBytes())
		}
	}
	// D1: (alpha beta) | gamma -> ab, ag, g = 3.
	r1 := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p3d", "alpha beta | gamma", "NOCONTENT", "DIALECT", "1"))
	if !searchTotalIs(t, r1, 3) {
		t.Fatalf("D1 precedence: want 3 (ab,ag,g), got %s", r1.ToBytes())
	}

	// Diagnostic: confirm alpha and gamma are individually indexed/searchable.
	// ab="alpha beta", ag="alpha gamma", g="gamma".
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p3d", "alpha", "NOCONTENT", "DIALECT", "2")); !searchTotalIs(t, r, 2) {
		t.Fatalf("alpha alone should match 2 (ab,ag), got %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p3d", "gamma", "NOCONTENT", "DIALECT", "2")); !searchTotalIs(t, r, 2) {
		t.Fatalf("gamma alone should match 2 (ag,g), got %s", r.ToBytes())
	}

	// D2: alpha (beta | gamma) -> ab, ag = 2. g excluded (no alpha).
	// The precedence flip is proven by D1=3 vs D2=2 (g drops out under D2).
	r2 := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p3d", "alpha beta | gamma", "NOCONTENT", "DIALECT", "2"))
	if !searchTotalIs(t, r2, 2) {
		t.Fatalf("D2 precedence: want 2 (ab,ag), got %s", r2.ToBytes())
	}
}
