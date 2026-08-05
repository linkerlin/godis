package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestP1aHDelTriggersReindex verifies HDEL on an indexed hash field updates the
// FT document: deleting the only field holding a term must remove the doc from
// search results. Before the fix, HDEL never called reindexHash, so stale docs
// stayed searchable.
func TestP1aHDelTriggersReindex(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p1a", "ON", "HASH", "PREFIX", "1", "p1a:", "SCHEMA", "t", "TEXT",
	)), "OK")

	// Hash carries the term in two fields; HDEL one must not drop the doc.
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p1a:1", "t", "hello", "extra", "hello")); protocol.IsErrorReply(r) {
		t.Fatalf("hset: %s", r.ToBytes())
	}
	// Sanity: searchable.
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p1a", "hello", "NOCONTENT")); !searchTotalIs(t, r, 1) {
		t.Fatalf("before del: %s", r.ToBytes())
	}

	// Delete the indexed field entirely -> doc must no longer match "hello".
	if r := db.Exec(nil, utils.ToCmdLine("HDEL", "p1a:1", "t")); protocol.IsErrorReply(r) {
		t.Fatalf("hdel: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p1a", "hello", "NOCONTENT")); !searchTotalIs(t, r, 0) {
		t.Fatalf("after del of t: %s", r.ToBytes())
	}

	// extra still holds "hello" in the same doc? Reindex must still reflect it.
	// (extra is not in the SCHEMA, so it is not indexed — total stays 0. This
	// confirms reindex honors the schema, not just any field.)
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p1a:1", "t", "world")); protocol.IsErrorReply(r) {
		t.Fatalf("hset2: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p1a", "world", "NOCONTENT")); !searchTotalIs(t, r, 1) {
		t.Fatalf("after re-add: %s", r.ToBytes())
	}
}

// TestP1cSpellCheckUsesTermsDicts verifies FT.SPELLCHECK INCLUDE/EXCLUDE
// dictionaries are actually consumed: INCLUDE adds candidates from a user dict
// even when they are absent from the index; EXCLUDE suppresses a suggestion.
func TestP1cSpellCheckUsesTermsDicts(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p1c", "SCHEMA", "t", "TEXT",
	)), "OK")
	// Index holds "hello"; query for "hallo" should suggest "hello".
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p1c", "d1", "FIELDS", "t", "hello")); protocol.IsErrorReply(r) {
		t.Fatalf("add: %s", r.ToBytes())
	}

	// Baseline: "hallo" -> "hello" suggested from the index dictionary.
	r := db.Exec(nil, utils.ToCmdLine("FT.SPELLCHECK", "p1c", "hallo"))
	if !spellSuggests(t, r, "hello") {
		t.Fatalf("baseline spellcheck should suggest hello: %s", r.ToBytes())
	}

	// Build a user dict containing "hallo" itself; INCLUDE it so "hallo" becomes
	// a candidate suggestion for the misspelling "hallo"->"hallo" (distance 0,
	// excluded by self-skip). Instead include a near-synonym "hullo" that is NOT
	// in the index, and confirm it surfaces only when INCLUDE is supplied.
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("FT.DICTADD", "syns", "hullo")), 1)

	r = db.Exec(nil, utils.ToCmdLine("FT.SPELLCHECK", "p1c", "hallo", "TERMS", "INCLUDE", "syns"))
	if !spellSuggests(t, r, "hullo") {
		t.Fatalf("INCLUDE dict should surface hullo: %s", r.ToBytes())
	}

	// Now EXCLUDE a dict that lists "hello": the index-derived suggestion must
	// disappear.
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("FT.DICTADD", "block", "hello")), 1)
	r = db.Exec(nil, utils.ToCmdLine("FT.SPELLCHECK", "p1c", "hallo", "TERMS", "EXCLUDE", "block"))
	if spellSuggests(t, r, "hello") {
		t.Fatalf("EXCLUDE dict should suppress hello: %s", r.ToBytes())
	}
}

// TestP1dProfileReportsHonestTotals verifies FT.PROFILE no longer emits the
// fabricated "Parsing time"/"Iterators profile" fields and instead reports an
// honest "Total profile time" + "Result count".
func TestP1dProfileReportsHonestTotals(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p1d", "SCHEMA", "t", "TEXT",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p1d", "d1", "FIELDS", "t", "hello")); protocol.IsErrorReply(r) {
		t.Fatalf("add: %s", r.ToBytes())
	}

	r := db.Exec(nil, utils.ToCmdLine("FT.PROFILE", "p1d", "SEARCH", "hello"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) < 2 {
		t.Fatalf("profile shape: %T %s", r, r.ToBytes())
	}
	prof := string(mr.Replies[1].ToBytes())
	if !strings.Contains(prof, "Total profile time") || !strings.Contains(prof, "Result count") {
		t.Fatalf("profile missing honest fields: %s", prof)
	}
	if strings.Contains(prof, "Parsing time") || strings.Contains(prof, "Iterators profile") {
		t.Fatalf("profile still emits fabricated fields: %s", prof)
	}
}

// TestP1eMinPrefixRejectsShortPrefix verifies FT.CONFIG MINPREFIX is read by
// the query engine: a prefix shorter than MINPREFIX must error.
func TestP1eMinPrefixRejectsShortPrefix(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p1e", "SCHEMA", "t", "TEXT",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p1e", "d1", "FIELDS", "t", "hello")); protocol.IsErrorReply(r) {
		t.Fatalf("add: %s", r.ToBytes())
	}
	// Default MINPREFIX=2: a 1-char prefix must be rejected.
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MINPREFIX", "2")), "OK")
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p1e", "h*"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "MINPREFIX") {
		t.Fatalf("short prefix should be rejected by MINPREFIX: %s", r.ToBytes())
	}
	// 2-char prefix is allowed.
	r = db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p1e", "he*"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("2-char prefix should pass: %s", r.ToBytes())
	}
	// Reset for other tests.
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MINPREFIX", "2")), "OK")
}

// TestP1eMaxExpansionsCapsPrefix verifies FT.CONFIG MAXEXPANSIONS is enforced:
// a prefix that expands to more terms than the limit must error.
func TestP1eMaxExpansionsCapsPrefix(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p1emax", "SCHEMA", "t", "TEXT",
	)), "OK")
	// Index 5 terms sharing the prefix "sh".
	for _, w := range []string{"ship", "shop", "shot", "shed", "shin"} {
		if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p1emax", "d_"+w, "FIELDS", "t", w)); protocol.IsErrorReply(r) {
			t.Fatalf("add %s: %s", w, r.ToBytes())
		}
	}
	// Set MAXEXPANSIONS=3: "sh*" expands to 5 terms -> must error.
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MAXEXPANSIONS", "3")), "OK")
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p1emax", "sh*"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "expansion") {
		t.Fatalf("over-limit prefix should error: %s", r.ToBytes())
	}
	// Restore defaults so other tests are not affected.
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MAXEXPANSIONS", "200")), "OK")
}

// searchTotalIs reports whether a FT.SEARCH reply's total hit count equals want.
func searchTotalIs(t *testing.T, r redis.Reply, want int64) bool {
	t.Helper()
	if r == nil {
		return false
	}
	switch rep := r.(type) {
	case *protocol.MultiRawReply:
		if len(rep.Replies) > 0 {
			if ir, ok := rep.Replies[0].(*protocol.IntReply); ok {
				return ir.Code == want
			}
		}
	case *protocol.MultiBulkReply:
		if len(rep.Args) > 0 {
			s := string(rep.Args[0])
			return strings.TrimSpace(s) == intToStr(want)
		}
	}
	return false
}

// spellSuggests reports whether a FT.SPELLCHECK reply mentions the given term
// as a suggestion.
func spellSuggests(t *testing.T, r redis.Reply, term string) bool {
	t.Helper()
	if r == nil {
		return false
	}
	return strings.Contains(string(r.ToBytes()), term)
}

// TestP1gNoOffsetsDisablesPhrase verifies FT.CREATE NOOFFSETS drops position
// data so phrase queries (quoted "a b") can no longer match.
func TestP1gNoOffsetsDisablesPhrase(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p1gno", "NOOFFSETS", "SCHEMA", "t", "TEXT",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p1gno", "d1", "FIELDS", "t", "hello world")); protocol.IsErrorReply(r) {
		t.Fatalf("add: %s", r.ToBytes())
	}
	// A plain AND query still matches.
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p1gno", "hello world", "NOCONTENT")); !searchTotalIs(t, r, 1) {
		t.Fatalf("AND query should match: %s", r.ToBytes())
	}
	// A phrase query relies on positions, which NOOFFSETS removed -> 0 hits.
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p1gno", "\"hello world\"", "NOCONTENT")); !searchTotalIs(t, r, 0) {
		t.Fatalf("phrase should not match under NOOFFSETS: %s", r.ToBytes())
	}
}

// TestP1gNoFieldsDisablesFieldScope verifies NOFIELDS suppresses the
// field-prefixed term copy, so @field: scoping no longer filters.
func TestP1gNoFieldsDisablesFieldScope(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p1gnf", "NOFIELDS", "SCHEMA", "t", "TEXT", "u", "TEXT",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p1gnf", "d1", "FIELDS", "t", "apple", "u", "banana")); protocol.IsErrorReply(r) {
		t.Fatalf("add: %s", r.ToBytes())
	}
	// With NOFIELDS, @u:apple still matches because field attribution is gone
	// and the term exists somewhere in the doc.
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p1gnf", "@u:apple", "NOCONTENT"))
	if !searchTotalIs(t, r, 1) {
		t.Fatalf("NOFIELDS should make @u:apple match (field scope ignored): %s", r.ToBytes())
	}
}

// TestP1gIndexLevelOptionsAccepted verifies the remaining index-level options
// parse without error and surface in FT.INFO: NOHL, MAXTEXTFIELDS, TEMPORARY,
// FILTER, INDEXALL.
func TestP1gIndexLevelOptionsAccepted(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p1gopt", "NOHL", "MAXTEXTFIELDS", "TEMPORARY", "60",
		"FILTER", "@__key", "INDEXALL", "ENABLE", "SCHEMA", "t", "TEXT",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	searchEnginesMu.RLock()
	engine := searchEngines["p1gopt"]
	searchEnginesMu.RUnlock()
	if engine == nil {
		t.Fatal("engine missing")
	}
	info := engine.Info()
	if info["no_highlight"] != true {
		t.Errorf("no_highlight not set: %v", info["no_highlight"])
	}
	if info["max_text_fields"] != true {
		t.Errorf("max_text_fields not set: %v", info["max_text_fields"])
	}
	if info["temporary"] != 60 {
		t.Errorf("temporary not set: %v", info["temporary"])
	}
	if info["filter"] != "@__key" {
		t.Errorf("filter not set: %v", info["filter"])
	}
	if info["index_all"] != "ENABLE" {
		t.Errorf("index_all not set: %v", info["index_all"])
	}
}

func intToStr(n int64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// TestP1iSuffixSearch verifies *suffix wildcard finds terms ending with the
// given suffix. Uses NOSTEM so indexed terms keep their full form (stemming
// would reduce "running" -> "run" and defeat the suffix test).
func TestP1iSuffixSearch(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p1isuf", "SCHEMA", "t", "TEXT", "NOSTEM",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	for _, w := range []string{"running", "swimming", "walking"} {
		if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p1isuf", "d_"+w, "FIELDS", "t", w)); protocol.IsErrorReply(r) {
			t.Fatalf("add %s: %s", w, r.ToBytes())
		}
	}
	// *ing should match all three.
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p1isuf", "*ing", "NOCONTENT")); !searchTotalIs(t, r, 3) {
		t.Fatalf("*ing suffix should match 3 docs: %s", r.ToBytes())
	}
}

// TestP1iInfixSearch verifies *infix* wildcard finds terms containing the
// given substring.
func TestP1iInfixSearch(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p1iinf", "SCHEMA", "t", "TEXT", "NOSTEM",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	for _, w := range []string{"foobar", "barbaz", "quxfoo"} {
		if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p1iinf", "d_"+w, "FIELDS", "t", w)); protocol.IsErrorReply(r) {
			t.Fatalf("add %s: %s", w, r.ToBytes())
		}
	}
	// *foo* should match foobar and quxfoo (2 docs containing "foo").
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p1iinf", "*foo*", "NOCONTENT")); !searchTotalIs(t, r, 2) {
		t.Fatalf("*foo* infix should match 2 docs: %s", r.ToBytes())
	}
}

// TestP1jFuzzyDistances verifies %t% (dist 1), %%t%% (dist 2). Uses NOSTEM so
// indexed terms are not reduced before Levenshtein comparison.
func TestP1jFuzzyDistances(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p1jfuz", "SCHEMA", "t", "TEXT", "NOSTEM",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	// hallo (base, dist 0), hello (dist 1: a->e), helly (dist 2: a->e, o->y).
	for _, w := range []string{"hello", "hallo", "helly"} {
		if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p1jfuz", "d_"+w, "FIELDS", "t", w)); protocol.IsErrorReply(r) {
			t.Fatalf("add %s: %s", w, r.ToBytes())
		}
	}
	// %hallo% (dist 1): matches "hallo" (dist 0) and "hello" (dist 1).
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p1jfuz", "%hallo%", "NOCONTENT")); !searchTotalIs(t, r, 2) {
		t.Fatalf("%%hallo%% (dist 1) should match 2: %s", r.ToBytes())
	}
	// %%hallo%% (dist 2): also picks up "helly" (dist 2 from "hallo").
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p1jfuz", "%%hallo%%", "NOCONTENT")); !searchTotalIs(t, r, 3) {
		t.Fatalf("%%%%hallo%%%% (dist 2) should match 3: %s", r.ToBytes())
	}
}

// TestP1fCaseSensitiveTag verifies a TAG field declared CASESENSITIVE keeps
// the original case in the index (the indexed tag token is not lowercased).
func TestP1fCaseSensitiveTag(t *testing.T) {
	db := makeTestDB()
	// CASESENSITIVE TAG: "Foo" stays "Foo".
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p1fcs", "SCHEMA", "tag", "TAG", "CASESENSITIVE",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p1fcs", "d1", "FIELDS", "tag", "Foo")); protocol.IsErrorReply(r) {
		t.Fatalf("add: %s", r.ToBytes())
	}
	// Inspect the raw index terms to confirm case was preserved.
	searchEnginesMu.RLock()
	engine := searchEngines["p1fcs"]
	searchEnginesMu.RUnlock()
	if engine == nil {
		t.Fatal("engine missing")
	}
	tags := engine.TagVals("tag")
	found := false
	for _, tv := range tags {
		if tv == "Foo" {
			found = true
		}
	}
	if !found {
		t.Fatalf("CASESENSITIVE tag should preserve 'Foo', got %v", tags)
	}
}

// TestP1fIndexEmpty verifies INDEXEMPTY makes empty-valued TEXT docs findable.
func TestP1fIndexEmpty(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p1fie", "SCHEMA", "t", "TEXT", "INDEXEMPTY",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	// A doc whose INDEXEMPTY field is the empty string.
	if r := db.Exec(nil, utils.ToCmdLine("FT.ADD", "p1fie", "d1", "FIELDS", "t", "")); protocol.IsErrorReply(r) {
		t.Fatalf("add empty: %s", r.ToBytes())
	}
	// The empty-token marker should be present in the term dictionary.
	searchEnginesMu.RLock()
	engine := searchEngines["p1fie"]
	searchEnginesMu.RUnlock()
	if engine == nil || !engine.TermExists("t:\x00empty") {
		t.Fatalf("INDEXEMPTY marker missing")
	}
}

// TestP1fGeoShapeAccepted verifies the GEOSHAPE field type parses, with
// optional FLAT / SPHERICAL coordinate system.
func TestP1fGeoShapeAccepted(t *testing.T) {
	db := makeTestDB()
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"default", []string{"geom", "GEOSHAPE"}},
		{"flat", []string{"geom", "GEOSHAPE", "FLAT"}},
		{"spherical", []string{"geom", "GEOSHAPE", "SPHERICAL"}},
	} {
		idxName := "p1fgs_" + tc.name
		cmd := append([]string{"FT.CREATE", idxName, "SCHEMA"}, tc.args...)
		if r := db.Exec(nil, utils.ToCmdLine(cmd...)); protocol.IsErrorReply(r) {
			t.Fatalf("%s create: %s", tc.name, r.ToBytes())
		}
	}
}

// TestP1fPhoneticMatcherValidated verifies PHONETIC rejects unknown matchers and
// accepts the four supported ones, and that PHONETIC is TEXT-only.
func TestP1fPhoneticMatcherValidated(t *testing.T) {
	db := makeTestDB()
	for _, m := range []string{"dm:en", "dm:fr", "dm:pt", "dm:es"} {
		if r := db.Exec(nil, utils.ToCmdLine("FT.CREATE", "p1fph_"+m, "SCHEMA", "t", "TEXT", "PHONETIC", m)); protocol.IsErrorReply(r) {
			t.Fatalf("PHONETIC %s should be accepted: %s", m, r.ToBytes())
		}
	}
	// Invalid matcher rejected.
	if r := db.Exec(nil, utils.ToCmdLine("FT.CREATE", "p1fbad", "SCHEMA", "t", "TEXT", "PHONETIC", "dm:xx")); !protocol.IsErrorReply(r) {
		t.Fatalf("PHONETIC dm:xx should be rejected: %s", r.ToBytes())
	}
	// PHONETIC on TAG rejected.
	if r := db.Exec(nil, utils.ToCmdLine("FT.CREATE", "p1fbad2", "SCHEMA", "t", "TAG", "PHONETIC", "dm:en")); !protocol.IsErrorReply(r) {
		t.Fatalf("PHONETIC on TAG should be rejected: %s", r.ToBytes())
	}
}

// TestP1fSortableUNF verifies SORTABLE UNF parses and stores the UNF flag.
func TestP1fSortableUNF(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p1funf", "SCHEMA", "t", "TEXT", "SORTABLE", "UNF",
	)); protocol.IsErrorReply(r) {
		t.Fatalf("create: %s", r.ToBytes())
	}
	searchEnginesMu.RLock()
	engine := searchEngines["p1funf"]
	searchEnginesMu.RUnlock()
	if engine == nil {
		t.Fatal("engine missing")
	}
	// Inspect the schema field to confirm SortableUNF stuck.
	info := engine.Info()
	attrs, _ := info["attributes"].([]map[string]interface{})
	hit := false
	for _, m := range attrs {
		if m["name"] == "t" || m["identifier"] == "t" {
			if m["sortable"] == true && m["sortable_unf"] == true {
				hit = true
			}
		}
	}
	if !hit {
		t.Fatalf("SORTABLE UNF not reflected in FT.INFO attributes: %v", attrs)
	}
}
