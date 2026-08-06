package acl

import (
	"strings"
	"testing"
)

// TestCategoriesCoverage verifies every category's commands resolve back to it
// (no typos like the historical " xrange" leading space), and that newly added
// commands (GETDEL, SINTERCARD, HEXPIRE, FT.*, VS.*) are classified.
func TestCategoriesCoverage(t *testing.T) {
	// Typo regression: " xrange" (leading space) previously never matched.
	if !containsStr(GetCommandCategories("xrange"), "@stream") {
		t.Fatalf("xrange should be in @stream")
	}
	// New commands now in the table.
	for _, tc := range []struct{ cmd, want string }{
		{"getdel", "@string"}, {"getex", "@string"}, {"lcs", "@string"},
		{"sintercard", "@set"}, {"smismember", "@set"},
		{"hexpire", "@hash"}, {"hrandfield", "@hash"},
		{"zmpop", "@sortedset"}, {"zrangestore", "@sortedset"}, {"zmscore", "@sortedset"},
		{"lmove", "@list"}, {"lpos", "@list"}, {"blmpop", "@list"},
		{"geosearch", "@geo"}, {"xautoclaim", "@stream"}, {"xsetid", "@stream"},
		{"touch", "@keyspace"}, {"unlink", "@keyspace"}, {"expiretime", "@keyspace"},
		{"fcall", "@scripting"}, {"function", "@scripting"},
	} {
		if !containsStr(GetCommandCategories(tc.cmd), tc.want) {
			t.Fatalf("%s should be in %s, got %v", tc.cmd, tc.want, GetCommandCategories(tc.cmd))
		}
	}
	// Extension categories via prefix fallback.
	for _, tc := range []struct{ cmd, want string }{
		{"ft.search", "@search"}, {"ft.create", "@search"},
		{"json.set", "@json"}, {"vs.add", "@vector"}, {"ts.add", "@timeseries"},
		{"bf.add", "@bloom"}, {"cf.add", "@cuckoo"}, {"cms.incrby", "@cms"},
		{"topk.add", "@topk"}, {"td.add", "@tdigest"},
	} {
		if !containsStr(GetCommandCategories(tc.cmd), tc.want) {
			t.Fatalf("%s should be in %s, got %v", tc.cmd, tc.want, GetCommandCategories(tc.cmd))
		}
	}
}

func containsStr(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	_ = strings.ToLower // keep strings import if used elsewhere later
	return false
}
