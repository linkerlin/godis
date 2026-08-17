package redisearch

import (
	"reflect"
	"strings"
	"testing"
)

func TestMatchedTermsApplySubset(t *testing.T) {
	fields := map[string]interface{}{
		"title": "red blue shoes",
		"body":  "ribbon",
	}
	got, err := EvalApplyExprWithQuery("matched_terms()", fields, []string{"red", "blue"})
	if err != nil {
		t.Fatalf("matched_terms: %v", err)
	}
	want := []string{"red", "blue"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("matched_terms want %v, got %#v", want, got)
	}

	// Only terms present in the document are returned (order preserved).
	got, err = EvalApplyExprWithQuery("matched_terms()", fields, []string{"blue", "green", "red"})
	if err != nil {
		t.Fatalf("matched_terms filter: %v", err)
	}
	want = []string{"blue", "red"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("matched_terms filter want %v, got %#v", want, got)
	}

	got, err = EvalApplyExprWithQuery("matched_terms()", fields, nil)
	if err != nil {
		t.Fatalf("matched_terms empty query: %v", err)
	}
	if s, ok := got.([]string); !ok || len(s) != 0 {
		t.Fatalf("empty query terms want [], got %#v", got)
	}

	got, err = EvalApplyExprWithQuery("matched_terms(1)", fields, []string{"red", "blue"})
	if err != nil {
		t.Fatalf("matched_terms(1): %v", err)
	}
	want = []string{"red"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("matched_terms(1) want %v, got %#v", want, got)
	}
}

func TestApplySplitMultiValue(t *testing.T) {
	got, err := EvalApplyExpr(`split("a, b, c")`, nil)
	if err != nil {
		t.Fatalf("split: %v", err)
	}
	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("split want %v, got %#v", want, got)
	}

	// Charset sep (not whole-string delimiter) + drop empty after strip.
	got, err = EvalApplyExpr(`split("a::b", ":")`, nil)
	if err != nil {
		t.Fatalf("split charset: %v", err)
	}
	want = []string{"a", "b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("split charset want %v, got %#v", want, got)
	}

	got, err = EvalApplyExpr(`split("a;b,c", ";,")`, nil)
	if err != nil {
		t.Fatalf("split multi-sep: %v", err)
	}
	want = []string{"a", "b", "c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("split multi-sep want %v, got %#v", want, got)
	}

	// Custom strip set (3rd arg); default strip is space only.
	got, err = EvalApplyExpr(`split("xa,xb", ",", "x")`, nil)
	if err != nil {
		t.Fatalf("split strip: %v", err)
	}
	want = []string{"a", "b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("split strip want %v, got %#v", want, got)
	}
}

func TestApplyFormatParseArgs(t *testing.T) {
	got, err := EvalApplyExpr(`format("Hi %s", "you")`, nil)
	if err != nil {
		t.Fatalf("format ok: %v", err)
	}
	if got != "Hi you" {
		t.Fatalf("format ok want Hi you, got %#v", got)
	}
	_, err = EvalApplyExpr(`format("bad %")`, nil)
	if err == nil || !strings.Contains(err.Error(), "SEARCH_PARSE_ARGS Bad format string!") {
		t.Fatalf("trailing %%: %v", err)
	}
	_, err = EvalApplyExpr(`format("%s")`, nil)
	if err == nil || !strings.Contains(err.Error(), "SEARCH_PARSE_ARGS Not enough arguments for format") {
		t.Fatalf("missing arg: %v", err)
	}
	_, err = EvalApplyExpr(`format("%d", 1)`, nil)
	if err == nil || !strings.Contains(err.Error(), "SEARCH_PARSE_ARGS Unknown format specifier passed") {
		t.Fatalf("bad specifier: %v", err)
	}
}

func TestExtractQueryTermsSplitsOperators(t *testing.T) {
	got := extractQueryTerms("red|blue shoes")
	want := []string{"red", "blue", "shoes"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("extractQueryTerms want %v, got %v", want, got)
	}
}
