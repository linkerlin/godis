package redisearch

import (
	"reflect"
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
}

func TestExtractQueryTermsSplitsOperators(t *testing.T) {
	got := extractQueryTerms("red|blue shoes")
	want := []string{"red", "blue", "shoes"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("extractQueryTerms want %v, got %v", want, got)
	}
}
