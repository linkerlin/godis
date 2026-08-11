package redisearch

import (
	"strings"
	"testing"
)

func TestSplitKNNClauseHappy(t *testing.T) {
	base, knn, err := SplitKNNClause("*=>[KNN 3 @vec $q AS dist EF_RUNTIME 40]")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if base != "*" || knn == nil {
		t.Fatalf("base=%q knn=%v", base, knn)
	}
	if knn.K != 3 || knn.Field != "vec" || knn.Param != "$q" || knn.ScoreAs != "dist" || knn.EFRuntime != 40 {
		t.Fatalf("clause=%+v", knn)
	}
}

func TestSplitKNNClauseNoMarker(t *testing.T) {
	base, knn, err := SplitKNNClause("@price:[0 10]")
	if err != nil || knn != nil || base != "@price:[0 10]" {
		t.Fatalf("want passthrough, got base=%q knn=%v err=%v", base, knn, err)
	}
}

func TestSplitKNNClauseErrorPaths(t *testing.T) {
	cases := []struct {
		q   string
		sub string
	}{
		{"*=>[KNN 1 @vec $q", "Invalid KNN clause syntax"},
		{"*=>[KNN 0 @vec $q]", "Invalid KNN K"},
		{"*=>[KNN 1 @vec q]", "must be a $parameter"},
		{"*=>[KNN 1 @vec]", "requires K, @field, and $param"},
		{"*=>[KNN 1 @vec $q EXTRA]", "Unexpected KNN token"},
		{"*=>[KNN 1 @vec $q EF_RUNTIME]", "EF_RUNTIME requires"},
		{"*=>[KNN 1 @vec $q EF_RUNTIME -1]", "Invalid KNN EF_RUNTIME"},
		{"*=>[KNN 1 @vec $q AS]", "AS requires"},
	}
	for _, tc := range cases {
		_, knn, err := SplitKNNClause(tc.q)
		if err == nil || knn != nil {
			t.Fatalf("%q: want error, got knn=%v err=%v", tc.q, knn, err)
		}
		if !strings.Contains(err.Error(), tc.sub) {
			t.Fatalf("%q: want %q in %v", tc.q, tc.sub, err)
		}
	}
}
