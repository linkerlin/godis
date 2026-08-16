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

func TestSplitKNNClauseYieldDistanceAs(t *testing.T) {
	base, knn, err := SplitKNNClause("*=>[KNN 2 @vec $q EF_RUNTIME 10]=>{$YIELD_DISTANCE_AS: dist}")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if base != "*" || knn == nil {
		t.Fatalf("base=%q knn=%v", base, knn)
	}
	if knn.ScoreAs != "dist" || knn.K != 2 || knn.EFRuntime != 10 {
		t.Fatalf("clause=%+v", knn)
	}
	// Attribute overrides earlier AS.
	_, knn2, err := SplitKNNClause("*=>[KNN 1 @vec $q AS old]=>{$YIELD_DISTANCE_AS: neu}")
	if err != nil || knn2 == nil || knn2.ScoreAs != "neu" {
		t.Fatalf("override want neu, got %+v err=%v", knn2, err)
	}
}

func TestSplitKNNClauseHybridPolicy(t *testing.T) {
	_, knn, err := SplitKNNClause("@price:[0 10]=>[KNN 1 @vec $q HYBRID_POLICY ADHOC_BF]")
	if err != nil || knn == nil || knn.HybridPolicy != "ADHOC_BF" || knn.K != 1 {
		t.Fatalf("want ADHOC_BF, got %+v err=%v", knn, err)
	}
	_, knn2, err := SplitKNNClause("*=>[KNN 1 @vec $q HYBRID_POLICY BATCHES]")
	if err != nil || knn2 == nil || knn2.HybridPolicy != "BATCHES" {
		t.Fatalf("want BATCHES, got %+v err=%v", knn2, err)
	}
	_, knn3, err := SplitKNNClause("*=>[KNN 2 @vec $q]=>{$HYBRID_POLICY: BATCHES; $BATCH_SIZE: 8}")
	if err != nil || knn3 == nil || knn3.HybridPolicy != "BATCHES" || knn3.BatchSize != 8 {
		t.Fatalf("want attr HYBRID_POLICY BATCHES + BATCH_SIZE 8, got %+v err=%v", knn3, err)
	}
}

func TestSplitKNNClauseShardKRatioAccepted(t *testing.T) {
	_, knn, err := SplitKNNClause("*=>[KNN 1 @vec $q]=>{$SHARD_K_RATIO: 0.5}")
	if err != nil || knn == nil || knn.K != 1 {
		t.Fatalf("want accept SHARD_K_RATIO 0.5, got %+v err=%v", knn, err)
	}
	_, knn2, err := SplitKNNClause("*=>[KNN 2 @vec $q]=>{$SHARD_K_RATIO: 1}")
	if err != nil || knn2 == nil || knn2.K != 2 {
		t.Fatalf("want accept SHARD_K_RATIO 1, got %+v err=%v", knn2, err)
	}
}

func TestSplitKNNClauseEFRuntimeAttrAndBatchSize(t *testing.T) {
	_, knn, err := SplitKNNClause("*=>[KNN 1 @vec $q]=>{$EF_RUNTIME: 32}")
	if err != nil || knn == nil || knn.EFRuntime != 32 {
		t.Fatalf("want EF_RUNTIME 32 from attr, got %+v err=%v", knn, err)
	}
	_, knn2, err := SplitKNNClause("*=>[KNN 2 @vec $q HYBRID_POLICY BATCHES BATCH_SIZE 8]")
	if err != nil || knn2 == nil || knn2.BatchSize != 8 || knn2.HybridPolicy != "BATCHES" {
		t.Fatalf("want BATCH_SIZE 8, got %+v err=%v", knn2, err)
	}
	_, knn3, err := SplitKNNClause("*=>[KNN 1 @vec $q]=>{$BATCH_SIZE: 16; $EPSILON: 0.25}")
	if err != nil || knn3 == nil || knn3.BatchSize != 16 || knn3.Epsilon != 0.25 {
		t.Fatalf("want attr BATCH_SIZE+EPSILON, got %+v err=%v", knn3, err)
	}
}

func TestStripTrailingAttrBlock(t *testing.T) {
	base, attrs, err := StripTrailingAttrBlock("@vec:[VECTOR_RANGE 0.5 $q]=>{$YIELD_DISTANCE_AS: dist; $EPSILON: 0.1}")
	if err != nil || base != "@vec:[VECTOR_RANGE 0.5 $q]" {
		t.Fatalf("base=%q err=%v", base, err)
	}
	if attrs["YIELD_DISTANCE_AS"] != "dist" || attrs["EPSILON"] != "0.1" {
		t.Fatalf("attrs=%v", attrs)
	}
	base2, attrs2, err := StripTrailingAttrBlock("@price:[0 10]")
	if err != nil || attrs2 != nil || base2 != "@price:[0 10]" {
		t.Fatalf("passthrough failed: %q %v %v", base2, attrs2, err)
	}
}

func TestSplitKNNClauseBatchSizeAndEpsilon(t *testing.T) {
	_, knn, err := SplitKNNClause("*=>[KNN 2 @vec $q HYBRID_POLICY BATCHES BATCH_SIZE 16 EPSILON 0.25]")
	if err != nil || knn == nil {
		t.Fatalf("parse: %+v err=%v", knn, err)
	}
	if knn.BatchSize != 16 || knn.Epsilon != 0.25 || knn.HybridPolicy != "BATCHES" {
		t.Fatalf("want BatchSize=16 Epsilon=0.25 BATCHES, got %+v", knn)
	}
	_, knn2, err := SplitKNNClause("*=>[KNN 1 @vec $q]=>{$BATCH_SIZE: 8; $EPSILON: 0.5}")
	if err != nil || knn2 == nil || knn2.BatchSize != 8 || knn2.Epsilon != 0.5 {
		t.Fatalf("attr block: %+v err=%v", knn2, err)
	}
	// Garbage values accepted (Redis ponytail); fields stay zero/off.
	_, knn3, err := SplitKNNClause("*=>[KNN 1 @vec $q BATCH_SIZE abc EPSILON xyz]")
	if err != nil || knn3 == nil || knn3.BatchSize != 0 || knn3.Epsilon != 0 {
		t.Fatalf("garbage accept: %+v err=%v", knn3, err)
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
		{"*=>[KNN 1 @vec $q HYBRID_POLICY]", "HYBRID_POLICY requires"},
		{"*=>[KNN 1 @vec $q HYBRID_POLICY FOO]", "Invalid KNN HYBRID_POLICY"},
		{"*=>[KNN 1 @vec $q]=>{$YIELD_DISTANCE_AS:}", "YIELD_DISTANCE_AS requires"},
		{"*=>[KNN 1 @vec $q]=>{$SHARD_K_RATIO:}", "SHARD_K_RATIO requires"},
		{"*=>[KNN 1 @vec $q]=>{$SHARD_K_RATIO: 0}", "Invalid KNN SHARD_K_RATIO"},
		{"*=>[KNN 1 @vec $q]=>{$SHARD_K_RATIO: 1.5}", "Invalid KNN SHARD_K_RATIO"},
		{"*=>[KNN 1 @vec $q]=>{$SHARD_K_RATIO: abc}", "Invalid KNN SHARD_K_RATIO"},
		{"*=>[KNN 1 @vec $q BATCH_SIZE]", "BATCH_SIZE requires"},
		{"*=>[KNN 1 @vec $q EPSILON]", "EPSILON requires"},
		{"*=>[KNN 1 @vec $q]=>{$BATCH_SIZE:}", "BATCH_SIZE requires"},
		{"*=>[KNN 1 @vec $q]=>{$EPSILON:}", "EPSILON requires"},
		{"*=>[KNN 1 @vec $q]=>{$EF_RUNTIME:}", "EF_RUNTIME requires"},
		{"*=>[KNN 1 @vec $q]=>{$EF_RUNTIME: 0}", "Invalid KNN EF_RUNTIME"},
		{"*=>[KNN 1 @vec $q]=>{$HYBRID_POLICY:}", "HYBRID_POLICY requires"},
		{"*=>[KNN 1 @vec $q]=>{$HYBRID_POLICY: FOO}", "Invalid KNN HYBRID_POLICY"},
		{"*=>[KNN 1 @vec $q] leftover", "Unexpected token after KNN"},
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
