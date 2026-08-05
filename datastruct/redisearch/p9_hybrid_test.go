package redisearch

import (
	"testing"
)

// TestP9CombineRRF verifies RRF fusion: a doc ranked high in BOTH lists should
// outrank a doc ranked high in only one.
func TestP9CombineRRF(t *testing.T) {
	text := []HybridHit{
		{DocID: "both", Rank: 0},
		{DocID: "text_only", Rank: 1},
	}
	vec := []HybridHit{
		{DocID: "both", Rank: 1},
		{DocID: "vec_only", Rank: 2},
	}
	cfg := HybridCombineConfig{Policy: HybridRRF, Constant: 60, Window: 10}
	res := CombineHybrid(text, vec, cfg)
	if len(res) != 3 {
		t.Fatalf("want 3 fused docs, got %d", len(res))
	}
	// "both": 1/60 + 1/61 ≈ 0.0328. "text_only": 1/61 ≈ 0.0164. "vec_only":
	// 1/62 ≈ 0.0161. So "both" must rank first.
	if res[0].DocID != "both" {
		t.Fatalf("RRF top should be 'both' (in both lists), got %s", res[0].DocID)
	}
	if res[0].FusedScore <= res[1].FusedScore {
		t.Fatalf("RRF: doc in both lists should outrank single-list docs: %+v", res)
	}
}

// TestP9CombineLinear verifies LINEAR fusion = alpha*text + beta*vec.
func TestP9CombineLinear(t *testing.T) {
	text := []HybridHit{{DocID: "d", Score: 0.8, Rank: 0}}
	vec := []HybridHit{{DocID: "d", Score: 0.4, Rank: 0}}
	cfg := HybridCombineConfig{Policy: HybridLinear, Alpha: 0.5, Beta: 0.5, Window: 10}
	res := CombineHybrid(text, vec, cfg)
	if len(res) != 1 {
		t.Fatalf("want 1 fused doc, got %d", len(res))
	}
	// 0.5*0.8 + 0.5*0.4 = 0.6
	if diff := res[0].FusedScore - 0.6; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("LINEAR fused score want 0.6, got %v", res[0].FusedScore)
	}
}

// TestP9NormalizeScores verifies min-max normalization to [0,1].
func TestP9NormalizeScores(t *testing.T) {
	hits := []HybridHit{
		{DocID: "lo", Score: 2},
		{DocID: "hi", Score: 10},
		{DocID: "mid", Score: 6},
	}
	out := NormalizeScores(hits)
	if out[0].Score != 0 || out[1].Score != 1 {
		t.Fatalf("normalize: lo should be 0, hi should be 1, got %v %v", out[0].Score, out[1].Score)
	}
	if diff := out[2].Score - 0.5; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("normalize: mid should be 0.5, got %v", out[2].Score)
	}
}
