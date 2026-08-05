package redisearch

import (
	"math"
	"sort"
)

// HybridCombinePolicy selects how text and vector ranked lists are fused.
type HybridCombinePolicy int

const (
	HybridRRF HybridCombinePolicy = iota
	HybridLinear
)

// HybridHit is one document's contribution from either the text or vector side
// of a hybrid query. Score is the raw relevance (text BM25 score or vector
// similarity); Rank is the 0-based position in that side's ranked list.
type HybridHit struct {
	DocID string
	Score float64
	Rank  int
}

// HybridCombineConfig parameterizes the fusion. RRF: score(d) = Σ 1/(Constant +
// rank_i(d)), default Constant 60. LINEAR: score(d) = Alpha*text + Beta*vec.
// Window caps how deep into each list to consider (default 20).
type HybridCombineConfig struct {
	Policy   HybridCombinePolicy
	Constant float64 // RRF only
	Alpha    float64 // LINEAR: text weight
	Beta     float64 // LINEAR: vector weight
	Window   int
}

// DefaultHybridCombineConfig is the Redis 8.4 default: RRF, constant 60, window 20.
func DefaultHybridCombineConfig() HybridCombineConfig {
	return HybridCombineConfig{
		Policy:   HybridRRF,
		Constant: 60,
		Window:   20,
	}
}

// HybridResult is one fused document.
type HybridResult struct {
	DocID      string
	FusedScore float64
	TextScore  float64 // 0 if the doc wasn't in the text list
	VecScore   float64 // 0 if the doc wasn't in the vector list
}

// CombineHybrid fuses a text ranked list and a vector ranked list into a single
// ranked list according to cfg. Docs appearing in only one list still score
// (the missing side contributes 0 for LINEAR, or nothing for RRF). The result
// is sorted by descending fused score.
func CombineHybrid(text, vec []HybridHit, cfg HybridCombineConfig) []HybridResult {
	if cfg.Window <= 0 {
		cfg.Window = 20
	}
	// Apply window: only the top-Window from each side contribute.
	if len(text) > cfg.Window {
		text = text[:cfg.Window]
	}
	if len(vec) > cfg.Window {
		vec = vec[:cfg.Window]
	}

	textBy := make(map[string]HybridHit, len(text))
	for _, h := range text {
		textBy[h.DocID] = h
	}
	vecBy := make(map[string]HybridHit, len(vec))
	for _, h := range vec {
		vecBy[h.DocID] = h
	}

	// Union of doc ids.
	ids := make(map[string]struct{}, len(textBy)+len(vecBy))
	for id := range textBy {
		ids[id] = struct{}{}
	}
	for id := range vecBy {
		ids[id] = struct{}{}
	}

	results := make([]HybridResult, 0, len(ids))
	for id := range ids {
		th, inText := textBy[id]
		vh, inVec := vecBy[id]
		var fused float64
		switch cfg.Policy {
		case HybridRRF:
			if inText {
				fused += 1.0 / (cfg.Constant + float64(th.Rank))
			}
			if inVec {
				fused += 1.0 / (cfg.Constant + float64(vh.Rank))
			}
		case HybridLinear:
			ts := 0.0
			if inText {
				ts = th.Score
			}
			vs := 0.0
			if inVec {
				vs = vh.Score
			}
			fused = cfg.Alpha*ts + cfg.Beta*vs
		}
		results = append(results, HybridResult{
			DocID:      id,
			FusedScore: fused,
			TextScore:  th.Score,
			VecScore:   vh.Score,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].FusedScore != results[j].FusedScore {
			return results[i].FusedScore > results[j].FusedScore
		}
		return results[i].DocID < results[j].DocID
	})
	return results
}

// NormalizeScores scales a hit list so the maximum score is 1.0 (min-max to
// [0,1]). Used before LINEAR fusion so text and vector scores are comparable.
// Empty or constant lists are a no-op.
func NormalizeScores(hits []HybridHit) []HybridHit {
	if len(hits) == 0 {
		return hits
	}
	minS, maxS := math.MaxFloat64, -math.MaxFloat64
	for _, h := range hits {
		if h.Score < minS {
			minS = h.Score
		}
		if h.Score > maxS {
			maxS = h.Score
		}
	}
	if maxS == minS {
		return hits
	}
	out := make([]HybridHit, len(hits))
	for i, h := range hits {
		out[i] = HybridHit{DocID: h.DocID, Rank: h.Rank, Score: (h.Score - minS) / (maxS - minS)}
	}
	return out
}
