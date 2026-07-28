package vector

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func TestHNSWInsertSearchRecall(t *testing.T) {
	const (
		n   = 500
		dim = 32
		k   = 10
	)
	rng := rand.New(rand.NewSource(42))
	vs := NewVectorSet()
	vs.ConfigureHNSW(16, 100)

	vecs := make([]*Vector, n)
	for i := 0; i < n; i++ {
		data := make([]float32, dim)
		for d := 0; d < dim; d++ {
			data[d] = rng.Float32()*2 - 1
		}
		vecs[i] = NewVector(data)
		if !vs.Add(fmt.Sprintf("e%d", i), vecs[i], nil) {
			t.Fatalf("add e%d failed", i)
		}
	}

	// Probe several queries; compare HNSW (forced, N>64) vs brute TRUTH.
	hits := 0
	total := 0
	for q := 0; q < 20; q++ {
		query := vecs[q*17%n]
		exact := vs.SearchWithMetricEF(query, k, CosineSimilarity, 0, true)
		approx := vs.SearchWithMetricEF(query, k, CosineSimilarity, 200, false)
		if len(exact) == 0 || len(approx) == 0 {
			t.Fatalf("empty results q=%d exact=%d approx=%d", q, len(exact), len(approx))
		}
		exactSet := make(map[string]struct{}, len(exact))
		for _, r := range exact {
			exactSet[r.ID] = struct{}{}
		}
		for _, r := range approx {
			total++
			if _, ok := exactSet[r.ID]; ok {
				hits++
			}
		}
	}
	recall := float64(hits) / float64(total)
	if recall < 0.90 {
		t.Fatalf("HNSW recall@%d = %.3f, want >= 0.90 (hits=%d total=%d)", k, recall, hits, total)
	}
}

func TestHNSWDeleteAndLinks(t *testing.T) {
	vs := NewVectorSet()
	vs.ConfigureHNSW(8, 50)
	a := NewVector([]float32{1, 0, 0})
	b := NewVector([]float32{0.9, 0.1, 0})
	c := NewVector([]float32{0, 1, 0})
	vs.Add("a", a, nil)
	vs.Add("b", b, nil)
	vs.Add("c", c, nil)

	layers, ok := vs.HNSWLinks("a")
	if !ok || len(layers) == 0 {
		t.Fatalf("expected links for a, ok=%v layers=%v", ok, layers)
	}
	if len(layers[0]) == 0 {
		t.Fatalf("expected layer-0 neighbors for a after inserts")
	}

	if !vs.Delete("b") {
		t.Fatal("delete b failed")
	}
	if _, found := vs.Get("b"); found {
		t.Fatal("b still present")
	}
	layers, ok = vs.HNSWLinks("b")
	if ok {
		t.Fatal("b should not be in HNSW after delete")
	}
	// Remaining nodes should still search.
	res := vs.SearchWithMetricEF(a, 2, CosineSimilarity, 20, false)
	if len(res) == 0 {
		t.Fatal("search after delete returned empty")
	}
	for _, r := range res {
		if r.ID == "b" {
			t.Fatal("deleted id leaked into search results")
		}
	}
}

func TestHNSWInfoParams(t *testing.T) {
	vs := NewVectorSet()
	vs.ConfigureHNSW(12, 80)
	vs.Add("x", NewVector([]float32{1, 0}), nil)
	m, ef, uid, maxLevel := vs.HNSWInfo()
	if m != 12 {
		t.Fatalf("m=%d want 12", m)
	}
	if ef != 80 {
		t.Fatalf("ef=%d want 80", ef)
	}
	if uid == 0 {
		t.Fatal("expected non-zero max node uid")
	}
	if maxLevel < 0 {
		t.Fatalf("maxLevel=%d", maxLevel)
	}
}

func TestHNSWCosineTopHit(t *testing.T) {
	vs := NewVectorSet()
	vs.Add("a", NewVector([]float32{1, 0, 0}), nil)
	vs.Add("b", NewVector([]float32{0, 1, 0}), nil)
	vs.Add("c", NewVector([]float32{0.99, 0.01, 0}), nil)
	q := NewVector([]float32{1, 0, 0})
	res := vs.SearchWithMetric(q, 2, CosineSimilarity)
	if len(res) < 1 || res[0].ID != "a" {
		t.Fatalf("top hit want a, got %#v", res)
	}
	if math.Abs(float64(res[0].Score-1)) > 1e-5 {
		t.Fatalf("self-similar score=%v", res[0].Score)
	}
}
