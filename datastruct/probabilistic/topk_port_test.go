package probabilistic

import "testing"

// Same seed + same inputs must produce the same drop sequence (the property
// real Redis relies on for replication and AOF replay via TOPK._ADD_SEEDED).
func TestTopKSeededDeterminism(t *testing.T) {
	run := func(seed uint64) (drops []string) {
		tk := NewTopKOpts(3, 50, 4, 0.9)
		rng := seed
		for _, item := range []string{"a", "a", "b", "c", "d", "a", "e", "f", "g", "h"} {
			if name, ok := tk.AddSeeded([]byte(item), 1, &rng); ok {
				drops = append(drops, name)
			}
		}
		return drops
	}
	if d1, d2 := run(42), run(42); len(d1) == 0 || len(d1) != len(d2) {
		t.Fatalf("seeded runs diverge: %v vs %v", d1, d2)
	}
	// The probed 8.10 drop sequence for this input (heap eviction path).
	tk := NewTopKOpts(3, 50, 4, 0.9)
	rng := uint64(1)
	var got []string
	for _, item := range []string{"a", "a", "b", "c", "d", "a", "e", "f", "g", "h"} {
		if name, ok := tk.AddSeeded([]byte(item), 1, &rng); ok {
			got = append(got, name)
		}
	}
	want := []string{"b", "c", "d", "e", "f"}
	if len(got) != len(want) {
		t.Fatalf("drops %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("drops %v want %v", got, want)
		}
	}
	// MurmurHash2 reference vector (seed 0, "a").
	if h := murmurHash2([]byte("a"), 0); h != 0x92685f5e {
		t.Fatalf("murmur2(a,0)=%x", h)
	}
}
