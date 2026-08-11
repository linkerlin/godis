package stream

import (
	"fmt"
	"testing"
	"time"
)

// TestOrderedRangeCorrectness verifies Range/ReverseRange over an ordered store
// returns correct, sorted results (the previous implementation scanned the
// whole map and bubble-sorted).
func TestOrderedRangeCorrectness(t *testing.T) {
	s := NewStream()
	// Add 100 entries with monotonically increasing IDs.
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("%d-%d", 1000+i, 0)
		if _, err := s.Add(id, map[string]string{"n": fmt.Sprint(i)}, nil); err != nil {
			t.Fatalf("add %s: %v", id, err)
		}
	}
	start := StreamID{Timestamp: 1000, Sequence: 0}
	end := StreamID{Timestamp: 1050, Sequence: 0}

	r := s.Range(start, end)
	if len(r) != 51 { // 1000..1050 inclusive
		t.Fatalf("Range want 51 entries, got %d", len(r))
	}
	// Must be ascending.
	for i := 1; i < len(r); i++ {
		if r[i].ID.Compare(r[i-1].ID) <= 0 {
			t.Fatalf("Range not ascending at %d", i)
		}
	}

	rev := s.ReverseRange(start, end, 0)
	if len(rev) != 51 {
		t.Fatalf("ReverseRange want 51 entries, got %d", len(rev))
	}
	for i := 1; i < len(rev); i++ {
		if rev[i].ID.Compare(rev[i-1].ID) >= 0 {
			t.Fatalf("ReverseRange not descending at %d", i)
		}
	}
	// Count-limited reverse.
	rev5 := s.ReverseRange(start, end, 5)
	if len(rev5) != 5 {
		t.Fatalf("ReverseRange count=5 want 5, got %d", len(rev5))
	}
	if rev5[0].ID.Compare(end) != 0 {
		t.Fatalf("ReverseRange should start at end, got %s", rev5[0].ID)
	}
}

// TestOrderedTrim verifies MAXLEN and MINID trimming keep the newest entries.
func TestOrderedTrim(t *testing.T) {
	s := NewStream()
	for i := 0; i < 50; i++ {
		id := fmt.Sprintf("%d-0", 2000+i)
		if _, err := s.Add(id, map[string]string{"n": fmt.Sprint(i)}, nil); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	// MAXLEN 10: keep the 10 newest (2040..2049).
	s.Trim(&AddOptions{HasMaxLen: true, MaxLen: 10, MaxLenApprox: false})
	if s.Len() != 10 {
		t.Fatalf("after MAXLEN trim want 10, got %d", s.Len())
	}
	first := s.Range(StreamID{Timestamp: 0}, StreamID{Timestamp: 1 << 40})[0]
	if first.ID.Timestamp != 2040 {
		t.Fatalf("oldest kept should be 2040, got %s", first.ID)
	}

	// MINID 2045: drop everything < 2045.
	s.Trim(&AddOptions{MinID: StreamID{Timestamp: 2045, Sequence: 0}})
	if s.Len() != 5 {
		t.Fatalf("after MINID trim want 5 (2045..2049), got %d", s.Len())
	}
}

// TestOrderedDelete verifies Delete removes from both the lookup dict and the
// ordered slice, keeping subsequent Ranges consistent.
func TestOrderedDelete(t *testing.T) {
	s := NewStream()
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("%d-0", 3000+i)
		if _, err := s.Add(id, map[string]string{}, nil); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	// Delete middle + head entries.
	del := []StreamID{
		{Timestamp: 3001, Sequence: 0},
		{Timestamp: 3010, Sequence: 0},
		{Timestamp: 3000, Sequence: 0},
	}
	if n := s.Delete(del); n != 3 {
		t.Fatalf("Delete want 3, got %d", n)
	}
	if s.Len() != 17 {
		t.Fatalf("Len want 17, got %d", s.Len())
	}
	// Deleted entries unreachable.
	if s.GetEntry(StreamID{Timestamp: 3010, Sequence: 0}) != nil {
		t.Fatalf("deleted entry should be gone from dict")
	}
	// Range must be contiguous and ascending (no holes ordering break).
	r := s.Range(StreamID{Timestamp: 3000}, StreamID{Timestamp: 3019})
	if len(r) != 17 {
		t.Fatalf("Range want 17, got %d", len(r))
	}
	for i := 1; i < len(r); i++ {
		if r[i].ID.Compare(r[i-1].ID) <= 0 {
			t.Fatalf("Range ordering broken after delete at %d", i)
		}
	}
}

// TestOrderedLargeTrimPerformance guards the O(n²) regression: trimming a large
// stream must not scan the whole map per removed entry.
func TestOrderedLargeTrimPerformance(t *testing.T) {
	s := NewStream()
	const n = 20000
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%d-0", 4000+i)
		if _, err := s.Add(id, map[string]string{}, nil); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	start := time.Now()
	s.Trim(&AddOptions{HasMaxLen: true, MaxLen: 1000, MaxLenApprox: false})
	elapsed := time.Since(start)
	if s.Len() != 1000 {
		t.Fatalf("trim want 1000, got %d", s.Len())
	}
	// O(n²) full-map-per-entry scan would take far longer; generous bound.
	if elapsed > 3*time.Second {
		t.Fatalf("trim took %v — O(n²) regression", elapsed)
	}
}
