package array

import "testing"

func TestSetGetLenCountAndRanges(t *testing.T) {
	a := New()
	if got := a.Set(2, []byte("c")); got != 1 {
		t.Fatalf("Set(2) new = %d, want 1", got)
	}
	if got := a.Set(0, []byte("a")); got != 1 {
		t.Fatalf("Set(0) new = %d, want 1", got)
	}
	if got := a.Set(2, []byte("c2")); got != 0 {
		t.Fatalf("Set(2) overwrite = %d, want 0", got)
	}
	if v, ok := a.Get(2); !ok || string(v) != "c2" {
		t.Fatalf("Get(2) = %q/%v", v, ok)
	}
	if a.Len() != 3 || a.Count() != 2 {
		t.Fatalf("Len=%d Count=%d, want 3/2", a.Len(), a.Count())
	}
	if got := a.Del(1); got != 0 {
		t.Fatalf("Del(empty) = %d, want 0", got)
	}
	if got := a.DelRanges([][2]int64{{0, 1}, {1, 2}}); got != 2 {
		t.Fatalf("DelRanges overlapping = %d, want 2", got)
	}
	if a.Len() != 0 || a.Count() != 0 {
		t.Fatalf("after deleting all: Len=%d Count=%d", a.Len(), a.Count())
	}
}

func TestScanRangeAndLimit(t *testing.T) {
	a := New()
	a.Set(0, []byte("a"))
	a.Set(2, []byte("c"))
	a.Set(5, []byte("f"))
	assertScan := func(got []ScanEntry, want ...string) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("Scan got %d entries, want %d: %+v", len(got), len(want), got)
		}
		for i, w := range want {
			if string(got[i].Value) != w {
				t.Fatalf("Scan[%d]=%q want %q", i, got[i].Value, w)
			}
		}
	}
	assertScan(a.Scan(0, 5, 0), "a", "c", "f")
	assertScan(a.Scan(5, 0, 0), "f", "c", "a")
	assertScan(a.Scan(0, 5, 1), "a")
	assertScan(a.Scan(0, 5, 2), "a", "c")
	assertScan(a.Scan(3, 4, 0))
	assertScan(a.Scan(6, 10, 0))
	assertScan(a.Scan(0, 100, 0), "a", "c", "f")
	assertScan(a.Scan(100, 0, 0), "f", "c", "a")

	empty := New()
	assertScan(empty.Scan(0, 0, 0))
}

func TestInsertNextIndexAndExhaustion(t *testing.T) {
	a := New()
	last, ok := a.Insert([]byte("a"), []byte("b"))
	if !ok || last != 1 {
		t.Fatalf("Insert = %d/%v, want 1/true", last, ok)
	}
	if next, ok := a.NextIndex(); !ok || next != 2 {
		t.Fatalf("NextIndex = %d/%v, want 2/true", next, ok)
	}
	a.SetCursor(MaxIndex + 1)
	if _, ok := a.NextIndex(); ok {
		t.Fatal("NextIndex should be exhausted")
	}
	if _, ok := a.Insert([]byte("x")); ok {
		t.Fatal("Insert should fail when cursor is exhausted")
	}
}

func TestRingInsertAndLastItems(t *testing.T) {
	a := New()
	for i, v := range []string{"v0", "v1", "v2", "v3"} {
		last, ok := a.RingInsert(3, []byte(v))
		if !ok || last != int64(i%3) {
			t.Fatalf("RingInsert(%s) = %d/%v, want %d", v, last, ok, i%3)
		}
	}
	if v, ok := a.Get(0); !ok || string(v) != "v3" {
		t.Fatalf("ring slot 0 = %q/%v, want v3", v, ok)
	}
	if a.Count() != 3 {
		t.Fatalf("ring Count=%d, want 3", a.Count())
	}
	assertItems(t, a.LastItems(3, false), "v1", "v2", "v3")
	assertItems(t, a.LastItems(3, true), "v3", "v2", "v1")
}

func TestLastItemsPlainArrayIncludesEmptySlots(t *testing.T) {
	a := New()
	a.Set(0, []byte("a"))
	a.Set(2, []byte("c"))
	assertItems(t, a.LastItems(2, false), "", "c")
	assertItems(t, a.LastItems(2, true), "c", "")
	assertItems(t, a.LastItems(5, false), "a", "", "c")
}

func TestRingTruncateAndRestoreState(t *testing.T) {
	a := New()
	a.Set(0, []byte("0"))
	a.Set(1, []byte("1"))
	a.Set(2, []byte("2"))
	a.Set(3, []byte("3"))
	a.Set(4, []byte("4"))
	if last, ok := a.RingInsert(3, []byte("x")); !ok || last != 0 {
		t.Fatalf("RingInsert truncate = %d/%v", last, ok)
	}
	if a.Len() != 3 || a.Count() != 3 {
		t.Fatalf("after truncate Len=%d Count=%d, want 3/3", a.Len(), a.Count())
	}

	slots, order, cursor, ring := a.State()
	b := RestoreState(slots, order, cursor, ring)
	if b.Len() != a.Len() || b.Count() != a.Count() {
		t.Fatalf("restored Len/Count = %d/%d, want %d/%d", b.Len(), b.Count(), a.Len(), a.Count())
	}
	if next, ok := b.NextIndex(); !ok || next != 1 {
		t.Fatalf("restored NextIndex = %d/%v, want 1", next, ok)
	}
	assertItems(t, b.LastItems(3, false), "1", "2", "x")
}

func TestRestoreDerivesOrderFromSlots(t *testing.T) {
	a := Restore([][]byte{[]byte("a"), nil, []byte("c")})
	if a.Count() != 2 || a.Len() != 3 {
		t.Fatalf("Restore Len/Count = %d/%d", a.Len(), a.Count())
	}
	assertItems(t, a.LastItems(3, false), "a", "", "c")
}

func TestRange(t *testing.T) {
	a := New()
	a.Set(1, []byte("b"))
	assertItems(t, a.Range(0, 2), "", "b", "")
	assertItems(t, a.Range(2, 0), "", "b", "")
	assertItems(t, a.Range(5, 6), "", "")
}

func assertItems(t *testing.T, got [][]byte, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("items len = %d, want %d (%q)", len(got), len(want), got)
	}
	for i, w := range want {
		if w == "" {
			if got[i] != nil {
				t.Fatalf("items[%d] = %q, want nil", i, got[i])
			}
			continue
		}
		if string(got[i]) != w {
			t.Fatalf("items[%d] = %q, want %q", i, got[i], w)
		}
	}
}
