package utils

import "testing"

// Semantics pinned against live Redis 8.10 (GETRANGE family):
// negative offsets convert then clamp to 0; end past size clamps to last.
func TestConvertRange(t *testing.T) {
	cases := []struct {
		start, end, size int64
		beg, stop        int // expected Go slice bounds
	}{
		{-8, 43, 5, 0, 5},  // GETRANGE s -8 43 => whole "49750"
		{-8, -7, 5, 0, 1},  // GETRANGE s -8 -7 => "4"
		{3, -7, 5, -1, -1}, // GETRANGE s 3 -7 => ""
		{0, -1, 5, 0, 5},   // whole string
		{1, 3, 5, 1, 4},
		{2, 2, 5, 2, 3},
		{5, 9, 5, -1, -1},  // start past end of string
		{-1, -1, 5, 4, 5},  // last char
		{-6, -6, 5, 0, 1},  // both clamp to 0 => first char
		{0, 0, 0, -1, -1},  // empty value
		{-1, -1, 1, 0, 1},
	}
	for _, c := range cases {
		beg, stop := ConvertRange(c.start, c.end, c.size)
		if beg != c.beg || stop != c.stop {
			t.Errorf("ConvertRange(%d,%d,%d) = (%d,%d), want (%d,%d)",
				c.start, c.end, c.size, beg, stop, c.beg, c.stop)
		}
	}
}
