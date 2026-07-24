package timeseries

import (
	"testing"
	"time"
)

func TestAggregateTWA(t *testing.T) {
	ts := NewTimeSeries("t", 0)
	if _, err := ts.Add(1, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.Add(100, 20); err != nil {
		t.Fatal(err)
	}
	t.Logf("len=%d", ts.Len())
	got := ts.Range(1, 99)
	t.Logf("range=%v", got)
	samples := ts.RangeWithAggregation(1, 99, 100*time.Millisecond, TwaAggregation)
	t.Logf("agg=%v", samples)
	if len(samples) != 1 {
		t.Fatalf("want 1 bucket, got %+v", samples)
	}
	if samples[0].Value < 9.9 || samples[0].Value > 10.1 {
		t.Fatalf("twa want ~10 got %v", samples[0].Value)
	}
}
