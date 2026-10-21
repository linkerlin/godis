package timeseries

import (
	"testing"
	"time"
)

func TestAddGetRangeLastDel(t *testing.T) {
	ts := NewTimeSeries("k", 0)
	ts.Add(1, 10)
	ts.Add(3, 30)
	ts.Add(2, 20)
	if ts.Len() != 3 {
		t.Fatalf("Len = %d, want 3", ts.Len())
	}
	if v, ok := ts.Get(2); !ok || v != 20 {
		t.Fatalf("Get(2) = %v/%v", v, ok)
	}
	if _, ok := ts.Get(9); ok {
		t.Fatal("Get(9) should miss")
	}
	last, ok := ts.GetLast()
	if !ok || last.Timestamp != 3 || last.Value != 30 {
		t.Fatalf("GetLast = %+v/%v", last, ok)
	}
	r := ts.Range(1, 2)
	if len(r) != 2 || r[0].Timestamp != 1 || r[1].Timestamp != 2 {
		t.Fatalf("Range(1,2) = %+v", r)
	}
	if got := ts.Del(1, 2); got != 2 {
		t.Fatalf("Del(1,2) = %d, want 2", got)
	}
	if ts.Len() != 1 {
		t.Fatalf("Len after Del = %d", ts.Len())
	}
}

func TestDuplicatePolicies(t *testing.T) {
	cases := []struct {
		policy DuplicatePolicy
		want   float64
	}{
		{DupFirst, 1},
		{DupLast, 2},
		{DupMin, 1},
		{DupMax, 2},
		{DupSum, 3},
	}
	for _, tc := range cases {
		ts := NewTimeSeries("k", 0)
		ts.Add(1, 1)
		if _, err := ts.AddWithPolicy(1, 2, tc.policy); err != nil {
			t.Fatalf("policy %d: %v", tc.policy, err)
		}
		if v, _ := ts.Get(1); v != tc.want {
			t.Errorf("policy %d value = %v, want %v", tc.policy, v, tc.want)
		}
	}
	ts := NewTimeSeries("k", 0)
	ts.Add(1, 1)
	if _, err := ts.AddWithPolicy(1, 2, DupBlock); err == nil {
		t.Fatal("DupBlock should reject duplicate timestamp")
	}
}

func TestRetentionTrimAndInfo(t *testing.T) {
	ts := NewTimeSeries("k", 10*time.Millisecond)
	ts.Samples = []Sample{
		{Timestamp: time.Now().Add(-time.Second).UnixMilli(), Value: 1},
		{Timestamp: time.Now().UnixMilli(), Value: 2},
	}
	if got := ts.Trim(); got != 1 {
		t.Fatalf("Trim = %d, want 1", got)
	}
	info := ts.Info()
	if info["totalSamples"] != 1 {
		t.Fatalf("Info totalSamples = %v", info["totalSamples"])
	}
	ts.SetRetention(time.Minute)
	if ts.GetRetention() != time.Minute {
		t.Fatalf("GetRetention = %v", ts.GetRetention())
	}
	if _, err := ts.Add(time.Now().Add(-time.Hour).UnixMilli(), 3); err == nil {
		t.Fatal("old timestamp should be rejected by retention")
	}
}

func TestLabelsAndDownsampleRules(t *testing.T) {
	ts := NewTimeSeries("k", 0)
	ts.AddLabel("host", "a")
	if got := ts.GetLabels()["host"]; got != "a" {
		t.Fatalf("label host = %q", got)
	}
	ts.SetLabels(map[string]string{"region": "cn"})
	if got := ts.GetLabels()["region"]; got != "cn" {
		t.Fatalf("label region = %q", got)
	}
	rule := DownsampleRule{TimeBucket: time.Minute, Aggregation: AvgAggregation, Destination: "dst"}
	ts.AddDownsampleRule(rule)
	rules := ts.GetDownsampleRules()
	if len(rules) != 1 || rules[0].Destination != "dst" {
		t.Fatalf("downsample rules = %+v", rules)
	}
	if !ts.RemoveDownsampleRule("dst") {
		t.Fatal("RemoveDownsampleRule(dst) = false")
	}
	if ts.RemoveDownsampleRule("dst") {
		t.Fatal("second RemoveDownsampleRule(dst) should be false")
	}
}

func TestAggregationTypeStringRoundTrip(t *testing.T) {
	for _, agg := range []AggregationType{
		AvgAggregation, SumAggregation, MinAggregation, MaxAggregation,
		CountAggregation, FirstAggregation, LastAggregation,
	} {
		s := AggregationTypeToString(agg)
		got, err := ParseAggregationType(s)
		if err != nil || got != agg {
			t.Errorf("round trip %d -> %q -> %d/%v", agg, s, got, err)
		}
	}
	if _, err := ParseAggregationType("bogus"); err == nil {
		t.Fatal("ParseAggregationType(bogus) should fail")
	}
}
