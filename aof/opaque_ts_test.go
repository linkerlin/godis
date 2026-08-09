package aof

import (
	"testing"

	"github.com/linkerlin/godis/datastruct/timeseries"
	"github.com/linkerlin/godis/interface/database"
)

func TestOpaqueTimeSeriesMetaRoundTrip(t *testing.T) {
	ts := timeseries.NewTimeSeries("sensor", 0)
	ts.DuplicatePolicy = timeseries.DupLast
	ts.ChunkSize = 128
	ts.SetLabels(map[string]string{"region": "cn"})
	if _, err := ts.Add(1_700_000_000_000, 1.5); err != nil {
		t.Fatal(err)
	}

	payload, ok := EncodeOpaque(&database.DataEntity{Data: ts})
	if !ok {
		t.Fatal("encode")
	}
	entity, ok := DecodeOpaque(payload)
	if !ok {
		t.Fatal("decode")
	}
	got := entity.Data.(*timeseries.TimeSeries)
	if got.DuplicatePolicy != timeseries.DupLast {
		t.Fatalf("DupPolicy=%v want LAST", got.DuplicatePolicy)
	}
	if got.ChunkSize != 128 {
		t.Fatalf("ChunkSize=%d want 128", got.ChunkSize)
	}
	if got.GetLabels()["region"] != "cn" {
		t.Fatalf("labels=%v", got.GetLabels())
	}
	samples := got.Range(0, 1<<62)
	if len(samples) != 1 || samples[0].Value != 1.5 {
		t.Fatalf("samples=%v", samples)
	}
}
