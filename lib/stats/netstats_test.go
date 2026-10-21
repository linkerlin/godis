package stats

import (
	"testing"
	"time"
)

func TestRecordGetAndReset(t *testing.T) {
	Reset()
	if in, out := GetStats(); in != 0 || out != 0 {
		t.Fatalf("after Reset: %d/%d", in, out)
	}
	RecordInput(100)
	RecordOutput(250)
	RecordInput(20)
	if in, out := GetStats(); in != 120 || out != 250 {
		t.Fatalf("GetStats=%d/%d", in, out)
	}
	Reset()
	if in, out := GetStats(); in != 0 || out != 0 {
		t.Fatalf("second Reset: %d/%d", in, out)
	}
}

func TestRates(t *testing.T) {
	Reset()
	globalNetStats.lastUpdateTime = time.Now().Add(-2 * time.Second)
	RecordInput(2048)
	RecordOutput(4096)
	in, out := GetRates()
	if in < 0.9 || in > 1.1 || out < 1.9 || out > 2.1 {
		t.Fatalf("rates KB/s = %f/%f", in, out)
	}
	// Before the one-second sample interval, the previous rate is returned.
	globalNetStats.lastUpdateTime = time.Now()
	RecordInput(2048)
	in, out = GetRates()
	if in < 0.9 || in > 1.1 || out < 1.9 || out > 2.1 {
		t.Fatalf("cached rates = %f/%f", in, out)
	}
}
