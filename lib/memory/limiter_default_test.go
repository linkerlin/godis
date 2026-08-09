package memory

import (
	"runtime"
	"testing"
)

func TestDefaultMemoryUsageUsesAllocNotTotalAlloc(t *testing.T) {
	l := NewLimiter(&Config{MaxMemory: 1 << 30, Policy: "noeviction"})
	got := l.getMemoryUsage()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	if got <= 0 {
		t.Fatalf("usage=%d", got)
	}
	// TotalAlloc is cumulative; default path must use live Alloc.
	if m.TotalAlloc > m.Alloc && got == int64(m.TotalAlloc) {
		t.Fatalf("default usage should track Alloc, not TotalAlloc (got=%d Alloc=%d TotalAlloc=%d)",
			got, m.Alloc, m.TotalAlloc)
	}
}
