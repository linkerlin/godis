package database

import (
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
)

func TestNoteUsedMemoryPeakTracksAlloc(t *testing.T) {
	prev := atomic.LoadUint64(&peakUsedMemory)
	t.Cleanup(func() { atomic.StoreUint64(&peakUsedMemory, prev) })

	atomic.StoreUint64(&peakUsedMemory, 100)
	if got := noteUsedMemoryPeak(50); got != 100 {
		t.Fatalf("peak should stay 100, got %d", got)
	}
	if got := noteUsedMemoryPeak(200); got != 200 {
		t.Fatalf("peak should rise to 200, got %d", got)
	}
	if atomic.LoadUint64(&peakUsedMemory) != 200 {
		t.Fatalf("stored peak=%d", peakUsedMemory)
	}
}

func TestMemoryStatsPeakAndOverheadAlignWithInfo(t *testing.T) {
	prev := atomic.LoadUint64(&peakUsedMemory)
	t.Cleanup(func() { atomic.StoreUint64(&peakUsedMemory, prev) })
	atomic.StoreUint64(&peakUsedMemory, 1)

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	_ = server.Exec(c, utils.ToCmdLine("SET", "mem-align", strings.Repeat("x", 1024)))

	statsR := server.Exec(c, utils.ToCmdLine("MEMORY", "STATS"))
	stats, ok := statsR.(*protocol.MapReply)
	if !ok {
		t.Fatalf("MEMORY STATS: %T", statsR)
	}
	peakStats := mustMapInt(t, stats, "peak.allocated")
	totalStats := mustMapInt(t, stats, "total.allocated")
	overStats := mustMapInt(t, stats, "overhead.total")
	keysCount := mustMapInt(t, stats, "keys.count")
	if peakStats < totalStats {
		t.Fatalf("peak.allocated (%d) < total.allocated (%d)", peakStats, totalStats)
	}
	wantOver := totalStats - keysCount*bytesPerKeyEstimate
	if wantOver < 0 {
		wantOver = 0
	}
	if overStats != wantOver {
		t.Fatalf("overhead.total=%d want Alloc-dataset=%d", overStats, wantOver)
	}

	infoR := server.Exec(c, utils.ToCmdLine("INFO", "memory"))
	bulk, ok := infoR.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO memory: %T", infoR)
	}
	info := string(bulk.Arg)
	used := mustParseInfoUint(t, info, "used_memory:")
	peakInfo := mustParseInfoUint(t, info, "used_memory_peak:")
	if peakInfo < used {
		t.Fatalf("used_memory_peak=%d < used_memory=%d", peakInfo, used)
	}
	if !strings.Contains(info, "used_memory_overhead:") {
		t.Fatalf("missing used_memory_overhead in INFO")
	}
}

func mustMapInt(t *testing.T, m *protocol.MapReply, key string) int64 {
	t.Helper()
	v, ok := m.Data[key].(*protocol.IntReply)
	if !ok {
		t.Fatalf("%s: %#v", key, m.Data[key])
	}
	return v.Code
}

func mustParseInfoUint(t *testing.T, info, key string) uint64 {
	t.Helper()
	for _, line := range strings.Split(info, "\r\n") {
		if strings.HasPrefix(line, key) {
			v, err := strconv.ParseUint(strings.TrimPrefix(line, key), 10, 64)
			if err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			return v
		}
	}
	t.Fatalf("missing %q in INFO", key)
	return 0
}
