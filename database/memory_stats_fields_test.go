package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
)

// Batch 60: MEMORY STATS Redis field-name alignment (Go estimates; not jemalloc).
func TestMemoryStatsRedisFieldNamesBatch60(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	_ = server.Exec(c, utils.ToCmdLine("SET", "b60mem", "v"))

	r := server.Exec(c, utils.ToCmdLine("MEMORY", "STATS"))
	m, ok := r.(*protocol.MapReply)
	if !ok {
		t.Fatalf("MEMORY STATS: %T", r)
	}
	for _, want := range []string{
		"peak.allocated", "total.allocated", "startup.allocated",
		"peak.percentage",
		"replication.backlog", "replica.fullsync.buffer",
		"clients.slaves", "clients.normal",
		"cluster.links", "aof.buffer",
		"lua.caches", "functions.caches", "script.VMs",
		"keys.count", "dataset.bytes", "keys.bytes-per-key",
		"dataset.percentage", "overhead.total",
		"db.dict.rehashing.count",
		"allocator", "allocator.allocated", "allocator.active", "allocator.resident",
		"allocator.muzzy",
		"allocator-fragmentation.ratio", "allocator-fragmentation.bytes",
		"allocator-rss.ratio", "allocator-rss.bytes",
		"rss-overhead.ratio", "rss-overhead.bytes",
		"process.rss", "fragmentation", "fragmentation.bytes",
	} {
		if _, ok := m.Data[want]; !ok {
			t.Fatalf("missing MEMORY STATS key %q", want)
		}
	}
	alloc, ok := m.Data["allocator"].(*protocol.BulkReply)
	if !ok || string(alloc.Arg) != "go" {
		t.Fatalf("allocator want go, got %v", m.Data["allocator"])
	}
	if mustMapInt(t, m, "allocator.muzzy") != 0 {
		t.Fatalf("allocator.muzzy must be 0 (no jemalloc muzzy)")
	}
	if mustMapInt(t, m, "keys.count") < 1 {
		t.Fatalf("keys.count want >=1")
	}
	if mustMapInt(t, m, "keys.bytes-per-key") != bytesPerKeyEstimate {
		t.Fatalf("keys.bytes-per-key want %d got %d",
			bytesPerKeyEstimate, mustMapInt(t, m, "keys.bytes-per-key"))
	}
	peak := mustMapInt(t, m, "peak.allocated")
	total := mustMapInt(t, m, "total.allocated")
	if peak < total {
		t.Fatalf("peak.allocated (%d) < total.allocated (%d)", peak, total)
	}
	pct, ok := m.Data["peak.percentage"].(*protocol.DoubleReply)
	if !ok || pct.Value <= 0 || pct.Value > 100.0001 {
		t.Fatalf("peak.percentage out of range: %v", m.Data["peak.percentage"])
	}
}

func TestMemoryStatsFunctionsCachesReflectsLibraries(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	InitFunctionsEngine(server.dbSet[0].Load().(*DB))

	before := server.Exec(c, utils.ToCmdLine("MEMORY", "STATS"))
	bm, ok := before.(*protocol.MapReply)
	if !ok {
		t.Fatalf("before STATS: %T", before)
	}
	base := mustMapInt(t, bm, "functions.caches")

	code := "#!lua name=b60lib\nredis.register_function('b60f', function(keys, args) return 1 end)"
	load := server.Exec(c, utils.ToCmdLine("FUNCTION", "LOAD", code))
	if protocol.IsErrorReply(load) {
		t.Fatalf("FUNCTION LOAD: %s", load.ToBytes())
	}

	after := server.Exec(c, utils.ToCmdLine("MEMORY", "STATS"))
	am, ok := after.(*protocol.MapReply)
	if !ok {
		t.Fatalf("after STATS: %T", after)
	}
	got := mustMapInt(t, am, "functions.caches")
	if got <= base {
		t.Fatalf("functions.caches should grow after LOAD: before=%d after=%d", base, got)
	}
}
