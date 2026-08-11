package database

import (
	"runtime"
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// jemalloc / used_memory accounting honesty (compat/jemalloc-info-boundary).
// Godis must NOT pretend to use jemalloc; used_memory* is Go-runtime based.

func TestInfoMemoryAllocatorHonestGo(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("INFO", "memory"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO memory: %T", r)
	}
	s := string(bulk.Arg)

	if !strings.Contains(s, "mem_allocator:go") {
		t.Fatalf("want mem_allocator:go, got:\n%s", s)
	}
	if strings.Contains(strings.ToLower(s), "jemalloc") {
		t.Fatalf("INFO memory must not claim jemalloc:\n%s", s)
	}
	// Classic Redis-jemalloc INFO keys that would imply arena accounting.
	for _, fake := range []string{
		"mem_allocator:jemalloc",
		"allocator_frag_bytes:", // Redis jemalloc-specific; we omit intentionally
	} {
		if strings.Contains(s, fake) {
			t.Fatalf("INFO must not advertise %q:\n%s", fake, s)
		}
	}
}

func TestInfoMemoryUsedMemoryMatchesGoAlloc(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("INFO", "memory"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO memory: %T", r)
	}
	s := string(bulk.Arg)

	used := mustParseInfoUint(t, s, "used_memory:")
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// used_memory is MemStats.Alloc at INFO time; allow generous skew for concurrent GC.
	lo, hi := m.Alloc/2, m.Alloc*2+1<<20
	if used < lo || used > hi {
		t.Fatalf("used_memory=%d not Go Alloc-scale (Alloc≈%d, band [%d,%d])", used, m.Alloc, lo, hi)
	}
}

func TestMemoryStatsAllocatorNotJemalloc(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	stats := server.Exec(c, utils.ToCmdLine("MEMORY", "STATS"))
	m, ok := stats.(*protocol.MapReply)
	if !ok {
		t.Fatalf("MEMORY STATS: %T", stats)
	}
	allocLabel, ok := m.Data["allocator"].(*protocol.BulkReply)
	if !ok || string(allocLabel.Arg) != "go" {
		t.Fatalf("MEMORY STATS allocator want go, got %v", m.Data["allocator"])
	}
	// No redis-style jemalloc.* nested keys.
	for k := range m.Data {
		if strings.Contains(strings.ToLower(k), "jemalloc") {
			t.Fatalf("MEMORY STATS must not expose jemalloc key %q", k)
		}
	}
}

func TestMemoryMallocStatsNoFakeJemalloc(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("MEMORY", "MALLOC-STATS"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("MALLOC-STATS: %T", r)
	}
	s := strings.ToLower(string(bulk.Arg))
	if strings.Contains(s, "jemalloc") {
		t.Fatalf("MALLOC-STATS must not pretend jemalloc:\n%s", bulk.Arg)
	}
	// Must admit Go runtime limitation, not dump fake arena tables.
	for _, badge := range []string{"arenas", "bins:", "extents:", "narenas"} {
		if strings.Contains(s, badge) {
			t.Fatalf("MALLOC-STATS looks like fake jemalloc dump (%q):\n%s", badge, bulk.Arg)
		}
	}
	if !strings.Contains(s, "go runtime") && !strings.Contains(s, "not available") {
		t.Fatalf("MALLOC-STATS should admit Go/unavailable, got: %s", bulk.Arg)
	}
}

func TestConfigJemallocBgThreadDoesNotChangeAllocator(t *testing.T) {
	// CONFIG jemalloc-bg-thread is a Redis-compat stub only; flipping it must
	// never rebrand INFO as jemalloc.
	server := getTestServer()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "jemalloc-bg-thread", "yes")), "OK")
	r := server.Exec(c, utils.ToCmdLine("INFO", "memory"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO memory: %T", r)
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "mem_allocator:go") {
		t.Fatalf("after jemalloc-bg-thread=yes still want mem_allocator:go:\n%s", s)
	}
	if strings.Contains(strings.ToLower(s), "jemalloc") {
		t.Fatalf("jemalloc-bg-thread stub must not make INFO claim jemalloc:\n%s", s)
	}
	_ = server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "jemalloc-bg-thread", "no"))
}

func TestFullInfoNeverClaimsJemallocAllocator(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("INFO"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO: %T", r)
	}
	s := string(bulk.Arg)
	if strings.Contains(s, "mem_allocator:jemalloc") {
		t.Fatalf("full INFO claims mem_allocator:jemalloc")
	}
	// mem_allocator line must be go if present.
	for _, line := range strings.Split(s, "\r\n") {
		if strings.HasPrefix(line, "mem_allocator:") {
			if line != "mem_allocator:go" {
				t.Fatalf("unexpected %q", line)
			}
		}
	}
}
