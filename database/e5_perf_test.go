package database

// E5-3 profiling benchmarks (docs/PERFORMANCE.md): DB-layer command cost,
// mirroring the E5-1 bench cells without network. Run with:
//
//	go test ./database -run '^$' -bench BenchmarkE5 -benchtime 2s -benchmem \
//	  -cpuprofile tmp/e5-cpu.out -memprofile tmp/e5-mem.out
//	go tool pprof -top -nodecount=15 database.test tmp/e5-cpu.out
//
// Cells: SET/GET parallel with maxmemory off (std-off), and SET under
// allkeys-lru at the cap (std-off-lru) where the E5-1 container matrix saw
// 465 ops/s vs redis 367k.

import (
	"fmt"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
)

// e5Keyspace matches the bench.sh -r 50000 LRU cell.
const e5Keyspace = 50000

func e5Server(b *testing.B, maxmemory int64, policy string) *Server {
	b.Helper()
	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:       1,
		AppendOnly:      false,
		Maxmemory:       maxmemory,
		MaxmemoryPolicy: policy,
	}
	srv, err := newServerWithSize(1024)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { config.Properties = old })
	return srv
}

func e5Exec(srv *Server, args ...string) {
	_ = srv.Exec(nil, utils.ToCmdLine(args...))
}

func BenchmarkE5SetParallel(b *testing.B) {
	srv := e5Server(b, 0, "noeviction")
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			e5Exec(srv, "SET", fmt.Sprintf("e5:%d", i%e5Keyspace), "v")
			i++
		}
	})
}

func BenchmarkE5GetParallel(b *testing.B) {
	srv := e5Server(b, 0, "noeviction")
	for i := 0; i < e5Keyspace; i++ {
		e5Exec(srv, "SET", fmt.Sprintf("e5:%d", i), "v")
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			e5Exec(srv, "GET", fmt.Sprintf("e5:%d", i%e5Keyspace))
			i++
		}
	})
}

// BenchmarkE5SetEvictionParallel mirrors std-off-lru-p16: 50k-key keyspace
// held at the maxmemory cap (~128B/key estimate), allkeys-lru, parallel SET.
func BenchmarkE5SetEvictionParallel(b *testing.B) {
	// 50k keys x 128B floor estimate = ~6.4MB; hold at 6.0MB so most writes evict.
	srv := e5Server(b, 6000000, "allkeys-lru")
	for i := 0; i < e5Keyspace; i++ {
		e5Exec(srv, "SET", fmt.Sprintf("e5:%d", i), "v")
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			e5Exec(srv, "SET", fmt.Sprintf("e5:%d", i%e5Keyspace), "v")
			i++
		}
	})
}
