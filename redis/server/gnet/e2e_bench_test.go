package gnet

// E5-3 round-3 e2e benchmark for the gnet engine: same 50 native connections
// with pipelined SET batches as the std engine's e2e_bench_test.go, to tell
// real per-command server cost from the redis-benchmark(C) interaction
// pathology (which does not reproduce with Go clients). Run with:
//
//	go test ./redis/server/gnet -run '^$' -bench BenchmarkE5E2E -benchtime 3s \
//	  -cpu 50 -cpuprofile tmp/gnet-e2e-cpu.out

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/linkerlin/godis/database"
)

func startBenchServer(b *testing.B) string {
	b.Helper()
	// grab a free port, then hand it to gnet (gnet.Run has no :0 discovery)
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	handler := NewGnetServer(database.MustNewStandaloneServer())
	go func() { _ = handler.Run(addr) }()
	b.Cleanup(func() { _ = handler.Close() })

	for i := 0; i < 200; i++ {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return addr
		}
		time.Sleep(5 * time.Millisecond)
	}
	b.Fatal("gnet bench server did not become ready")
	return ""
}

type e2eClient struct {
	conn net.Conn
	r    *bufio.Reader
	req  []byte
}

func dialE2E(b *testing.B, addr string, pipeline int, key string) *e2eClient {
	b.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		b.Fatal(err)
	}
	req := []byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$1\r\nv\r\n", len(key), key))
	batch := make([]byte, 0, len(req)*pipeline)
	for i := 0; i < pipeline; i++ {
		batch = append(batch, req...)
	}
	return &e2eClient{conn: conn, r: bufio.NewReader(conn), req: batch}
}

func (c *e2eClient) roundTrip(pipeline int) {
	if _, err := c.conn.Write(c.req); err != nil {
		panic(err)
	}
	for i := 0; i < pipeline; i++ {
		if _, err := io.ReadFull(c.r, make([]byte, 5)); err != nil { // "+OK\r\n"
			panic(err)
		}
	}
}

func (c *e2eClient) close() { _ = c.conn.Close() }

func benchE2E(b *testing.B, pipeline int) {
	addr := startBenchServer(b)
	b.RunParallel(func(pb *testing.PB) {
		c := dialE2E(b, addr, pipeline, "e2ekey")
		defer c.close()
		for pb.Next() {
			c.roundTrip(pipeline)
		}
	})
}

func BenchmarkE5E2ESetP1(b *testing.B)   { benchE2E(b, 1) }
func BenchmarkE5E2ESetP16(b *testing.B) { benchE2E(b, 16) }
func BenchmarkE5E2ESetP64(b *testing.B) { benchE2E(b, 64) }
