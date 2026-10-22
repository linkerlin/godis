package std

// E5-3 round-2 e2e benchmark: the std server serving 50 native connections
// (mirrors the std-off container cells without docker). Run with:
//
//	go test ./redis/server/std -run '^$' -bench BenchmarkE5E2E -benchtime 3s \
//	  -cpu 50 -cpuprofile tmp/e2e-cpu.out
//	go tool pprof -top -nodecount=15 std.test tmp/e2e-cpu.out

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/linkerlin/godis/tcp"
)

func startBenchServer(b *testing.B) string {
	b.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	handler, err := MakeHandler()
	if err != nil {
		b.Fatal(err)
	}
	closeChan := make(chan struct{})
	done := make(chan struct{})
	b.Cleanup(func() {
		close(closeChan)
		_ = handler.Close()
		<-done // no accept/Handle goroutine outlives the benchmark
	})
	go func() {
		defer close(done)
		tcp.ListenAndServe(listener, handler, closeChan)
	}()
	return listener.Addr().String()
}

// e2eClient writes one pipelined batch (pipeline SET requests) and reads the
// fixed-size +OK replies.
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
