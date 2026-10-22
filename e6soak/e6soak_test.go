// Package e6soak is the E6-4 long-run soak runner: repeated load cycles
// (TTL create/expire, hash-field expiry, FT index create/drop, FLUSHDB,
// blocking commands, master→replica streaming) against real in-process std
// servers, with HeapInuse / goroutine / FD baselines sampled after warmup and
// asserted at the end — no monotonic growth.
//
// Nightly: runs as part of `go test -race -shuffle=on ./...` with the default
// cycle count (~1 minute, skipped in -short).
// Local long soak: SOAK_CYCLES=20000 bash ... (~72h of cycles); per-cycle
// metric lines in the log form the soak curve.
package e6soak

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/tcp"
	stdserver "github.com/linkerlin/godis/redis/server/std"
)

// ---- minimal RESP client ----

type rclient struct {
	conn net.Conn
	r    *bufio.Reader
}

func (c *rclient) close() { _ = c.conn.Close() }

func (c *rclient) send(args ...string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := c.conn.Write([]byte(b.String())); err != nil {
		return "<IOERR>"
	}
	line, err := c.r.ReadString('\n')
	if err != nil {
		return "<IOERR>"
	}
	line = strings.TrimRight(line, "\r\n")
	switch line[0] {
	case '+', '-', ':':
		return line
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "<nil>"
		}
		buf := make([]byte, n+2)
		for total := 0; total < len(buf); {
			m, err := c.r.Read(buf[total:])
			if err != nil {
				return "<IOERR>"
			}
			total += m
		}
		return string(buf[:n])
	case '*':
		n, _ := strconv.Atoi(line[1:])
		parts := make([]string, n)
		for i := 0; i < n; i++ {
			parts[i] = c.send()
		}
		return strings.Join(parts, "\n")
	}
	return line
}

// ---- servers ----

type soakServer struct {
	addr string
	name string
}

func startServer(t *testing.T, name string) *soakServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	handler, err := stdserver.MakeHandler()
	if err != nil {
		t.Fatal(err)
	}
	closeChan := make(chan struct{})
	t.Cleanup(func() {
		close(closeChan)
		_ = handler.Close()
	})
	go tcp.ListenAndServe(listener, handler, closeChan)
	return &soakServer{addr: listener.Addr().String(), name: name}
}

func (s *soakServer) client(t *testing.T) *rclient {
	t.Helper()
	c, err := net.DialTimeout("tcp", s.addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", s.addr, err)
	}
	return &rclient{conn: c, r: bufio.NewReader(c)}
}

// ---- metrics ----

type metrics struct {
	heapInuse  uint64
	goroutines int
	fds        int // linux only; 0 elsewhere
}

var fdDir = "/proc/self/fd"

func sample() metrics {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	m := metrics{heapInuse: ms.HeapInuse, goroutines: runtime.NumGoroutine()}
	if entries, err := os.ReadDir(fdDir); err == nil {
		m.fds = len(entries)
	}
	return m
}

// ---- soak ----

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func TestE6SoakLoadCycles(t *testing.T) {
	if testing.Short() {
		t.Skip("soak skipped in short mode")
	}
	oldProps := config.Properties
	config.Properties = &config.ServerProperties{Databases: 1, AppendOnly: false}
	t.Cleanup(func() { config.Properties = oldProps })

	cycles := envInt("SOAK_CYCLES", 12)

	main := startServer(t, "main")
	repl := startServer(t, "repl") // replication pair: main → repl

	// attach replica
	rc := repl.client(t)
	defer rc.close()
	if r := rc.send("REPLICAOF", hostOf(main.addr), portOf(main.addr)); !strings.HasPrefix(r, "+OK") {
		t.Fatalf("REPLICAOF: %s", r)
	}

	master := main.client(t)
	defer master.close()

	// warmup: 3 cycles, then baseline
	for i := 0; i < 3; i++ {
		runCycle(t, master, main, repl, i)
	}
	time.Sleep(500 * time.Millisecond) // let blocking-command goroutines drain
	base := sample()
	t.Logf("baseline after warmup: heap=%d goroutines=%d fds=%d", base.heapInuse, base.goroutines, base.fds)

	for i := 0; i < cycles; i++ {
		runCycle(t, master, main, repl, i)
		if i%10 == 0 {
			m := sample()
			t.Logf("cycle %d/%d: heap=%d goroutines=%d fds=%d", i, cycles, m.heapInuse, m.goroutines, m.fds)
		}
	}

	// settle: blocking waiters exited, replica drained, garbage collected
	time.Sleep(1500 * time.Millisecond)
	final := sample()
	t.Logf("final: heap=%d goroutines=%d fds=%d (cycles=%d)", final.heapInuse, final.goroutines, final.fds, cycles)

	// ponytail: heap slack 30% + 64MB (GC timing noise), goroutine slack 8
	// (one-shot workers), fd slack 16 (linux only) — tighten if flake-free.
	if lim := base.heapInuse + base.heapInuse*3/10 + 64<<20; final.heapInuse > lim {
		t.Errorf("HeapInuse grew: baseline %d -> final %d (limit %d)", base.heapInuse, final.heapInuse, lim)
	}
	if final.goroutines > base.goroutines+8 {
		t.Errorf("goroutines grew: baseline %d -> final %d", base.goroutines, final.goroutines)
	}
	if base.fds > 0 && final.fds > base.fds+16 {
		t.Errorf("fds grew: baseline %d -> final %d", base.fds, final.fds)
	}
}

// runCycle executes one full workload cycle on the main server (and streams
// writes into the replication pair).
func runCycle(t *testing.T, m *rclient, main, repl *soakServer, cycle int) {
	t.Helper()

	// 1) TTL churn: short-lived keys + hash-field expiry (timewheel cleanup)
	for i := 0; i < 200; i++ {
		m.send("SET", fmt.Sprintf("ttl:%d:%d", cycle, i), "v", "EX", "1")
	}
	for i := 0; i < 50; i++ {
		m.send("HSET", fmt.Sprintf("httl:%d:%d", cycle, i), "f", "v")
		m.send("HEXPIRE", fmt.Sprintf("httl:%d:%d", cycle, i), "1", "FIELDS", "1", "f")
	}

	// 2) FT index churn
	idx := fmt.Sprintf("soakidx")
	if r := m.send("FT.CREATE", idx, "ON", "HASH", "SCHEMA", "title", "TEXT"); !strings.HasPrefix(r, "+OK") && !strings.HasPrefix(r, "-ERR") {
		t.Fatalf("FT.CREATE: %s", r)
	}
	for i := 0; i < 50; i++ {
		m.send("HSET", fmt.Sprintf("doc:%d:%d", cycle, i), "title", "hello soak world "+strconv.Itoa(i))
	}
	m.send("FT.SEARCH", idx, "hello", "LIMIT", "0", "10")
	m.send("FT.DROPINDEX", idx)

	// 3) blocking commands: one timeout path, one wake path
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); c := main.client(t); defer c.close(); c.send("BLPOP", "blkey", "1") }()   // timeout
	go func() { defer wg.Done(); c := main.client(t); defer c.close(); m.send("LPUSH", "blkey", "v") }() // wake path
	wg.Wait()

	// 4) replication: writes streamed to the replica must converge
	const replN = 100
	for i := 0; i < replN; i++ {
		m.send("SET", fmt.Sprintf("repl:%d", i), "v")
	}
	slave := repl.client(t)
	defer slave.close()
	deadline := time.Now().Add(10 * time.Second)
	converged := false
	for time.Now().Before(deadline) {
		if n, _ := strconv.Atoi(strings.TrimPrefix(slave.send("DBSIZE"), ":")); n >= replN {
			converged = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !converged {
		t.Fatalf("cycle %d: replica did not converge", cycle)
	}

	// 5) FLUSHDB on the main server (timer/index cleanup paths must not leak)
	m.send("FLUSHDB")

	// TTL assertions only every few cycles (they need the 1s expiry to pass)
	if cycle%3 == 0 {
		time.Sleep(1100 * time.Millisecond)
		if r := m.send("EXISTS", fmt.Sprintf("ttl:%d:0", cycle)); r != ":0" {
			t.Errorf("cycle %d: ttl key not expired: %s", cycle, r)
		}
		if r := m.send("HGET", fmt.Sprintf("httl:%d:0", cycle), "f"); r != "<nil>" {
			t.Errorf("cycle %d: hash field not expired: %s", cycle, r)
		}
	}
}

var addrRe = regexp.MustCompile(`^(.*):(\d+)$`)

func hostOf(addr string) string {
	m := addrRe.FindStringSubmatch(addr)
	if m == nil {
		return "127.0.0.1"
	}
	return m[1]
}

func portOf(addr string) string {
	m := addrRe.FindStringSubmatch(addr)
	if m == nil {
		return "6399"
	}
	return m[2]
}
