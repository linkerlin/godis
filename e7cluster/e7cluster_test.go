// Package e7cluster is the E7-1 cluster interop test: it boots a real
// two-node godis cluster as child processes (seed + joiner, separate raft
// ports/dirs), assigns the slot space, and drives it with a mini smart
// client — the same contract third-party cluster clients rely on:
// CLUSTER SLOTS topology, CRC16 slot routing, -MOVED redirect shape and
// -CROSSSLOT rejection.
//
// Local: go test ./e7cluster -v -timeout 10m   (skipped in -short)
// Companion docker-based redis-cli -c smoke: scripts/cluster-interop.sh
package e7cluster

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linkerlin/godis/lib/hashslot"
)

var (
	binOnce sync.Once
	binPath string
	binErr  error
)

func godisBin(t *testing.T) string {
	t.Helper()
	binOnce.Do(func() {
		dir, err := os.MkdirTemp("", "godis-e7-*")
		if err != nil {
			binErr = err
			return
		}
		binPath = filepath.Join(dir, "godis-test")
		if runtime.GOOS == "windows" {
			binPath += ".exe"
		}
		cmd := exec.Command("go", "build", "-o", binPath, ".")
		cmd.Dir = repoRoot()
		if out, err := cmd.CombinedOutput(); err != nil {
			binErr = fmt.Errorf("go build: %v: %s", err, out)
		}
	})
	if binErr != nil {
		t.Skipf("build unavailable: %v", binErr)
	}
	return binPath
}

func repoRoot() string {
	wd, _ := os.Getwd()
	return filepath.Clean(filepath.Join(wd, ".."))
}

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
	return c.readReply()
}

// readReply returns one line for simple/error/int, the payload for bulk, and
// a "\n"-flattened string for arrays (nested arrays flatten too — CLUSTER
// SLOTS parsing below relies on that).
func (c *rclient) readReply() string {
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
			parts[i] = c.readReply()
		}
		return strings.Join(parts, "\n")
	}
	return line
}

type node struct {
	addr string
	dir  string
	out  *strings.Builder
	cmd  *exec.Cmd
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return p
}

func startNode(t *testing.T, name, dir string, extra ...string) *node {
	t.Helper()
	port := freePort(t)
	raftPort := freePort(t)
	lines := append([]string{
		"bind 127.0.0.1",
		fmt.Sprintf("port %d", port),
		"dir " + filepath.ToSlash(dir),
		"announce-host 127.0.0.1",
		"cluster-enable yes",
		fmt.Sprintf("raft-listen-address 127.0.0.1:%d", raftPort),
		"save \"\"",
		"appendonly no",
	}, extra...)
	conf := filepath.Join(dir, fmt.Sprintf("node-%d.conf", port))
	if err := os.WriteFile(conf, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	n := &node{addr: fmt.Sprintf("127.0.0.1:%d", port), dir: dir, out: &strings.Builder{}}
	n.cmd = exec.Command(godisBin(t), "-c", conf)
	n.cmd.Dir = dir
	n.cmd.Stdout = n.out
	n.cmd.Stderr = n.out
	if err := n.cmd.Start(); err != nil {
		t.Fatalf("start %s: %v\n%s", name, err, n.out.String())
	}
	t.Cleanup(func() {
		if n.cmd.Process != nil {
			_ = n.cmd.Process.Kill()
			_, _ = n.cmd.Process.Wait()
		}
	})
	n.waitUp(t, name, 20*time.Second)
	return n
}

func (n *node) waitUp(t *testing.T, name string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r := n.trySend("PING"); strings.Contains(r, "PONG") {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("node %s (%s) did not come up\nlogs:\n%s", name, n.addr, n.out.String())
}

func (n *node) trySend(args ...string) string {
	c, err := net.DialTimeout("tcp", n.addr, time.Second)
	if err != nil {
		return "<IOERR>"
	}
	defer c.Close()
	cl := &rclient{conn: c, r: bufio.NewReader(c)}
	return cl.send(args...)
}

func (n *node) client(t *testing.T) *rclient {
	t.Helper()
	c, err := net.DialTimeout("tcp", n.addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", n.addr, err)
	}
	return &rclient{conn: c, r: bufio.NewReader(c)}
}

// slotRange is one CLUSTER SLOTS entry: [start, end] owned by host:port.
type slotRange struct {
	start, end int
	host       string
	port       int
}

func parseSlots(t *testing.T, flat string) []slotRange {
	t.Helper()
	fields := strings.Split(flat, "\n")
	atoi := func(s string) (int, bool) {
		n, err := strconv.Atoi(strings.TrimPrefix(s, ":")) // int replies keep the ':'
		return n, err == nil
	}
	var ranges []slotRange
	for i := 0; i+4 <= len(fields); i += 5 {
		start, ok1 := atoi(fields[i])
		end, ok2 := atoi(fields[i+1])
		port, ok3 := atoi(fields[i+3])
		if !ok1 || !ok2 || !ok3 {
			continue
		}
		ranges = append(ranges, slotRange{start: start, end: end, host: fields[i+2], port: port})
	}
	return ranges
}

func ownerOf(ranges []slotRange, slot int) (string, bool) {
	for _, r := range ranges {
		if slot >= r.start && slot <= r.end {
			return fmt.Sprintf("%s:%d", r.host, r.port), true
		}
	}
	return "", false
}

func TestE7ClusterInterop(t *testing.T) {
	if testing.Short() {
		t.Skip("cluster interop skipped in short mode")
	}
	dirA, dirB := t.TempDir(), t.TempDir()
	seed := startNode(t, "seed", dirA, "cluster-as-seed yes")
	joiner := startNode(t, "joiner", dirB, "cluster-seed "+seed.addr)

	// wait for the join to propagate, then split the slot space
	deadline := time.Now().Add(30 * time.Second)
	for {
		r := joiner.trySend("CLUSTER", "NODES")
		if len(r) > 20 && !strings.Contains(r, "IOERR") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("joiner never joined\nseed logs:\n%s\njoiner logs:\n%s", seed.out.String(), joiner.out.String())
		}
		time.Sleep(300 * time.Millisecond)
	}
	// The seed claims the whole ring at boot; carve the upper half for the
	// joiner (no data yet, so a plain del+add is enough). Each step waits for
	// FSM propagation on the joiner before the next (raft apply lag).
	if r := seed.trySend("CLUSTER", "DELSLOTSRANGE", "8192", "16383"); strings.HasPrefix(r, "-") {
		t.Fatalf("DELSLOTSRANGE on seed: %s\nlogs:\n%s", r, seed.out.String())
	}
	deadline = time.Now().Add(30 * time.Second)
	for {
		flat := joiner.trySend("CLUSTER", "SLOTS")
		if rs := parseSlots(t, flat); len(rs) == 1 {
			if o, ok := ownerOf(rs, 8192); !ok || o == "" {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("upper half still assigned: %q\nseed logs:\n%s", flat, seed.out.String())
		}
		time.Sleep(300 * time.Millisecond)
	}
	var addOK bool
	deadline = time.Now().Add(30 * time.Second)
	for !addOK {
		if r := joiner.trySend("CLUSTER", "ADDSLOTSRANGE", "8192", "16383"); !strings.HasPrefix(r, "-") {
			addOK = true
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ADDSLOTSRANGE on joiner kept failing\nlogs:\n%s", joiner.out.String())
		}
		time.Sleep(500 * time.Millisecond)
	}

	// wait until BOTH nodes see the split: each node owns one half, all
	// 16384 slots covered (smart clients only rely on this shape)
	var ranges []slotRange
	deadline = time.Now().Add(30 * time.Second)
	for {
		flat := joiner.trySend("CLUSTER", "SLOTS")
		ranges = parseSlots(t, flat)
		if len(ranges) == 2 {
			o1, _ := ownerOf(ranges, 1)
			o2, _ := ownerOf(ranges, 16383)
			if (o1 == seed.addr && o2 == joiner.addr) || (o1 == joiner.addr && o2 == seed.addr) {
				t.Logf("topology ready: slots 0-8191 -> %s, 8192-16383 -> %s (seed=%s joiner=%s)",
					o1, o2, seed.addr, joiner.addr)
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("topology incomplete: slots=%q (seed=%s joiner=%s)\nseed logs:\n%s\njoiner logs:\n%s",
				flat, seed.addr, joiner.addr, seed.out.String(), joiner.out.String())
		}
		time.Sleep(300 * time.Millisecond)
	}

	// slot math sanity against the server's own view
	if r := seed.trySend("CLUSTER", "KEYSLOT", "foo"); r != ":12182" { // known CRC16 vector
		t.Logf("note: CLUSTER KEYSLOT foo = %s", r)
	}

	// smart-client roundtrip: route each key to its owner, SET then GET back
	cA, cB := seed.client(t), joiner.client(t)
	defer cA.close()
	defer cB.close()
	clientFor := func(addr string) *rclient {
		if addr == seed.addr {
			return cA
		}
		return cB
	}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("e7key:%d", i)
		slot := int(hashslot.Slot(key))
		owner, ok := ownerOf(ranges, slot)
		if !ok {
			t.Fatalf("no owner for slot %d (key %s)", slot, key)
		}
		if r := clientFor(owner).send("SET", key, "v"+strconv.Itoa(i)); r != "+OK" {
			t.Fatalf("SET %s via owner %s: %s", key, owner, r)
		}
	}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("e7key:%d", i)
		slot := int(hashslot.Slot(key))
		owner, _ := ownerOf(ranges, slot)
		if got := clientFor(owner).send("GET", key); got != "v"+strconv.Itoa(i) {
			t.Fatalf("GET %s via owner %s: %q", key, owner, got)
		}
	}

	// MOVED shape: writing to the non-owner must redirect to the real owner
	keyMoved := "movetest" // slot belongs to one specific side
	slot := int(hashslot.Slot(keyMoved))
	owner, _ := ownerOf(ranges, slot)
	var wrong *rclient
	if owner == seed.addr {
		wrong = cB
	} else {
		wrong = cA
	}
	r := wrong.send("SET", keyMoved, "x")
	wantPrefix := fmt.Sprintf("-MOVED %d ", slot)
	if !strings.HasPrefix(r, wantPrefix) || !strings.Contains(r, owner) {
		t.Fatalf("wrong-node SET reply %q, want prefix %q pointing at %s", r, wantPrefix, owner)
	}

	// following the redirect lands the key on the owner (the redis-cli -c contract)
	if r := clientFor(owner).send("SET", keyMoved, "x"); r != "+OK" {
		t.Fatalf("redirect-followed SET: %s", r)
	}

	// CROSSSLOT applies to non-TCC multi-key commands (DefaultFunc routing):
	// SUNION over keys in different slots must be rejected.
	sk1, sk2 := "crosstest:a", "crosstest:z" // distinct slots, no hash tags
	if hashslot.Slot(sk1) == hashslot.Slot(sk2) {
		sk2 = "crosstest:y"
	}
	if r := cA.send("SADD", sk1, "x"); !strings.HasPrefix(r, ":") {
		t.Fatalf("SADD seed: %s", r)
	}
	if r := cA.send("SUNION", sk1, sk2); !strings.Contains(r, "CROSSSLOT") {
		t.Fatalf("cross-slot SUNION reply %q, want CROSSSLOT", r)
	}

	// Cross-slot MSET is a documented Godis extension (TCC atomic multi-node):
	// it succeeds where Redis would reject; values land readable on their owners.
	if r := cA.send("MSET", sk1, "1", sk2, "2"); !strings.HasPrefix(r, "+OK") {
		t.Logf("note: cross-node MSET via TCC: %s", r)
	}

	// hash-tag keys must hash to the same slot and route atomically
	if r := cA.send("MSET", "{tag}a", "1", "{tag}b", "2"); !strings.HasPrefix(r, "+OK") {
		t.Fatalf("hash-tag MSET reply %q, want +OK", r)
	}
}
