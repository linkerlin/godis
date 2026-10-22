// Package e6chaos hosts the E6-3 crash-consistency / fault-injection tests.
// They spawn the real godis binary as child processes, kill it hard
// (Process.Kill == TerminateProcess on Windows, SIGKILL semantics) at
// deliberate moments, restart it on the same data dir, and assert the
// invariant: every acknowledged write is still there, or the server refuses
// to start with an explicit error — never silent loss.
//
// Local:  go test ./e6chaos -v -timeout 15m
// Nightly: runs as part of `go test -race -shuffle=on ./...` (skipped in -short).
package e6chaos

import (
	"bufio"
	"bytes"
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
)

var (
	binOnce sync.Once
	binPath string
	binErr  error
)

func godisBin(t *testing.T) string {
	t.Helper()
	binOnce.Do(func() {
		dir, err := os.MkdirTemp("", "godis-e6chaos-*")
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
		out, err := cmd.CombinedOutput()
		if err != nil {
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

// ---- minimal RESP client (synchronous, one command per round trip) ----

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
		if _, err := readFull(c.r, buf); err != nil {
			return "<IOERR>"
		}
		return string(buf[:n])
	case '*':
		n, _ := strconv.Atoi(line[1:])
		parts := make([]string, n)
		for i := 0; i < n; i++ {
			parts[i] = c.send() // nested reads share the same reader
		}
		return strings.Join(parts, "\n")
	}
	return line
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// ---- child-process server management ----

type childServer struct {
	cmd  *exec.Cmd
	addr string
	out  *bytes.Buffer
	dir  string
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

// startServer writes a config into dir and boots a fresh godis child.
// extra conf lines are appended verbatim.
func startServer(t *testing.T, dir string, extra ...string) *childServer {
	t.Helper()
	port := freePort(t)
	conf := filepath.Join(dir, fmt.Sprintf("conf-%d.conf", port))
	lines := append([]string{
		"bind 127.0.0.1",
		fmt.Sprintf("port %d", port),
		"dir " + filepath.ToSlash(dir),
		"save \"\"",
		"loglevel notice",
	}, extra...)
	if err := os.WriteFile(conf, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cs := &childServer{addr: fmt.Sprintf("127.0.0.1:%d", port), dir: dir}
	cs.out = &bytes.Buffer{}
	cs.cmd = exec.Command(godisBin(t), "-c", conf)
	cs.cmd.Dir = dir // data files (AOF/RDB) resolve against the data dir
	cs.cmd.Stdout = cs.out
	cs.cmd.Stderr = cs.out
	if err := cs.cmd.Start(); err != nil {
		t.Fatalf("start child: %v\nlogs:\n%s", err, cs.out.String())
	}
	cs.waitUp(t, 15*time.Second)
	t.Cleanup(func() { cs.stop() })
	return cs
}

func (cs *childServer) waitUp(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", cs.addr, 500*time.Millisecond)
		if err == nil {
			cl := &rclient{conn: c, r: bufio.NewReader(c)}
			if r := cl.send("PING"); strings.Contains(r, "PONG") {
				_ = c.Close()
				return
			}
			_ = c.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	cs.stop()
	t.Fatalf("server %s did not come up\nlogs:\n%s", cs.addr, cs.out)
}

func (cs *childServer) client(t *testing.T) *rclient {
	t.Helper()
	c, err := net.DialTimeout("tcp", cs.addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", cs.addr, err)
	}
	return &rclient{conn: c, r: bufio.NewReader(c)}
}

// kill is the kill -9 equivalent: TerminateProcess on Windows, SIGKILL on unix.
func (cs *childServer) kill() {
	if cs.cmd.Process != nil {
		_ = cs.cmd.Process.Kill()
	}
	_, _ = cs.cmd.Process.Wait()
}

func (cs *childServer) stop() {
	if cs.cmd.Process != nil {
		_ = cs.cmd.Process.Kill()
	}
	_, _ = cs.cmd.Process.Wait()
}

func (cs *childServer) dieWithLogs(t *testing.T, msg string) {
	t.Helper()
	cs.stop()
	t.Fatalf("%s\nlogs:\n%s", msg, cs.out.String())
}

// ---- shared scenario helpers ----

func skipShort(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test skipped in short mode")
	}
}

// loadKeys writes n keys with SET, asserting every +OK (the acknowledgement
// the zero-loss invariant is measured against).
func loadKeys(t *testing.T, cs *childServer, prefix string, n int) {
	t.Helper()
	cl := cs.client(t)
	defer cl.close()
	for i := 0; i < n; i++ {
		if r := cl.send("SET", fmt.Sprintf("%s%d", prefix, i), fmt.Sprintf("v%d", i)); r != "+OK" {
			cs.dieWithLogs(t, fmt.Sprintf("SET %s%d not acknowledged: %s", prefix, i, r))
		}
	}
}

func dbsize(t *testing.T, cs *childServer) int {
	t.Helper()
	cl := cs.client(t)
	defer cl.close()
	return atoiReply(t, cl.send("DBSIZE"))
}

func atoiReply(t *testing.T, r string) int {
	t.Helper()
	n, err := strconv.Atoi(strings.TrimPrefix(r, ":"))
	if err != nil {
		t.Fatalf("not an integer reply: %q", r)
	}
	return n
}

func waitDbsize(t *testing.T, cs *childServer, want int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if dbsize(t, cs) >= want {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

func infoContains(t *testing.T, cs *childServer, sub string) bool {
	t.Helper()
	cl := cs.client(t)
	defer cl.close()
	return strings.Contains(cl.send("INFO", "replication"), sub)
}

// assertAllKeysPresent verifies every acknowledged key survived a restart.
func assertAllKeysPresent(t *testing.T, cs *childServer, prefix string, n int) {
	t.Helper()
	cl := cs.client(t)
	defer cl.close()
	missing := 0
	for i := 0; i < n; i++ {
		if got := cl.send("GET", fmt.Sprintf("%s%d", prefix, i)); got != fmt.Sprintf("v%d", i) {
			missing++
			if missing <= 5 {
				t.Errorf("key %s%d lost/changed: got %q", prefix, i, got)
			}
		}
	}
	if missing > 0 {
		cs.dieWithLogs(t, fmt.Sprintf("%d/%d acknowledged keys lost", missing, n))
	}
}

// ---- scenarios ----

// TestE6ChaosKillDuringRewrite kills the server mid-AOF-rewrite (several kill
// timings) and asserts no acknowledged write is lost after restart.
func TestE6ChaosKillDuringRewrite(t *testing.T) {
	skipShort(t)
	for _, fsync := range []string{"everysec", "always"} {
		for _, delay := range []time.Duration{30 * time.Millisecond, 90 * time.Millisecond, 200 * time.Millisecond} {
			t.Run(fmt.Sprintf("%s/kill@%s", fsync, delay), func(t *testing.T) {
				dir := t.TempDir()
				cs := startServer(t, dir,
					"appendonly yes",
					"appendfsync "+fsync,
					"appendfilename appendonly.aof",
				)
				const n = 8000
				loadKeys(t, cs, "k", n)
				if fsync == "everysec" {
					time.Sleep(1300 * time.Millisecond) // let the fsync window close
				}
				cs.client(t).send("BGREWRITEAOF")
				time.Sleep(delay)
				cs.kill() // kill -9 mid-rewrite (or right after — both must be safe)

				cs2 := startServer(t, dir,
					"appendonly yes",
					"appendfsync "+fsync,
					"appendfilename appendonly.aof",
				)
				if got := dbsize(t, cs2); got < n {
					cs2.dieWithLogs(t, fmt.Sprintf("DBSIZE %d < %d after kill-mid-rewrite restart", got, n))
				}
				assertAllKeysPresent(t, cs2, "k", n)
			})
		}
	}
}

// TestE6ChaosKillDuringBGSAVE kills the server mid-BGSAVE; with AOF enabled
// the AOF must remain the source of truth and survive intact.
func TestE6ChaosKillDuringBGSAVE(t *testing.T) {
	skipShort(t)
	dir := t.TempDir()
	cs := startServer(t, dir,
		"appendonly yes",
		"appendfsync everysec",
		"appendfilename appendonly.aof",
		"dbfilename dump.rdb",
	)
	const n = 5000
	loadKeys(t, cs, "k", n)
	time.Sleep(1300 * time.Millisecond)
	cs.client(t).send("BGSAVE")
	time.Sleep(50 * time.Millisecond)
	cs.kill()

	cs2 := startServer(t, dir,
		"appendonly yes",
		"appendfsync everysec",
		"appendfilename appendonly.aof",
		"dbfilename dump.rdb",
	)
	if got := dbsize(t, cs2); got < n {
		cs2.dieWithLogs(t, fmt.Sprintf("DBSIZE %d < %d after kill-mid-bgsave restart", got, n))
	}
	assertAllKeysPresent(t, cs2, "k", n)
}

// TestE6ChaosTruncatedAofRestart simulates a torn write (what a failed fsync
// leaves behind): append a partial command to a complete AOF, restart, and
// require either a successful prefix load or an explicit startup error.
func TestE6ChaosTruncatedAofRestart(t *testing.T) {
	skipShort(t)
	dir := t.TempDir()
	cs := startServer(t, dir,
		"appendonly yes",
		"appendfsync always",
		"appendfilename appendonly.aof",
	)
	const n = 3000
	loadKeys(t, cs, "k", n)
	cs.kill() // fsync=always: AOF on disk is complete at this point

	aof := filepath.Join(dir, "appendonly.aof")
	f, err := os.OpenFile(aof, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("*3\r\n$3\r\nSET\r\n$5\r\nlost"); err != nil { // torn tail
		t.Fatal(err)
	}
	_ = f.Close()

	cs2 := startServer(t, dir,
		"appendonly yes",
		"appendfsync everysec",
		"appendfilename appendonly.aof",
	)
	if got := dbsize(t, cs2); got < n {
		cs2.dieWithLogs(t, fmt.Sprintf("DBSIZE %d < %d: torn AOF tail destroyed acknowledged data", got, n))
	}
	assertAllKeysPresent(t, cs2, "k", n)
}

// TestE6ChaosSlaveInterruptResync kills a replica mid-replication (after full
// sync, during incremental stream), restarts it on the same dir, and asserts
// it reconnects as a replica and converges.
func TestE6ChaosSlaveInterruptResync(t *testing.T) {
	skipShort(t)
	mdir, sdir := t.TempDir(), t.TempDir()
	master := startServer(t, mdir,
		"appendonly yes", "appendfsync everysec", "appendfilename appendonly.aof")
	const first = 2000
	loadKeys(t, master, "k", first)

	slave := startServer(t, sdir, "appendonly yes", "appendfsync always", "appendfilename appendonly.aof")
	slave.client(t).send("REPLICAOF", "127.0.0.1", strings.TrimPrefix(master.addr, "127.0.0.1:"))
	if !waitDbsize(t, slave, first, 30*time.Second) {
		slave.dieWithLogs(t, "slave did not complete initial sync")
	}

	// incremental phase, then hard-kill the slave mid-stream
	loadKeys(t, master, "j", 2000)
	time.Sleep(300 * time.Millisecond)
	slave.kill()

	slave2 := startServer(t, sdir, "appendonly yes", "appendfsync always", "appendfilename appendonly.aof")
	slave2.client(t).send("REPLICAOF", "127.0.0.1", strings.TrimPrefix(master.addr, "127.0.0.1:"))
	if !waitDbsize(t, slave2, first+2000, 30*time.Second) {
		slave2.dieWithLogs(t, "restarted slave failed to re-sync to master state")
	}
	if !infoContains(t, slave2, "role:slave") {
		slave2.dieWithLogs(t, "restarted slave did not stay in replica role")
	}
}

// TestE6ChaosMasterDownSlaveStaysReplica pins the E1-2 guarantee: losing the
// master must not silently promote a replica to master.
func TestE6ChaosMasterDownSlaveStaysReplica(t *testing.T) {
	skipShort(t)
	mdir, sdir := t.TempDir(), t.TempDir()
	master := startServer(t, mdir,
		"appendonly yes", "appendfsync everysec", "appendfilename appendonly.aof")
	const n = 1000
	loadKeys(t, master, "k", n)

	slave := startServer(t, sdir, "appendonly yes", "appendfsync always", "appendfilename appendonly.aof")
	slave.client(t).send("REPLICAOF", "127.0.0.1", strings.TrimPrefix(master.addr, "127.0.0.1:"))
	if !waitDbsize(t, slave, n, 30*time.Second) {
		slave.dieWithLogs(t, "initial sync incomplete")
	}

	master.kill() // master loss
	time.Sleep(3 * time.Second)
	if !infoContains(t, slave, "role:slave") {
		slave.dieWithLogs(t, "slave self-promoted after master loss (must stay replica and retry)")
	}
}
