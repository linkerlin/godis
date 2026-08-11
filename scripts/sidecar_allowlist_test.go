package scripts_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestR41SidecarAllowlistScaffold locks the R4-1 honesty contract in the
// shell/PowerShell scaffolds: allowlist is fixed and does not claim a full suite.
func TestR41SidecarAllowlistScaffold(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	shPath := filepath.Join(dir, "redis-sidecar-diff.sh")
	psPath := filepath.Join(dir, "redis-sidecar-diff.ps1")
	for _, p := range []string{shPath, psPath} {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		s := string(b)
		if !strings.Contains(s, "NOT a full compatibility suite") {
			t.Fatalf("%s must disclaim full suite", p)
		}
		for _, cmd := range []string{
			"PING", "ECHO", "SET", "GET", "STRLEN", "APPEND", "DEL", "EXISTS", "INCR", "DECR", "TYPE",
			"HSET", "HGET", "HLEN", "HEXISTS", "HDEL",
			"LPUSH", "LLEN", "LINDEX", "LPOP",
			"--raw",
		} {
			if !strings.Contains(s, cmd) {
				t.Fatalf("%s missing allowlist cmd %s", p, cmd)
			}
		}
		for _, banned := range []string{"DUMP", "RESTORE", "FT.SEARCH", "CLUSTER MEET", "FUNCTION"} {
			// Comment may mention out-of-scope names; live invocations must not.
			// Heuristic: no line that invokes them as redis-cli/Assert args without "Out of scope".
			for _, line := range strings.Split(s, "\n") {
				trim := strings.TrimSpace(line)
				if strings.HasPrefix(trim, "#") || strings.HasPrefix(trim, "//") {
					continue
				}
				if strings.Contains(strings.ToUpper(line), "OUT OF SCOPE") {
					continue
				}
				if strings.Contains(line, banned) && !strings.Contains(strings.ToLower(line), "out of scope") {
					// Allow documentation echo lines that mention dumps as excluded.
					if strings.Contains(line, "echo") || strings.Contains(line, "Write-Host") {
						continue
					}
					t.Fatalf("%s must not invoke %s outside out-of-scope docs: %s", p, banned, trim)
				}
			}
		}
		if !strings.Contains(s, "--selfcheck") && !strings.Contains(s, "SelfCheck") {
			t.Fatalf("%s missing selfcheck mode", p)
		}
	}
}
