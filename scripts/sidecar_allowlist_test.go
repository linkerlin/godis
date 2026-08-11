package scripts_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestR41SidecarAllowlistScaffold locks the R4-1 honesty contract: cases file
// drives allowlist; scripts do not claim a full suite / FT / DUMP / cluster.
func TestR41SidecarAllowlistScaffold(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	dir := filepath.Dir(file)
	casesPath := filepath.Join(dir, "r4-1-cases.txt")
	shPath := filepath.Join(dir, "redis-sidecar-diff.sh")
	psPath := filepath.Join(dir, "redis-sidecar-diff.ps1")

	casesBytes, err := os.ReadFile(casesPath)
	if err != nil {
		t.Fatalf("read cases: %v", err)
	}
	cases := string(casesBytes)
	if !strings.Contains(cases, "@allowlist") {
		t.Fatal("r4-1-cases.txt missing @allowlist")
	}
	for _, cmd := range []string{
		"PING", "ECHO", "SET", "GET", "STRLEN", "APPEND", "DEL", "EXISTS", "INCR", "DECR", "TYPE",
		"HSET", "HGET", "HLEN", "HEXISTS", "HDEL",
		"LPUSH", "LLEN", "LINDEX", "LPOP",
		"SADD", "SCARD", "SISMEMBER", "SREM",
		"ZADD", "ZSCORE", "ZCARD", "ZREM",
		"TTL", "PTTL", "PEXPIRE", "EXPIRE", "PERSIST",
		"XADD", "XLEN",
		"GEOADD",
		"SETBIT", "GETBIT", "BITCOUNT", "BITOP",
		"PFADD", "PFCOUNT",
	} {
		if !strings.Contains(cases, cmd) {
			t.Fatalf("r4-1-cases.txt missing allowlist cmd %s", cmd)
		}
	}
	for _, banned := range []string{"DUMP", "RESTORE", "FT.SEARCH", "CLUSTER MEET", "FUNCTION", "SMEMBERS", "HGETALL"} {
		if lineInvokesBanned(cases, banned) {
			t.Fatalf("r4-1-cases.txt must not invoke %s", banned)
		}
	}
	if !strings.Contains(cases, "Out of scope") && !strings.Contains(cases, "out of scope") {
		t.Fatal("r4-1-cases.txt must document out-of-scope surface")
	}
	// Honesty markers for known gaps (must not be silently turned into PASS asserts).
	for _, marker := range []string{
		"@skip TYPE stream:",
		"@skip GEODIST/GEOPOS:",
		"@todo XRANGE",
		"@todo large PFCOUNT",
	} {
		if !strings.Contains(cases, marker) {
			t.Fatalf("r4-1-cases.txt missing honesty marker %q", marker)
		}
	}

	for _, p := range []string{shPath, psPath} {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		s := string(b)
		if !strings.Contains(s, "NOT a full compatibility suite") {
			t.Fatalf("%s must disclaim full suite", p)
		}
		if !strings.Contains(s, "r4-1-cases.txt") {
			t.Fatalf("%s must load r4-1-cases.txt", p)
		}
		if !strings.Contains(s, "--raw") {
			t.Fatalf("%s must use redis-cli --raw", p)
		}
		if !strings.Contains(s, "FAIL") {
			t.Fatalf("%s must emit readable FAIL diagnostics", p)
		}
		if !strings.Contains(s, "@skip") || !strings.Contains(s, "@todo") {
			t.Fatalf("%s must honor @skip/@todo markers", p)
		}
		for _, banned := range []string{"DUMP", "RESTORE", "FT.SEARCH", "CLUSTER MEET", "FUNCTION"} {
			if lineInvokesBanned(s, banned) {
				t.Fatalf("%s must not invoke %s outside docs", p, banned)
			}
		}
		if !strings.Contains(s, "--selfcheck") && !strings.Contains(s, "SelfCheck") {
			t.Fatalf("%s missing selfcheck mode", p)
		}
	}
}

func lineInvokesBanned(src, banned string) bool {
	for _, line := range strings.Split(src, "\n") {
		trim := strings.TrimSpace(line)
		if trim == "" || strings.HasPrefix(trim, "#") || strings.HasPrefix(trim, "//") {
			continue
		}
		if strings.Contains(strings.ToUpper(line), "OUT OF SCOPE") {
			continue
		}
		if strings.Contains(line, banned) && !strings.Contains(strings.ToLower(line), "out of scope") {
			if strings.Contains(line, "echo") || strings.Contains(line, "Write-Host") {
				continue
			}
			// @allowlist / disclaimer / @skip / @todo lines may name excluded surfaces.
			if strings.Contains(line, "@allowlist") || strings.Contains(strings.ToLower(line), "full suite") {
				continue
			}
			if strings.HasPrefix(trim, "@skip") || strings.HasPrefix(trim, "@todo") {
				continue
			}
			return true
		}
	}
	return false
}
