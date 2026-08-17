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
		"TYPE-stream|stream|TYPE",
		"EXISTS-after-lpop|0|EXISTS",
		"EXISTS-after-srem|0|EXISTS",
		"EXISTS-after-zrem|0|EXISTS",
		"@skip GEODIST/GEOPOS:",
		"@todo XRANGE",
		"@todo large PFCOUNT",
		"BITOP-DIFF|1|BITOP DIFF",
		"BITOP-ONE|1|BITOP ONE",
		"LCS|mytext|LCS",
		"COPY|1|COPY",
		"MSETNX|1|MSETNX",
		"GETSET|old|GETSET",
		"ZREVRANK|0|ZREVRANK",
		"BITPOS|7|BITPOS",
		"RPUSHX-hit|2|RPUSHX",
		"INCRBYFLOAT|1.5|INCRBYFLOAT",
		"TOUCH-miss|0|TOUCH",
		"RENAME|OK|RENAME",
		"EXPIREAT|1|EXPIREAT",
		"GETEX|v|GETEX",
		"EXPIRETIME|2000000000|EXPIRETIME",
		"LPOS|0|LPOS",
		"ZREMRANGEBYSCORE|2|ZREMRANGEBYSCORE",
		"LMOVE|a|LMOVE",
		"PSETEX|OK|PSETEX",
		"ZRANGE0|a|ZRANGE",
		"HMGET-f1|v1|HMGET",
		"LRANGE0|x|LRANGE",
		"GETDEL-b61|bye|GETDEL",
		"ZRANK-b61|0|ZRANK",
		"ZMSCORE|1|ZMSCORE",
		"ZREVRANGE0|b|ZREVRANGE",
		"SMISMEMBER|1|SMISMEMBER",
		"HINCRBYFLOAT|0.5|HINCRBYFLOAT",
		"PEXPIRETIME|2000000000000|PEXPIRETIME",
		"ZDIFFSTORE|1|ZDIFFSTORE",
		"ZPOPMIN|a\\n1|ZPOPMIN",
		"ZPOPMAX|b\\n2|ZPOPMAX",
		"ZLEXCOUNT|3|ZLEXCOUNT",
		"PFMERGE|OK|PFMERGE",
		"ZRANGEBYSCORE|a|ZRANGEBYSCORE",
		"ZRANGESTORE|2|ZRANGESTORE",
		"ZRANGEBYLEX|a|ZRANGEBYLEX",
		"BITFIELD-set|0|BITFIELD",
		"HEXPIRE-b64|1|HEXPIRE",
		"HTTL-b64|>=1|HTTL",
		"ZREMRANGEBYLEX|2|ZREMRANGEBYLEX",
		"HPEXPIRE-b65|1|HPEXPIRE",
		"HPTTL-b65|>=1|HPTTL",
		"SDIFF|a|SDIFF",
		"ZDIFF|a|ZDIFF",
		"XTRIM-b66|1|XTRIM",
		"ZMPOP|sidecar:b66:zm:{{ID}}\\na\\n1|ZMPOP",
		"LMPOP|sidecar:b66:lm:{{ID}}\\nc|LMPOP",
		"SINTER|b|SINTER",
		"STRLEN-b67|5|STRLEN",
		"APPEND-b67|6|APPEND",
		"GETRANGE-b67|hel|GETRANGE",
		"DECRBY-b67|7|DECRBY",
		"SUNION|a|SUNION",
		"ZREMRANGEBYRANK|2|ZREMRANGEBYRANK",
		"LREM-b68|2|LREM",
		"HSETNX-b68|1|HSETNX",
		"ZREVRANGEBYSCORE|c\\nb\\na|ZREVRANGEBYSCORE",
		"RPOPLPUSH|b|RPOPLPUSH",
		"MGET|a\\nb|MGET",
		"HSTRLEN|5|HSTRLEN",
		"RENAMENX-hit|1|RENAMENX",
		"SINTERCARD|1|SINTERCARD",
		"ZINTERCARD|1|ZINTERCARD",
		"LINSERT-b70|3|LINSERT",
		"LSET-b70|OK|LSET",
		"RPOP-b70|c|RPOP",
		"ZINTER|b|ZINTER",
		"ZUNION|a|ZUNION",
		"MOVE|1|MOVE",
		"LINDEX-b71|b|LINDEX",
		"GEOHASH|sqc8b49rnys0|GEOHASH",
		"SPOP|only|SPOP",
		"HKEYS|f|HKEYS",
		"HVALS|v|HVALS",
		"ZRANDMEMBER|only|ZRANDMEMBER",
		"HRANDFIELD|f|HRANDFIELD",
		"SRANDMEMBER|only|SRANDMEMBER",
		"GEORADIUS-b73|Catania|GEORADIUS",
		"GEOSEARCH-b73|Catania|GEOSEARCH",
		"LCS-LEN|6|LCS",
		"ZRANGE-WS|a\\n1|ZRANGE",
		"ZREVRANGE-WS|b\\n2|ZREVRANGE",
		"EXPIRE-NX|1|EXPIRE",
		"EXPIRE-NX2|0|EXPIRE",
		"SISMEMBER-hit|1|SISMEMBER",
		"SISMEMBER-miss|0|SISMEMBER",
		"GETRANGE-neg|text|GETRANGE",
		"EXPIRE-XX0|0|EXPIRE",
		"EXPIRE-GT|1|EXPIRE",
		"EXPIRE-LT|1|EXPIRE",
		"ZADD-NX|1|ZADD",
		"ZADD-XX|0|ZADD",
		"BITCOUNT-b0|1|BITCOUNT",
		"BITCOUNT-b1|1|BITCOUNT",
		"ZADD-CH|2|ZADD",
		"ZADD-GT|0|ZADD",
		"ZADD-LT|0|ZADD",
		"ZADD-INCR|2.5|ZADD",
		"SREM-b76|1|SREM",
		"GETRANGE-pos|ell|GETRANGE",
		"OBJECT-ENC|embstr|OBJECT",
		"SET-GET|abc|SET",
		"BITFIELD-GET|97|BITFIELD",
		"EXISTS-MULTI|1|EXISTS",
		"LTRIM-b77|OK|LTRIM",
		"ZCOUNT-b77|2|ZCOUNT",
		"ZINCRBY-b77|1.5|ZINCRBY",
		"HMSET-b77|OK|HMSET",
		"HGET-b77|2|HGET",
		"PEXPIREAT-b77|1|PEXPIREAT",
		"PEXPIRETIME-b77|2000000000000|PEXPIRETIME",
		"SETRANGE-b78|5|SETRANGE",
		"GET-b78str|Hello|GET",
		"SUNIONSTORE-b78|3|SUNIONSTORE",
		"SINTERSTORE-b78|1|SINTERSTORE",
		"SDIFFSTORE-b78|1|SDIFFSTORE",
		"SMOVE-b78|1|SMOVE",
		"ZUNIONSTORE-b78|3|ZUNIONSTORE",
		"ZINTERSTORE-b78|1|ZINTERSTORE",
		"HINCRBY-b78|8|HINCRBY",
		"INCRBY-b78|5|INCRBY",
		"SETBIT-b78|0|SETBIT",
		"GETBIT-b78|1|GETBIT",
		"XADD-b78|1-0|XADD",
		"XDEL-b78|1|XDEL",
		"XLEN-b78|0|XLEN",
	} {
		if !strings.Contains(cases, marker) {
			t.Fatalf("r4-1-cases.txt missing honesty/assert marker %q", marker)
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
		if !strings.Contains(s, "expand_want") && !strings.Contains(s, "Expand-WantEscapes") {
			t.Fatalf("%s must expand WANT \\n escapes for multi-line --raw", p)
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
