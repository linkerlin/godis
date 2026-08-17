package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// TestFTAggregateSortByMulti applies secondary SORTBY keys (Redis 8.x).
func TestFTAggregateSortByMulti(t *testing.T) {
	db := makeTestDB()
	create := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "sbmulti", "ON", "HASH", "PREFIX", "1", "sbm:",
		"SCHEMA", "a", "NUMERIC", "SORTABLE", "b", "NUMERIC", "SORTABLE", "c", "TAG", "SORTABLE",
	))
	if protocol.IsErrorReply(create) {
		t.Fatalf("FT.CREATE: %s", create.ToBytes())
	}
	for _, kv := range []struct{ k, a, b, c string }{
		{"sbm:1", "1", "2", "x"},
		{"sbm:2", "1", "1", "y"},
		{"sbm:3", "2", "9", "z"},
		{"sbm:4", "1", "2", "a"},
	} {
		if r := db.Exec(nil, utils.ToCmdLine("HSET", kv.k, "a", kv.a, "b", kv.b, "c", kv.c)); protocol.IsErrorReply(r) {
			t.Fatalf("HSET %s: %s", kv.k, r.ToBytes())
		}
	}

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "sbmulti", "*",
		"LOAD", "3", "@a", "@b", "@c",
		"SORTBY", "4", "@a", "ASC", "@b", "DESC",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("multi SORTBY: %s", r.ToBytes())
	}
	mr, ok := unwrapFTAggregate(r)
	if !ok {
		t.Fatalf("want multi-bulk, got %T %s", r, r.ToBytes())
	}
	if len(mr.Args) != 5 || string(mr.Args[0]) != "4" {
		t.Fatalf("want Total=4 + 4 rows, got %d elems %v", len(mr.Args), mr.Args)
	}
	rows := make([]map[string]string, 0, 4)
	for _, raw := range mr.Args[1:] {
		rows = append(rows, parseAggRowFields(raw))
	}
	// @a ASC @b DESC → (1,2), (1,2), (1,1), (2,9)
	if rows[0]["a"] != "1" || rows[0]["b"] != "2" {
		t.Fatalf("row0 want a=1 b=2, got %v", rows[0])
	}
	if rows[1]["a"] != "1" || rows[1]["b"] != "2" {
		t.Fatalf("row1 want a=1 b=2, got %v", rows[1])
	}
	if rows[2]["a"] != "1" || rows[2]["b"] != "1" {
		t.Fatalf("row2 want a=1 b=1 (secondary DESC), got %v", rows[2])
	}
	if rows[3]["a"] != "2" {
		t.Fatalf("row3 want a=2, got %v", rows[3])
	}

	// Directions omitted → both ASC: first row b=1.
	asc := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "sbmulti", "*",
		"LOAD", "2", "@a", "@b",
		"SORTBY", "2", "@a", "@b",
	))
	am, ok := unwrapFTAggregate(asc)
	if !ok {
		t.Fatalf("SORTBY 2 @a @b: %T %s", asc, asc.ToBytes())
	}
	first := parseAggRowFields(am.Args[1])
	if first["b"] != "1" {
		t.Fatalf("dual ASC want first b=1, got %v full=%s", first, asc.ToBytes())
	}

	// Multi + MAX: top-2 after sort; Total stays pre-MAX.
	mx := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "sbmulti", "*",
		"LOAD", "2", "@a", "@b",
		"SORTBY", "4", "@a", "ASC", "@b", "DESC", "MAX", "2",
	))
	mm, ok := unwrapFTAggregate(mx)
	if !ok {
		t.Fatalf("multi MAX: %T %s", mx, mx.ToBytes())
	}
	if string(mm.Args[0]) != "4" || len(mm.Args) != 3 {
		t.Fatalf("want Total=4 and 2 rows, got %s", mx.ToBytes())
	}
	if !strings.Contains(string(mx.ToBytes()), "1") {
		t.Fatalf("MAX rows should still include a=1: %s", mx.ToBytes())
	}

	// Tertiary key breaks a/b ties (Redis 8.x).
	tri := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "sbmulti", "*",
		"LOAD", "3", "@a", "@b", "@c",
		"SORTBY", "6", "@a", "ASC", "@b", "DESC", "@c", "ASC",
	))
	tm, ok := unwrapFTAggregate(tri)
	if !ok {
		t.Fatalf("tertiary SORTBY: %T %s", tri, tri.ToBytes())
	}
	t0 := parseAggRowFields(tm.Args[1])
	t1 := parseAggRowFields(tm.Args[2])
	if t0["a"] != "1" || t0["b"] != "2" || t0["c"] != "a" {
		t.Fatalf("tertiary row0 want a=1 b=2 c=a, got %v", t0)
	}
	if t1["a"] != "1" || t1["b"] != "2" || t1["c"] != "x" {
		t.Fatalf("tertiary row1 want a=1 b=2 c=x, got %v", t1)
	}

	bad := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "sbmulti", "*",
		"SORTBY", "3", "@a", "ASC",
	))
	if !protocol.IsErrorReply(bad) || !strings.Contains(string(bad.ToBytes()), "Bad arguments for SORTBY") {
		t.Fatalf("short SORTBY nargs: %s", bad.ToBytes())
	}
}

func parseAggRowFields(row []byte) map[string]string {
	out := make(map[string]string)
	chunks := splitNestedRESPBulks(string(row))
	for i := 0; i+1 < len(chunks); i += 2 {
		k := strings.TrimPrefix(chunks[i], "@")
		out[k] = chunks[i+1]
	}
	return out
}

func splitNestedRESPBulks(s string) []string {
	var out []string
	for len(s) > 0 {
		switch s[0] {
		case '*':
			if idx := strings.Index(s, "\r\n"); idx >= 0 {
				s = s[idx+2:]
				continue
			}
			return out
		case '$':
			idx := strings.Index(s, "\r\n")
			if idx < 0 {
				return out
			}
			n := 0
			for _, c := range s[1:idx] {
				if c < '0' || c > '9' {
					return out
				}
				n = n*10 + int(c-'0')
			}
			s = s[idx+2:]
			if n < 0 || n > len(s) {
				return out
			}
			out = append(out, s[:n])
			s = s[n:]
			if strings.HasPrefix(s, "\r\n") {
				s = s[2:]
			}
		default:
			return out
		}
	}
	return out
}
