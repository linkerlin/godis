package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2bmInfoMemoryShallowFields(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("INFO", "memory"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO memory: %T", r)
	}
	s := string(bulk.Arg)
	for _, want := range []string{
		"used_memory_rss_human:",
		"used_memory_peak_perc:",
		"used_memory_startup:",
		"allocator_allocated:",
		"allocator_active:",
		"allocator_resident:",
		"mem_fragmentation_bytes:",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %q in:\n%s", want, s)
		}
	}
}

func TestM2bmMemoryStatsRedisKeys(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	_ = server.Exec(c, utils.ToCmdLine("SET", "k1", "v"))
	r := server.Exec(c, utils.ToCmdLine("MEMORY", "STATS"))
	raw, ok := r.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("MEMORY STATS: %T %s", r, r.ToBytes())
	}
	keys := map[string]bool{}
	for i := 0; i+1 < len(raw.Replies); i += 2 {
		if b, ok := raw.Replies[i].(*protocol.BulkReply); ok {
			keys[string(b.Arg)] = true
		}
	}
	for _, want := range []string{
		"peak.allocated", "total.allocated", "startup.allocated",
		"keys.count", "dataset.bytes", "allocator.allocated", "fragmentation",
	} {
		if !keys[want] {
			t.Fatalf("missing key %q in MEMORY STATS", want)
		}
	}
}

func TestM2bmObjectEmbstr(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "short", "hello")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "short")), "embstr")

	long := strings.Repeat("x", 45)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "long", long)), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "long")), "raw")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "n", "42")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "n")), "int")
}

func TestM2bmFTDefaultDialectAndAggregateCap(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "m2bmft", "ON", "HASH", "PREFIX", "1", "m:",
		"SCHEMA", "t", "TEXT",
	)), "OK")
	for i := 0; i < 5; i++ {
		_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "m2bmft", "m:"+string(rune('a'+i)), "FIELDS", "t", "hello"))
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "DEFAULT_DIALECT", "2")), "OK")
	ok := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "m2bmft", "hello"))
	if protocol.IsErrorReply(ok) {
		t.Fatalf("DEFAULT_DIALECT 2 should work: %s", ok.ToBytes())
	}

	// Force invalid default via map bypass is not possible; SET rejects 99.
	badSet := db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "DEFAULT_DIALECT", "99"))
	if !protocol.IsErrorReply(badSet) {
		t.Fatalf("want reject DEFAULT_DIALECT 99")
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MAXSEARCHRESULTS", "2")), "OK")
	agg := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "m2bmft", "hello", "LIMIT", "0", "10"))
	if protocol.IsErrorReply(agg) {
		t.Fatalf("AGGREGATE: %s", agg.ToBytes())
	}
	// MultiBulk: [total, group1, group2, ...] — with LIMIT capped at 2, at most 2 result rows after total
	mb, ok2 := agg.(*protocol.MultiBulkReply)
	if !ok2 {
		t.Fatalf("AGGREGATE type %T", agg)
	}
	if len(mb.Args) > 3 { // total + ≤2 rows
		t.Fatalf("MAXSEARCHRESULTS 2: want ≤3 elems, got %d", len(mb.Args))
	}
	_ = db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MAXSEARCHRESULTS", "10000"))
	_ = db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "DEFAULT_DIALECT", "1"))
}

func TestM2bmLuaSetrespHGetAllMap(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "h", "a", "1", "b", "2"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('HGETALL', KEYS[1])
return type(t) .. ':' .. tostring(t['a'])
`, "1", "h"))
	asserts.AssertBulkReply(t, r, "table:1")

	r = db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(2)
local t = redis.call('HGETALL', KEYS[1])
return type(t) .. ':' .. tostring(#t)
`, "1", "h"))
	asserts.AssertBulkReply(t, r, "table:4")
}
