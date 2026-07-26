package database

import (
	"fmt"
	"strings"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2bnConfigGodisProperties(t *testing.T) {
	oldProps := config.Properties
	config.Properties = &config.ServerProperties{
		UseGnet:          false,
		SearchBackend:    "native",
		VectorBackend:    "native",
		MetricsAddr:      "",
		SearchSQLitePath: "",
		SqliteMmapSize:   0,
		Databases:        16,
	}
	defer func() { config.Properties = oldProps }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "use-gnet", "yes")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "search-backend", "sqlite")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "metrics-addr", ":9090")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "sqlite-mmap-size", "4096")), "OK")

	if !config.Properties.UseGnet || config.Properties.SearchBackend != "sqlite" {
		t.Fatalf("props: gnet=%v backend=%s", config.Properties.UseGnet, config.Properties.SearchBackend)
	}
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "use-gnet")),
		[]string{"use-gnet", "yes"})
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "metrics-addr")),
		[]string{"metrics-addr", ":9090"})

	bad := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "search-backend", "bogus"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want reject bogus backend")
	}
}

func TestM2bnClientListQbufFree(t *testing.T) {
	c := connection.NewFakeConn()
	line := formatClientListLine(c)
	if !strings.Contains(line, "qbuf-free=16384") {
		t.Fatalf("want qbuf-free=16384: %q", line)
	}
}

func TestM2bnMemoryStatsShallowKeys(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("MEMORY", "STATS"))
	raw, ok := r.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("MEMORY STATS: %T", r)
	}
	keys := map[string]bool{}
	for i := 0; i+1 < len(raw.Replies); i += 2 {
		if b, ok := raw.Replies[i].(*protocol.BulkReply); ok {
			keys[string(b.Arg)] = true
		}
	}
	for _, want := range []string{"keys.bytes-per-key", "dataset.percentage", "overhead.hashtable.main"} {
		if !keys[want] {
			t.Fatalf("missing %q", want)
		}
	}
}

func TestM2bnObjectListpackIntset(t *testing.T) {
	db := makeTestDB()
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "h", "a", "1"))
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "h")), "listpack")

	_ = db.Exec(nil, utils.ToCmdLine("SADD", "si", "1", "2", "3"))
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "si")), "intset")

	_ = db.Exec(nil, utils.ToCmdLine("SADD", "ss", "a", "b"))
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "ss")), "listpack")
}

func TestM2bnLuaSetrespConfigAndSmembers(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("SADD", "s", "x", "y"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('SMEMBERS', KEYS[1])
return tostring(t['x'] == true)
`, "1", "s"))
	asserts.AssertBulkReply(t, r, "true")

	// Leave setresp(3) active, then convert a CONFIG-GET-shaped MultiBulk.
	_ = db.Exec(nil, utils.ToCmdLine("EVAL", `redis.setresp(3); return 1`, "0"))
	got := redisReplyToGo(protocol.MakeMultiBulkReply([][]byte{[]byte("hz"), []byte("10")}), "CONFIG")
	m, ok := got.(map[string]interface{})
	if !ok || fmt.Sprint(m["hz"]) != "10" {
		t.Fatalf("CONFIG-shaped map: %#v", got)
	}

	r = db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(2)
local t = redis.call('SMEMBERS', KEYS[1])
return type(t) .. ':' .. tostring(#t)
`, "1", "s"))
	asserts.AssertBulkReply(t, r, "table:2")
}
