package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2cgConfigPersistenceStubs(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:                16,
		StopWritesOnBgsaveError:  true,
		RDBCompression:           true,
		RDBChecksum:              true,
		AutoAofRewritePercentage: 100,
		AutoAofRewriteMinSize:    67108864,
	}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "stop-writes-on-bgsave-error")),
		[]string{"stop-writes-on-bgsave-error", "yes"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "stop-writes-on-bgsave-error", "no")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "rdbcompression")),
		[]string{"rdbcompression", "yes"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "rdbcompression", "no")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "rdbchecksum")),
		[]string{"rdbchecksum", "yes"})
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "no-appendfsync-on-rewrite")),
		[]string{"no-appendfsync-on-rewrite", "no"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "no-appendfsync-on-rewrite", "yes")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "auto-aof-rewrite-percentage")),
		[]string{"auto-aof-rewrite-percentage", "100"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "auto-aof-rewrite-percentage", "50")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "auto-aof-rewrite-min-size")),
		[]string{"auto-aof-rewrite-min-size", "67108864"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "auto-aof-rewrite-min-size", "1048576")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "auto-aof-rewrite-min-size")),
		[]string{"auto-aof-rewrite-min-size", "1048576"})
}

func TestM2cgInfoUnblockedWatchedAndCPUMainThread(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	r := server.Exec(c, utils.ToCmdLine("INFO", "clients"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO clients: %T", r)
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "unblocked_clients:0") || !strings.Contains(s, "total_watched_keys:") {
		t.Fatalf("missing clients fields: %s", s)
	}

	r = server.Exec(c, utils.ToCmdLine("INFO", "cpu"))
	bulk, ok = r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO cpu: %T", r)
	}
	s = string(bulk.Arg)
	if !strings.Contains(s, "used_cpu_sys_main_thread:") || !strings.Contains(s, "used_cpu_user_main_thread:") {
		t.Fatalf("missing cpu main_thread fields: %s", s)
	}
}

func TestM2cgACLCatJSONSearchVector(t *testing.T) {
	db := makeTestDB()
	cats := db.Exec(nil, utils.ToCmdLine("ACL", "CAT"))
	mb, ok := cats.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("ACL CAT: %T", cats)
	}
	joined := ""
	for _, a := range mb.Args {
		joined += string(a) + " "
	}
	for _, want := range []string{"@json", "@search", "@vector"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("ACL CAT missing %s: %s", want, joined)
		}
	}

	r := db.Exec(nil, utils.ToCmdLine("ACL", "CAT", "@json"))
	jsonCmds, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(jsonCmds.Args) == 0 {
		t.Fatalf("ACL CAT @json: %T %s", r, r.ToBytes())
	}
	foundSet := false
	for _, a := range jsonCmds.Args {
		if string(a) == "json.set" {
			foundSet = true
			break
		}
	}
	if !foundSet {
		t.Fatalf("ACL CAT @json missing json.set: %s", r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("ACL", "CAT", "@search"))
	searchCmds, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(searchCmds.Args) == 0 {
		t.Fatalf("ACL CAT @search: %T %s", r, r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("ACL", "CAT", "@vector"))
	vecCmds, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(vecCmds.Args) == 0 {
		t.Fatalf("ACL CAT @vector: %T %s", r, r.ToBytes())
	}
	foundVAdd := false
	for _, a := range vecCmds.Args {
		if string(a) == "vadd" {
			foundVAdd = true
			break
		}
	}
	if !foundVAdd {
		t.Fatalf("ACL CAT @vector missing vadd: %s", r.ToBytes())
	}
}

func TestM2cgLuaFloatIncrSetrespNumber(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1", "m"))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "h", "f", "1.5"))
	_ = db.Exec(nil, utils.ToCmdLine("SET", "k", "2.5"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
return type(redis.call('ZINCRBY', KEYS[1], '0.5', 'm'))
`, "1", "z"))
	asserts.AssertBulkReply(t, r, "number")

	r = db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
return type(redis.call('HINCRBYFLOAT', KEYS[1], 'f', '0.25'))
`, "1", "h"))
	asserts.AssertBulkReply(t, r, "number")

	r = db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
return type(redis.call('INCRBYFLOAT', KEYS[1], '1.25'))
`, "1", "k"))
	asserts.AssertBulkReply(t, r, "number")
}
