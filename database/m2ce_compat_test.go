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

func TestM2ceConfigReplicaOfString(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:             16,
		ReplicaServeStaleData: true,
		ReplicaPriority:       100,
	}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "replicaof")),
		[]string{"replicaof", ""})
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "slaveof")),
		[]string{"slaveof", ""})

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replicaof", "127.0.0.1 6380")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "replicaof")),
		[]string{"replicaof", "127.0.0.1 6380"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replicaof", "no one")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "replicaof")),
		[]string{"replicaof", ""})

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "replica-priority")),
		[]string{"replica-priority", "100"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replica-priority", "50")), "OK")

	bad := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replicaof", "onlyhost"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want reject invalid replicaof")
	}
}

func TestM2ceInfoLastCowAndReplKbps(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	pers := server.Exec(c, utils.ToCmdLine("INFO", "persistence"))
	pb, ok := pers.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO persistence: %T", pers)
	}
	ps := string(pb.Arg)
	if !strings.Contains(ps, "rdb_last_cow_size:0") || !strings.Contains(ps, "aof_last_cow_size:0") {
		t.Fatalf("missing last_cow fields: %s", ps)
	}

	repl := server.Exec(c, utils.ToCmdLine("INFO", "replication"))
	rb, ok := repl.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO replication: %T", repl)
	}
	rs := string(rb.Arg)
	if !strings.Contains(rs, "instantaneous_input_repl_kbps:0.00") ||
		!strings.Contains(rs, "instantaneous_output_repl_kbps:0.00") {
		t.Fatalf("missing repl kbps: %s", rs)
	}
}

func TestM2ceACLCatAll(t *testing.T) {
	db := makeTestDB()
	cats := db.Exec(nil, utils.ToCmdLine("ACL", "CAT"))
	mb, ok := cats.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("ACL CAT: %T", cats)
	}
	found := false
	for _, a := range mb.Args {
		if string(a) == "read" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("ACL CAT missing all-or-read: %s", cats.ToBytes())
	}

	r := db.Exec(nil, utils.ToCmdLine("ACL", "CAT", "read"))
	all, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(all.Args) < 10 {
		t.Fatalf("ACL CAT all: %T n=%d %s", r, len(all.Args), r.ToBytes())
	}
	joined := string(bytesJoin(all.Args))
	if !strings.Contains(joined, "get") || !strings.Contains(joined, "exists") {
		t.Fatalf("ACL CAT read missing get/exists: %s", joined)
	}
}

func TestM2ceLuaZScoreZmscoreSetrespNumber(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1.5", "a", "2.25", "b"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local s = redis.call('ZSCORE', KEYS[1], 'a')
return type(s) .. ':' .. tostring(s)
`, "1", "z"))
	asserts.AssertBulkReply(t, r, "number:1.5")

	r = db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('ZMSCORE', KEYS[1], 'a', 'b')
return type(t[1]) .. ':' .. tostring(t[1]+t[2])
`, "1", "z"))
	asserts.AssertBulkReply(t, r, "number:3.75")
}
