package database

import (
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2caLuaSPopCountSetresp(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("SADD", "s", "a", "b", "c", "d"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('SPOP', KEYS[1], 2)
local n = 0
for _ in pairs(t) do n = n + 1 end
return tostring(n)
`, "1", "s"))
	asserts.AssertBulkReply(t, r, "2")
}

func TestM2caConfigLazyfreeUserAndAofLoadStubs(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{Databases: 16}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	for _, key := range []string{
		"lazyfree-lazy-user-del",
		"lazyfree-lazy-user-flush",
		"replica-lazy-flush",
		"aof-load-truncated",
	} {
		asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", key)),
			[]string{key, "no"})
		asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", key, "yes")), "OK")
		asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", key)),
			[]string{key, "yes"})
	}
	bad := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "aof-load-truncated", "maybe"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want reject invalid aof-load-truncated")
	}
}

func TestM2caInfoServerBuildIdAndClock(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	r := server.Exec(c, utils.ToCmdLine("INFO", "server"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO server: %T %s", r, r.ToBytes())
	}
	s := string(bulk.Arg)
	for _, want := range []string{
		"redis_build_id:",
		"gcc_version:0",
		"monotonic_clock:go-time",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %q in INFO server: %s", want, s)
		}
	}
}

func TestM2caSlowlogGetClientNameField(t *testing.T) {
	logger := NewSlowLogger(10, 0)
	start := time.Now().Add(-time.Millisecond)
	logger.Record(start, utils.ToCmdLine("GET", "k"), "127.0.0.1:9", "alice")

	reply := logger.HandleSlowlogCommand(utils.ToCmdLine("SLOWLOG", "GET", "1"))
	mr, ok := reply.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("SLOWLOG GET: %T %s", reply, reply.ToBytes())
	}
	entry, ok := mr.Replies[0].(*protocol.MultiRawReply)
	if !ok || len(entry.Replies) != 6 {
		t.Fatalf("want 6 fields: %T len=%v", mr.Replies[0], len(entry.Replies))
	}
	addr, ok := entry.Replies[4].(*protocol.BulkReply)
	if !ok || string(addr.Arg) != "127.0.0.1:9" {
		t.Fatalf("addr: %v", entry.Replies[4])
	}
	name, ok := entry.Replies[5].(*protocol.BulkReply)
	if !ok || string(name.Arg) != "alice" {
		t.Fatalf("client name: %v", entry.Replies[5])
	}
}

func TestM2caACLDryRunUnsubscribe(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "SETUSER", "unu", "on", "nopass", "+unsubscribe", "+punsubscribe", "+sunsubscribe", "resetchannels", "&news:*",
	)), "OK")

	ok := server.Exec(c, utils.ToCmdLine("ACL", "DRYRUN", "unu", "UNSUBSCRIBE", "news:1"))
	asserts.AssertStatusReply(t, ok, "OK")

	deny := server.Exec(c, utils.ToCmdLine("ACL", "DRYRUN", "unu", "UNSUBSCRIBE", "other"))
	if !protocol.IsErrorReply(deny) || !strings.Contains(string(deny.ToBytes()), "channel") {
		t.Fatalf("want channel deny: %s", deny.ToBytes())
	}
}
