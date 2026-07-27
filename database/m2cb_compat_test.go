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

func TestM2cbConfigActiverehashingSanitizeIgnoreWarnings(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{Databases: 16, ActiveRehashing: true}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "activerehashing")),
		[]string{"activerehashing", "yes"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "activerehashing", "no")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "activerehashing")),
		[]string{"activerehashing", "no"})

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "sanitize-dump-payload")),
		[]string{"sanitize-dump-payload", "no"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "sanitize-dump-payload", "yes")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "sanitize-dump-payload")),
		[]string{"sanitize-dump-payload", "yes"})

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "ignore-warnings")),
		[]string{"ignore-warnings", ""})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "ignore-warnings", "WARNING1")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "ignore-warnings")),
		[]string{"ignore-warnings", "WARNING1"})

	bad := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "activerehashing", "maybe"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want reject invalid activerehashing")
	}
}

func TestM2cbInfoIoThreadsAndAllocatorFrag(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	clients := server.Exec(c, utils.ToCmdLine("INFO", "clients"))
	cb, ok := clients.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(cb.Arg), "io_threads_active:0") {
		t.Fatalf("INFO clients missing io_threads_active: %s", clients.ToBytes())
	}

	mem := server.Exec(c, utils.ToCmdLine("INFO", "memory"))
	mb, ok := mem.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO memory: %T", mem)
	}
	s := string(mb.Arg)
	for _, want := range []string{"allocator_frag_ratio:", "mem_not_counted_for_evict:0"} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %q: %s", want, s)
		}
	}
}

func TestM2cbDebugSleep(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("DEBUG", "SLEEP", "0")), "OK")

	bad := server.Exec(c, utils.ToCmdLine("DEBUG", "SLEEP", "-1"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want reject negative sleep")
	}
	unknown := server.Exec(c, utils.ToCmdLine("DEBUG", "NOSUCH"))
	if !protocol.IsErrorReply(unknown) || !strings.Contains(string(unknown.ToBytes()), "DEBUG HELP") {
		t.Fatalf("want DEBUG HELP hint: %s", unknown.ToBytes())
	}
	help := server.Exec(c, utils.ToCmdLine("DEBUG", "HELP"))
	if _, ok := help.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("DEBUG HELP: %T", help)
	}
}

func TestM2cbLuaZRandMemberCountSetresp(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1", "a", "2", "b", "3", "c"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('ZRANDMEMBER', KEYS[1], 2)
local n = 0
for _ in pairs(t) do n = n + 1 end
return tostring(n)
`, "1", "z"))
	asserts.AssertBulkReply(t, r, "2")
}
