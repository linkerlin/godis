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

func TestM2bvClientListUser(t *testing.T) {
	alice := connection.NewFakeConn()
	bob := connection.NewFakeConn()
	alice.SetACLUser("alice")
	bob.SetACLUser("bob")
	RegisterClient(alice)
	RegisterClient(bob)
	defer UnregisterClient(alice)
	defer UnregisterClient(bob)

	r := execClientListConn(nil, [][]byte{[]byte("USER"), []byte("alice")})
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("CLIENT LIST USER: %T %s", r, r.ToBytes())
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "user=alice") || strings.Contains(s, "user=bob") {
		t.Fatalf("want only alice: %q", s)
	}
}

func TestM2bvObjectFreqRequiresLFU(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{Databases: 16, MaxmemoryPolicy: "allkeys-lru"}
	defer func() { config.Properties = old }()

	db := makeTestDB()
	_ = db.Exec(nil, utils.ToCmdLine("SET", "k", "v"))
	r := db.Exec(nil, utils.ToCmdLine("OBJECT", "FREQ", "k"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "LFU maxmemory policy") {
		t.Fatalf("want LFU policy error: %s", r.ToBytes())
	}

	config.Properties.MaxmemoryPolicy = "allkeys-lfu"
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "FREQ", "k")), 0)
}

func TestM2bvACLCatFastSlowBlocking(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	for _, cat := range []string{"fast", "blocking", "slow"} {
		r := server.Exec(c, utils.ToCmdLine("ACL", "CAT", cat))
		mb, ok := r.(*protocol.MultiBulkReply)
		if !ok || len(mb.Args) == 0 {
			t.Fatalf("ACL CAT %s: %T %s", cat, r, r.ToBytes())
		}
	}
	asserts.AssertErrReply(t, server.Exec(c, utils.ToCmdLine("ACL", "CAT", "@fast")),
		"ERR Unknown category '@fast'")
	fast := server.Exec(c, utils.ToCmdLine("ACL", "CAT", "fast"))
	joined := string(bytesJoin(fast.(*protocol.MultiBulkReply).Args))
	if !strings.Contains(joined, "ping") && !strings.Contains(joined, "get") {
		// ping/get may or may not both be marked fast; at least one known fast cmd
		if !strings.Contains(joined, "auth") && !strings.Contains(joined, "echo") {
			t.Fatalf("fast empty of known cmds: %s", joined)
		}
	}
}

func TestM2bvWaitAOF(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{Databases: 16, AppendOnly: false}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, server.Exec(c, utils.ToCmdLine("WAITAOF", "1", "0", "0")),
		"ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.")

	r := server.Exec(c, utils.ToCmdLine("WAITAOF", "0", "0", "0"))
	raw, ok := r.(*protocol.MultiRawReply)
	if !ok || len(raw.Replies) != 2 {
		t.Fatalf("WAITAOF numlocal=0 AOF=off: %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, raw.Replies[0], 0)
	asserts.AssertIntReply(t, raw.Replies[1], 0)

	config.Properties.AppendOnly = true
	r = server.Exec(c, utils.ToCmdLine("WAITAOF", "1", "0", "0"))
	asserts.AssertIntReply(t, r.(*protocol.MultiRawReply).Replies[0], 1)

	bad := server.Exec(c, utils.ToCmdLine("WAITAOF", "1", "0"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want arity error")
	}
}

func TestM2bvFunctionStatsNested(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "STATS"))
	m, ok := r.(*protocol.MapReply)
	if !ok {
		t.Fatalf("FUNCTION STATS: %T %s", r, r.ToBytes())
	}
	if _, ok := m.Data["running_script"]; !ok {
		t.Fatalf("want running_script key: %v", m.Data)
	}
	engines, ok := m.Data["engines"].(*protocol.MapReply)
	if !ok {
		t.Fatalf("engines should be Map, got %T", m.Data["engines"])
	}
	if _, ok := engines.Data["LUA"].(*protocol.MapReply); !ok {
		t.Fatalf("engines.LUA should be Map, got %T", engines.Data["LUA"])
	}
}
