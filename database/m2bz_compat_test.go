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

func TestM2bzScriptExistsIntegerReplies(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)

	sha := db.Exec(nil, utils.ToCmdLine("SCRIPT", "LOAD", "return 1"))
	bulk, ok := sha.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("SCRIPT LOAD: %T %s", sha, sha.ToBytes())
	}

	r := db.Exec(nil, utils.ToCmdLine("SCRIPT", "EXISTS", string(bulk.Arg), "deadbeef"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("SCRIPT EXISTS want MultiRawReply, got %T %s", r, r.ToBytes())
	}
	if len(mr.Replies) != 2 {
		t.Fatalf("want 2 replies, got %d", len(mr.Replies))
	}
	i0, ok := mr.Replies[0].(*protocol.IntReply)
	if !ok || i0.Code != 1 {
		t.Fatalf("sha0 want :1, got %T %s", mr.Replies[0], mr.Replies[0].ToBytes())
	}
	i1, ok := mr.Replies[1].(*protocol.IntReply)
	if !ok || i1.Code != 0 {
		t.Fatalf("sha1 want :0, got %T %s", mr.Replies[1], mr.Replies[1].ToBytes())
	}
}

func TestM2bzHelloResp3ProtoIdIntegers(t *testing.T) {
	c := connection.NewFakeConn()
	r := HelloWithRole(c, utils.ToCmdLine("3"), "master")
	m, ok := r.(*protocol.MapReply)
	if !ok {
		t.Fatalf("HELLO 3: %T %s", r, r.ToBytes())
	}
	proto, ok := m.Data["proto"].(*protocol.IntReply)
	if !ok || proto.Code != 3 {
		t.Fatalf("proto want IntReply 3, got %T %v", m.Data["proto"], m.Data["proto"])
	}
	id, ok := m.Data["id"].(*protocol.IntReply)
	if !ok {
		t.Fatalf("id want IntReply, got %T %v", m.Data["id"], m.Data["id"])
	}
	if id.Code != c.GetClientID() {
		t.Fatalf("id=%d want %d", id.Code, c.GetClientID())
	}
}

func TestM2bzInfoServerGitShaAndSupervised(t *testing.T) {
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
		"redis_git_sha1:",
		"redis_git_dirty:",
		"process_supervised:no",
		"atomicvar_api:go-atomic",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %q in INFO server: %s", want, s)
		}
	}
}

func TestM2bzConfigJemallocAndLazyfreeStubs(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{Databases: 16}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "jemalloc-bg-thread")),
		[]string{"jemalloc-bg-thread", "no"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "jemalloc-bg-thread", "yes")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "jemalloc-bg-thread")),
		[]string{"jemalloc-bg-thread", "yes"})

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "lazyfree-lazy-expire")),
		[]string{"lazyfree-lazy-expire", "no"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "lazyfree-lazy-expire", "yes")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "lazyfree-lazy-expire")),
		[]string{"lazyfree-lazy-expire", "yes"})

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "lazyfree-lazy-server-del")),
		[]string{"lazyfree-lazy-server-del", "no"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "lazyfree-lazy-server-del", "yes")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "lazyfree-lazy-server-del")),
		[]string{"lazyfree-lazy-server-del", "yes"})

	bad := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "jemalloc-bg-thread", "maybe"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want reject invalid jemalloc-bg-thread")
	}
}

func TestM2bzLuaZUnionSetWithoutScores(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z1", "1", "a", "2", "b"))
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z2", "1", "a", "3", "c"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('ZUNION', 2, KEYS[1], KEYS[2])
return tostring(t['a'] == true) .. ':' .. tostring(t['c'] == true) .. ':' .. tostring(t['b'] == true)
`, "2", "z1", "z2"))
	asserts.AssertBulkReply(t, r, "true:true:true")
}
