package database

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func newM2uServer(t *testing.T) *Server {
	t.Helper()
	config.Properties = &config.ServerProperties{
		Databases:  1,
		AppendOnly: false,
	}
	server, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	return server
}

func TestM2uNoEvictionOOM(t *testing.T) {
	server := newM2uServer(t)
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-policy", "noeviction")), "OK")
	// Allow ~3 keys (128 bytes each estimate)
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory", "384")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "a", "1")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "b", "1")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "c", "1")), "OK")
	oom := server.Exec(c, utils.ToCmdLine("SET", "d", "1"))
	if !protocol.IsErrorReply(oom) || !strings.Contains(string(oom.ToBytes()), "OOM") {
		t.Fatalf("expected OOM, got %s", oom.ToBytes())
	}
}

func TestM2uAllKeysRandomEvict(t *testing.T) {
	server := newM2uServer(t)
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-policy", "allkeys-random")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory", "384")), "OK")
	for i := 0; i < 3; i++ {
		asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "k"+strconv.Itoa(i), "1")), "OK")
	}
	ok := server.Exec(c, utils.ToCmdLine("SET", "new", "1"))
	asserts.AssertStatusReply(t, ok, "OK")
	db := server.dbSet[0].Load().(*DB)
	if db.data.Len() > 3 {
		t.Fatalf("expected eviction to keep <=3 keys, got %d", db.data.Len())
	}
	if _, exists := db.GetEntity("new"); !exists {
		t.Fatal("new key should exist after eviction write")
	}
}

func TestM2uConfigExtraDirectives(t *testing.T) {
	server := newM2uServer(t)
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "timeout", "60")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "protected-mode", "yes")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "loglevel", "notice")), "OK")
	got := server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "timeout"))
	multi, ok := got.(*protocol.MultiBulkReply)
	if !ok || len(multi.Args) < 2 || string(multi.Args[1]) != "60" {
		t.Fatalf("CONFIG GET timeout: %s", got.ToBytes())
	}
	bad := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-policy", "bogus"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("bogus policy should fail")
	}
}

func TestM2uObjectIdleTime(t *testing.T) {
	server := newM2uServer(t)
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "idle", "1")), "OK")
	server.Exec(c, utils.ToCmdLine("GET", "idle"))
	time.Sleep(1100 * time.Millisecond)
	r := server.Exec(c, utils.ToCmdLine("OBJECT", "IDLETIME", "idle"))
	ir, ok := r.(*protocol.IntReply)
	if !ok || ir.Code < 1 {
		t.Fatalf("IDLETIME expected >=1, got %T %v", r, r)
	}
}
