package database

import (
	"strconv"
	"strings"
	"sync"
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

// TestM2uEvictPropagatesToAOF ensures maxmemory eviction writes DEL to AOF
// (replication backlog is AOF-fed), matching key expiry propagation.
func TestM2uEvictPropagatesToAOF(t *testing.T) {
	server := newM2uServer(t)
	c := connection.NewFakeConn()
	db := server.dbSet[0].Load().(*DB)
	var mu sync.Mutex
	var aofLines []CmdLine
	db.addAof = func(line CmdLine) {
		mu.Lock()
		aofLines = append(aofLines, line)
		mu.Unlock()
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-policy", "allkeys-random")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory", "384")), "OK")
	for i := 0; i < 3; i++ {
		asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "e"+strconv.Itoa(i), "1")), "OK")
	}
	// Trigger eviction.
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "trigger", "1")), "OK")

	mu.Lock()
	defer mu.Unlock()
	foundDel := false
	for _, line := range aofLines {
		if len(line) >= 2 && string(line[0]) == "del" {
			foundDel = true
			break
		}
	}
	if !foundDel {
		t.Fatalf("eviction should propagate DEL to AOF, got %v", aofLines)
	}
}

// Large SET value must trip maxmemory (not undercounted as 128B/key).
func TestM2uLargeValueMaxmemory(t *testing.T) {
	server := newM2uServer(t)
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-policy", "noeviction")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory", "1024")), "OK")
	big := strings.Repeat("x", 4096)
	oom := server.Exec(c, utils.ToCmdLine("SET", "big", big))
	if !protocol.IsErrorReply(oom) || !strings.Contains(string(oom.ToBytes()), "OOM") {
		t.Fatalf("expected OOM for large SET, got %s", oom.ToBytes())
	}

	// Stored large value also counted in used estimate.
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory", "0")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "stored", big)), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory", "2048")), "OK")
	oom2 := server.Exec(c, utils.ToCmdLine("SET", "tiny", "1"))
	if !protocol.IsErrorReply(oom2) || !strings.Contains(string(oom2.ToBytes()), "OOM") {
		t.Fatalf("expected OOM after large value stored, got %s", oom2.ToBytes())
	}
	used := server.approxKeyMemoryUsage()
	if used < 4096 {
		t.Fatalf("used estimate %d still looks like 128B/key undercount", used)
	}
}

func TestM2uConfigExtraDirectives(t *testing.T) {
	server := newM2uServer(t)
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "timeout", "60")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "protected-mode", "yes")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "loglevel", "notice")), "OK")
	got := server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "timeout"))
	if val, ok := configReplyValue(got, "timeout"); !ok || val != "60" {
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
