package database

import (
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestDebugSetActiveExpire verifies DEBUG SET-ACTIVE-EXPIRE 0 disables active
// expiry (an expired key survives until lazily accessed) and 1 re-enables it.
func TestDebugSetActiveExpire(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	// Ensure enabled, then verify an expired key IS actively removed.
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("DEBUG", "SET-ACTIVE-EXPIRE", "1")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "ae", "v")), "OK")
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("PEXPIRE", "ae", "1500")), 1)
	waitUntilServerExpired(t, server, "ae", 3*time.Second)

	// Disable active expiry: an expired key must linger (lazy deletion only).
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("DEBUG", "SET-ACTIVE-EXPIRE", "0")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "ae2", "v")), "OK")
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("PEXPIRE", "ae2", "500")), 1)
	time.Sleep(1200 * time.Millisecond)
	// Active expiry is off: the key still exists in the data dict (checking via
	// the internal dict avoids the lazy-delete path of GET/EXISTS).
	holder := server.dbSet[0]
	db0 := holder.Load().(*DB)
	if _, exists := db0.data.Get("ae2"); !exists {
		t.Fatalf("with active expiry off, key should linger in the dict")
	}
	// A lazy access (GET) still reclaims it.
	if r := server.Exec(c, utils.ToCmdLine("GET", "ae2")); !isNullReply(r) {
		t.Fatalf("lazy GET should see the key expired, got %s", r.ToBytes())
	}
	// Re-enable.
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("DEBUG", "SET-ACTIVE-EXPIRE", "1")), "OK")
	// Invalid argument rejected.
	if r := server.Exec(c, utils.ToCmdLine("DEBUG", "SET-ACTIVE-EXPIRE", "2")); !protocol.IsErrorReply(r) {
		t.Fatalf("DEBUG SET-ACTIVE-EXPIRE 2 should error: %s", r.ToBytes())
	}
}

// waitUntilServerExpired polls EXISTS until the key is gone (active expiry).
func waitUntilServerExpired(t *testing.T, server *Server, key string, timeout time.Duration) {
	t.Helper()
	c := connection.NewFakeConn()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r := server.Exec(c, utils.ToCmdLine("EXISTS", key)); isIntReply(r, 0) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("key %q not expired within %v", key, timeout)
}

// TestDebugDigest verifies DEBUG DIGEST computes a real SHA1 over the dataset
// (changes when data changes) and DIGEST-VALUE hashes individual values.
func TestDebugDigest(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	if r := server.Exec(c, utils.ToCmdLine("FLUSHALL")); protocol.IsErrorReply(r) {
		t.Fatalf("flushall: %s", r.ToBytes())
	}
	r := server.Exec(c, utils.ToCmdLine("DEBUG", "DIGEST"))
	d1, ok := r.(*protocol.StatusReply)
	if !ok || len(d1.Status) != 40 {
		t.Fatalf("DIGEST shape: %T %s", r, r.ToBytes())
	}
	// Adding a key must change the digest.
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "dg", "value")), "OK")
	r = server.Exec(c, utils.ToCmdLine("DEBUG", "DIGEST"))
	d2, _ := r.(*protocol.StatusReply)
	if d2.Status == d1.Status {
		t.Fatalf("DIGEST should change after data change")
	}

	// DIGEST-VALUE of an existing key is a 40-char hex digest; missing -> null.
	r = server.Exec(c, utils.ToCmdLine("DEBUG", "DIGEST-VALUE", "dg", "missing"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("DIGEST-VALUE shape: %T %s", r, r.ToBytes())
	}
	if len(mr.Replies) != 2 {
		t.Fatalf("DIGEST-VALUE want 2 replies, got %d", len(mr.Replies))
	}
	if s, ok := mr.Replies[0].(*protocol.StatusReply); !ok || len(s.Status) != 40 {
		t.Fatalf("existing key digest should be 40 hex chars: %s", mr.Replies[0].ToBytes())
	}
	if _, ok := mr.Replies[1].(*protocol.NullBulkReply); !ok {
		t.Fatalf("missing key should be null: %s", mr.Replies[1].ToBytes())
	}
	_ = strings.ToLower
}

// TestDebugChangeReplId verifies DEBUG CHANGE-REPL-ID rotates the master
// replication ID (forcing replicas to full-resync on their next PSYNC).
func TestDebugChangeReplId(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	oldID := server.masterStatus.replId
	if oldID == "" {
		t.Fatalf("master should have a repl id")
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("DEBUG", "CHANGE-REPL-ID")), "OK")
	if server.masterStatus.replId == oldID || len(server.masterStatus.replId) != 40 {
		t.Fatalf("CHANGE-REPL-ID should rotate the repl id: %q -> %q", oldID, server.masterStatus.replId)
	}
}
