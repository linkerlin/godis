package database

import (
	"sync"
	"testing"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// waitUntilExpired polls GET until the key returns null (or deadline passes).
func waitUntilExpired(t *testing.T, db *DB, key string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		r := db.Exec(nil, utils.ToCmdLine("GET", key))
		if _, ok := r.(*protocol.NullBulkReply); ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("key %q not expired within %v", key, timeout)
}

// TestExpireNonPositiveTTLDeletes verifies EXPIRE/PEXPIRE with TTL <= 0 delete
// the key immediately (Redis semantics), instead of scheduling a never-firing
// timewheel job.
func TestExpireNonPositiveTTLDeletes(t *testing.T) {
	db := makeTestDB()
	for _, kv := range []struct{ key, cmd string; ttl string }{
		{"k0", "EXPIRE", "0"},
		{"kneg", "EXPIRE", "-1"},
		{"pk0", "PEXPIRE", "0"},
		{"pkneg", "PEXPIRE", "-1"},
	} {
		asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", kv.key, "v")), "OK")
		r := db.Exec(nil, utils.ToCmdLine(kv.cmd, kv.key, kv.ttl))
		if ir, ok := r.(*protocol.IntReply); !ok || ir.Code != 1 {
			t.Fatalf("%s %s %s should return 1 (deleted), got %s", kv.cmd, kv.key, kv.ttl, r.ToBytes())
		}
		if r := db.Exec(nil, utils.ToCmdLine("GET", kv.key)); !isNullReply(r) {
			t.Fatalf("%s %s %s should delete the key, GET returned %s", kv.cmd, kv.key, kv.ttl, r.ToBytes())
		}
	}
	// Missing key returns 0.
	if r := db.Exec(nil, utils.ToCmdLine("EXPIRE", "missing", "0")); !isIntReply(r, 0) {
		t.Fatalf("EXPIRE missing 0 should return 0, got %s", r.ToBytes())
	}
}

// TestPExpireFractionalSecondActive verifies a sub-second TTL actually expires
// via the time wheel. Previously the floor-truncated schedule fired one tick
// early, found the deadline unreached, removed the job, and the key never
// expired.
func TestPExpireFractionalSecondActive(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "frac", "v")), "OK")
	// 1500ms on a 1s wheel: must expire by ~2s.
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PEXPIRE", "frac", "1500")), 1)
	waitUntilExpired(t, db, "frac", 3*time.Second)
}

// TestExpiredKeyPropagatesToAOF verifies an actively-expired key writes a DEL
// to the AOF so replicas (fed from the AOF backlog) drop it too.
func TestExpiredKeyPropagatesToAOF(t *testing.T) {
	db := makeTestDB()
	var mu sync.Mutex
	var aofLines []CmdLine
	db.addAof = func(line CmdLine) {
		mu.Lock()
		aofLines = append(aofLines, line)
		mu.Unlock()
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "prop", "v")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXPIRE", "prop", "1")), 1)
	waitUntilExpired(t, db, "prop", 3*time.Second)
	mu.Lock()
	defer mu.Unlock()
	foundDel := false
	for _, line := range aofLines {
		if len(line) > 1 && string(line[0]) == "del" && string(line[1]) == "prop" {
			foundDel = true
		}
	}
	if !foundDel {
		t.Fatalf("expired key should propagate a DEL to the AOF, got %v", aofLines)
	}
}

// TestWatchFailedWriteNoAbort verifies a WRONGTYPE write from another client
// does NOT invalidate a WATCH (versions bump only on successful writes).
func TestWatchFailedWriteNoAbort(t *testing.T) {
	server := getTestServer()
	w := connection.NewFakeConn()
	other := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(w, utils.ToCmdLine("FLUSHALL")), "OK")
	asserts.AssertStatusReply(t, server.Exec(w, utils.ToCmdLine("SET", "wk", "stringval")), "OK")

	// Watch the key, then have another client attempt a WRONGTYPE write.
	asserts.AssertStatusReply(t, server.Exec(w, utils.ToCmdLine("WATCH", "wk")), "OK")
	bad := server.Exec(other, utils.ToCmdLine("SADD", "wk", "member"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("SADD on a string should fail: %s", bad.ToBytes())
	}

	// EXEC should NOT abort (the failed write didn't modify wk).
	asserts.AssertStatusReply(t, server.Exec(w, utils.ToCmdLine("MULTI")), "OK")
	asserts.AssertStatusReply(t, server.Exec(w, utils.ToCmdLine("SET", "other", "1")), "QUEUED")
	r := server.Exec(w, utils.ToCmdLine("EXEC"))
	if _, ok := r.(*protocol.NullMultiBulkReply); ok {
		t.Fatalf("EXEC should not abort: the WRONGTYPE write never modified wk")
	}
}

func isNullReply(r redis.Reply) bool {
	if r == nil {
		return false
	}
	switch r.(type) {
	case *protocol.NullBulkReply, *protocol.NullMultiBulkReply:
		return true
	}
	return false
}

func isIntReply(r redis.Reply, want int64) bool {
	if ir, ok := r.(*protocol.IntReply); ok {
		return ir.Code == want
	}
	return false
}
