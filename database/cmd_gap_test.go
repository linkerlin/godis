package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestZInterCard verifies the new ZINTERCARD command (intersection
// cardinality with optional LIMIT early-stop).
func TestZInterCard(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine("ZADD", "za", "1", "a", "2", "b", "3", "c")); protocol.IsErrorReply(r) {
		t.Fatalf("zadd za: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("ZADD", "zb", "1", "b", "2", "c", "3", "d")); protocol.IsErrorReply(r) {
		t.Fatalf("zadd zb: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("ZADD", "zc", "1", "c")); protocol.IsErrorReply(r) {
		t.Fatalf("zadd zc: %s", r.ToBytes())
	}

	// za ∩ zb = {b, c} = 2.
	if r := db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "za", "zb")); !isIntReply(r, 2) {
		t.Fatalf("ZINTERCARD za zb want 2, got %s", r.ToBytes())
	}
	// za ∩ zb ∩ zc = {c} = 1.
	if r := db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "3", "za", "zb", "zc")); !isIntReply(r, 1) {
		t.Fatalf("ZINTERCARD 3-way want 1, got %s", r.ToBytes())
	}
	// LIMIT 1: early-stop at 1.
	if r := db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "za", "zb", "LIMIT", "1")); !isIntReply(r, 1) {
		t.Fatalf("ZINTERCARD LIMIT 1 want 1, got %s", r.ToBytes())
	}
	// Missing key -> 0.
	if r := db.Exec(nil, utils.ToCmdLine("ZINTERCARD", "2", "za", "missing")); !isIntReply(r, 0) {
		t.Fatalf("ZINTERCARD with missing key want 0, got %s", r.ToBytes())
	}
}

// TestSortRo verifies SORT_RO runs read-only and rejects STORE.
func TestSortRo(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	if r := server.Exec(c, utils.ToCmdLine("RPUSH", "sl", "3", "1", "2")); protocol.IsErrorReply(r) {
		t.Fatalf("rpush: %s", r.ToBytes())
	}

	r := server.Exec(c, utils.ToCmdLine("SORT_RO", "sl", "ASC"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("SORT_RO should work: %s", r.ToBytes())
	}
	// STORE rejected.
	r = server.Exec(c, utils.ToCmdLine("SORT_RO", "sl", "STORE", "dest"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "SORT_RO does not support") {
		t.Fatalf("SORT_RO STORE should be rejected: %s", r.ToBytes())
	}
}

// TestEvalRo verifies EVAL_RO rejects nested write commands but allows reads,
// and EVALSHA_RO resolves a previously cached script.
func TestEvalRo(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "rokey", "v")), "OK")

	// Read inside EVAL_RO: allowed.
	r := db.Exec(nil, utils.ToCmdLine("EVAL_RO", "return redis.call('GET', KEYS[1])", "1", "rokey"))
	if bulk, ok := r.(*protocol.BulkReply); !ok || string(bulk.Arg) != "v" {
		t.Fatalf("EVAL_RO read should return 'v', got %s", r.ToBytes())
	}
	// Write inside EVAL_RO: rejected.
	r = db.Exec(nil, utils.ToCmdLine("EVAL_RO", "return redis.call('SET', KEYS[1], 'x')", "1", "rokey"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "read-only") {
		t.Fatalf("EVAL_RO write should be rejected: %s", r.ToBytes())
	}
	// The write must not have happened.
	if r := db.Exec(nil, utils.ToCmdLine("GET", "rokey")); !isBulkReply(r, "v") {
		t.Fatalf("EVAL_RO rejected write must not apply, got %s", r.ToBytes())
	}

	// EVALSHA_RO after SCRIPT LOAD.
	r = db.Exec(nil, utils.ToCmdLine("SCRIPT", "LOAD", "return redis.call('GET', KEYS[1])"))
	sha, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("SCRIPT LOAD: %s", r.ToBytes())
	}
	r = db.Exec(nil, utils.ToCmdLine("EVALSHA_RO", string(sha.Arg), "1", "rokey"))
	if bulk, ok := r.(*protocol.BulkReply); !ok || string(bulk.Arg) != "v" {
		t.Fatalf("EVALSHA_RO should return 'v', got %s", r.ToBytes())
	}
}

func isBulkReply(r redis.Reply, want string) bool {
	if b, ok := r.(*protocol.BulkReply); ok {
		return string(b.Arg) == want
	}
	return false
}
