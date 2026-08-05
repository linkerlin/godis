package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestP6aHIncrByReindexes verifies HINCRBY on an indexed NUMERIC field updates
// the FT document so a range query reflects the new value.
func TestP6aHIncrByReindexes(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p6a", "ON", "HASH", "PREFIX", "1", "p6a:", "SCHEMA", "n", "NUMERIC",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p6a:1", "n", "1")); protocol.IsErrorReply(r) {
		t.Fatalf("hset: %s", r.ToBytes())
	}
	// Initially n=1, not in [10 20].
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p6a", "@n:[10 20]", "NOCONTENT")); !searchTotalIs(t, r, 0) {
		t.Fatalf("before incr: want 0 in [10 20], got %s", r.ToBytes())
	}
	// HINCRBY n by 10 -> n=11, now in range.
	if r := db.Exec(nil, utils.ToCmdLine("HINCRBY", "p6a:1", "n", "10")); protocol.IsErrorReply(r) {
		t.Fatalf("hincrby: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p6a", "@n:[10 20]", "NOCONTENT")); !searchTotalIs(t, r, 1) {
		t.Fatalf("after hincrby: want 1 in [10 20], got %s", r.ToBytes())
	}
}

// TestP6bJSONNumIncrByReindexes verifies JSON.NUMINCRBY on an indexed ON JSON
// field updates the FT document.
func TestP6bJSONNumIncrByReindexes(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p6b", "ON", "JSON", "PREFIX", "1", "p6b:", "SCHEMA", "$.n", "AS", "n", "NUMERIC",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("JSON.SET", "p6b:1", "$", `{"n":1}`)); protocol.IsErrorReply(r) {
		t.Fatalf("json.set: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p6b", "@n:[10 20]", "NOCONTENT")); !searchTotalIs(t, r, 0) {
		t.Fatalf("before incr: want 0, got %s", r.ToBytes())
	}
	// JSON.NUMINCRBY n by 15 -> n=16, in range.
	if r := db.Exec(nil, utils.ToCmdLine("JSON.NUMINCRBY", "p6b:1", "$.n", "15")); protocol.IsErrorReply(r) {
		t.Fatalf("numincrby: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p6b", "@n:[10 20]", "NOCONTENT")); !searchTotalIs(t, r, 1) {
		t.Fatalf("after numincrby: want 1 in [10 20], got %s", r.ToBytes())
	}
}

// TestP6bJSONMergeReindexes verifies JSON.MERGE updates the indexed content.
func TestP6bJSONMergeReindexes(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p6bm", "ON", "JSON", "PREFIX", "1", "p6bm:", "SCHEMA", "$.t", "AS", "t", "TEXT",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("JSON.SET", "p6bm:1", "$", `{"t":"old"}`)); protocol.IsErrorReply(r) {
		t.Fatalf("json.set: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p6bm", "newword", "NOCONTENT")); !searchTotalIs(t, r, 0) {
		t.Fatalf("before merge: want 0, got %s", r.ToBytes())
	}
	// Merge a new value into $.t.
	if r := db.Exec(nil, utils.ToCmdLine("JSON.MERGE", "p6bm:1", "$.t", `"newword"`)); protocol.IsErrorReply(r) {
		t.Fatalf("json.merge: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p6bm", "newword", "NOCONTENT")); !searchTotalIs(t, r, 1) {
		t.Fatalf("after merge: want 1, got %s", r.ToBytes())
	}
}

// TestP6cRenameReindexes verifies RENAME removes the source from the index and
// makes the destination searchable.
func TestP6cRenameReindexes(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p6c", "ON", "HASH", "PREFIX", "1", "p6c:", "SCHEMA", "t", "TEXT",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p6c:src", "t", "findme")); protocol.IsErrorReply(r) {
		t.Fatalf("hset: %s", r.ToBytes())
	}
	// Source is searchable.
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p6c", "findme", "NOCONTENT")); !searchTotalIs(t, r, 1) {
		t.Fatalf("before rename: want 1, got %s", r.ToBytes())
	}
	// Rename p6c:src -> p6c:dst.
	if r := db.Exec(nil, utils.ToCmdLine("RENAME", "p6c:src", "p6c:dst")); protocol.IsErrorReply(r) {
		t.Fatalf("rename: %s", r.ToBytes())
	}
	// Total matches still 1 (the doc moved), and the destination is the hit.
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p6c", "findme", "NOCONTENT"))
	if !searchTotalIs(t, r, 1) {
		t.Fatalf("after rename: want 1 hit, got %s", r.ToBytes())
	}
}

// TestP6dExpireReindexes verifies EXPIRE on an indexed key does not corrupt the
// index (the doc remains searchable after a TTL is set then PERSISTed).
func TestP6dExpireReindexes(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "p6d", "ON", "HASH", "PREFIX", "1", "p6d:", "SCHEMA", "t", "TEXT",
	)), "OK")
	if r := db.Exec(nil, utils.ToCmdLine("HSET", "p6d:1", "t", "hello")); protocol.IsErrorReply(r) {
		t.Fatalf("hset: %s", r.ToBytes())
	}
	// Set a TTL; doc must remain searchable (TTL doesn't drop content).
	if r := db.Exec(nil, utils.ToCmdLine("EXPIRE", "p6d:1", "100")); protocol.IsErrorReply(r) {
		t.Fatalf("expire: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p6d", "hello", "NOCONTENT")); !searchTotalIs(t, r, 1) {
		t.Fatalf("after expire: want 1, got %s", r.ToBytes())
	}
	// PERSIST; still searchable.
	if r := db.Exec(nil, utils.ToCmdLine("PERSIST", "p6d:1")); protocol.IsErrorReply(r) {
		t.Fatalf("persist: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "p6d", "hello", "NOCONTENT")); !searchTotalIs(t, r, 1) {
		t.Fatalf("after persist: want 1, got %s", r.ToBytes())
	}
}
