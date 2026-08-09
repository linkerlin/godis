package database

import (
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func intArray(reply redis.Reply) []int64 {
	multi, ok := reply.(*protocol.MultiRawReply)
	if !ok {
		return nil
	}
	out := make([]int64, len(multi.Replies))
	for i, r := range multi.Replies {
		ir, ok := r.(*protocol.IntReply)
		if !ok {
			return nil
		}
		out[i] = ir.Code
	}
	return out
}

func TestHExpireBasic(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "hk", "f1", "v1", "f2", "v2")), 2)

	reply := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "hk", "1", "FIELDS", "1", "f1"))
	got := intArray(reply)
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected [1], got %v", got)
	}

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HGET", "hk", "f1")), "v1")

	time.Sleep(1100 * time.Millisecond)
	asserts.AssertNullBulk(t, db.Exec(nil, utils.ToCmdLine("HGET", "hk", "f1")))
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HGET", "hk", "f2")), "v2")
}

func TestHExpireMissingAndConditions(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "hk", "f1", "v1")), 1)

	reply := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "hk", "10", "FIELDS", "2", "f1", "missing"))
	got := intArray(reply)
	if len(got) != 2 || got[0] != 1 || got[1] != -2 {
		t.Fatalf("expected [1 -2], got %v", got)
	}

	reply = db.Exec(nil, utils.ToCmdLine("HEXPIRE", "hk", "10", "NX", "FIELDS", "1", "f1"))
	got = intArray(reply)
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("expected NX on existing field to fail, got %v", got)
	}

	reply = db.Exec(nil, utils.ToCmdLine("HEXPIRE", "hk", "10", "XX", "FIELDS", "1", "missing"))
	got = intArray(reply)
	if len(got) != 1 || got[0] != -2 {
		t.Fatalf("expected XX on missing field to be -2, got %v", got)
	}

	reply = db.Exec(nil, utils.ToCmdLine("HEXPIRE", "hk", "10", "XX", "FIELDS", "1", "f1"))
	got = intArray(reply)
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected XX on existing field to succeed, got %v", got)
	}

	reply = db.Exec(nil, utils.ToCmdLine("HEXPIRE", "nokey", "10", "FIELDS", "1", "f1"))
	got = intArray(reply)
	if len(got) != 1 || got[0] != -2 {
		t.Fatalf("expected -2 for non-existent key, got %v", got)
	}
}

func TestHExpireConditionsGTandLT(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "hk", "f1", "v1")), 1)
	reply := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "hk", "100", "FIELDS", "1", "f1"))
	if got := intArray(reply); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected [1], got %v", got)
	}

	reply = db.Exec(nil, utils.ToCmdLine("HEXPIRE", "hk", "10", "GT", "FIELDS", "1", "f1"))
	if got := intArray(reply); len(got) != 1 || got[0] != 0 {
		t.Fatalf("expected GT fail, got %v", got)
	}

	reply = db.Exec(nil, utils.ToCmdLine("HEXPIRE", "hk", "200", "GT", "FIELDS", "1", "f1"))
	if got := intArray(reply); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected GT succeed, got %v", got)
	}

	reply = db.Exec(nil, utils.ToCmdLine("HEXPIRE", "hk", "300", "LT", "FIELDS", "1", "f1"))
	if got := intArray(reply); len(got) != 1 || got[0] != 0 {
		t.Fatalf("expected LT fail, got %v", got)
	}

	reply = db.Exec(nil, utils.ToCmdLine("HEXPIRE", "hk", "150", "LT", "FIELDS", "1", "f1"))
	if got := intArray(reply); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected LT succeed, got %v", got)
	}
}

func TestHExpireAtAndPExpire(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "hk", "f1", "v1")), 1)

	future := time.Now().Add(2 * time.Second).Unix()
	reply := db.Exec(nil, utils.ToCmdLine("HEXPIREAT", "hk", strconv.FormatInt(future, 10), "FIELDS", "1", "f1"))
	if got := intArray(reply); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected [1], got %v", got)
	}

	reply = db.Exec(nil, utils.ToCmdLine("HPTTL", "hk", "f1"))
	pttl, ok := reply.(*protocol.IntReply)
	if !ok || pttl.Code <= 0 {
		t.Fatalf("expected positive PTTL, got %v", reply)
	}

	reply = db.Exec(nil, utils.ToCmdLine("HPExpire", "hk", "1", "FIELDS", "1", "f1"))
	if got := intArray(reply); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected [1], got %v", got)
	}
	time.Sleep(50 * time.Millisecond)
	asserts.AssertNullBulk(t, db.Exec(nil, utils.ToCmdLine("HGET", "hk", "f1")))
}

func TestHExpireAfterClassicHash(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "hk", "f1", "v1")), 1)
	reply := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "hk", "3600", "FIELDS", "1", "f1"))
	if got := intArray(reply); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected [1], got %v", got)
	}

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HGET", "hk", "f1")), "v1")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "hk")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HDEL", "hk", "f1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HLEN", "hk")), 0)
}

func TestHExpireMutuallyExclusiveFlags(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "hk", "f1", "v1")), 1)
	reply := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "hk", "10", "NX", "XX", "FIELDS", "1", "f1"))
	if reply == nil {
		t.Fatal("expected error reply")
	}
	if !strings.Contains(string(reply.ToBytes()), "mutually exclusive") {
		t.Fatalf("expected mutually exclusive error, got %s", reply.ToBytes())
	}
}

func TestHExpirePastTimeDeletesField(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "hk", "f1", "v1")), 1)

	past := time.Now().Add(-10 * time.Second).Unix()
	reply := db.Exec(nil, utils.ToCmdLine("HEXPIREAT", "hk", strconv.FormatInt(past, 10), "FIELDS", "1", "f1"))
	if got := intArray(reply); len(got) != 1 || got[0] != 2 {
		t.Fatalf("expected [2], got %v", got)
	}
	asserts.AssertNullBulk(t, db.Exec(nil, utils.ToCmdLine("HGET", "hk", "f1")))
}

func TestHExpireInTransaction(t *testing.T) {
	db := makeTestDB()
	conn := connection.NewFakeConn()
	asserts.AssertIntReply(t, db.Exec(conn, utils.ToCmdLine("HSET", "hk", "f1", "v1")), 1)

	asserts.AssertStatusReply(t, db.Exec(conn, utils.ToCmdLine("MULTI")), "OK")
	db.Exec(conn, utils.ToCmdLine("HEXPIRE", "hk", "3600", "FIELDS", "1", "f1"))
	reply := db.Exec(conn, utils.ToCmdLine("EXEC"))
	multi, ok := reply.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("expected MultiRawReply, got %T", reply)
	}
	if len(multi.Replies) != 1 {
		t.Fatalf("expected 1 reply, got %d", len(multi.Replies))
	}
	got := intArray(multi.Replies[0])
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected [[1]], got %v", got)
	}
}

// TestHashFieldLazyExpirePropagatesHDel verifies that lazily expiring a hash field
// writes HDEL to the AOF (so replicas drop the field too).
func TestHashFieldLazyExpirePropagatesHDel(t *testing.T) {
	db := makeTestDB()
	var mu sync.Mutex
	var aofLines []CmdLine
	db.addAof = func(line CmdLine) {
		mu.Lock()
		aofLines = append(aofLines, line)
		mu.Unlock()
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "hk", "f1", "v1", "f2", "v2")), 2)
	reply := db.Exec(nil, utils.ToCmdLine("HPEXPIRE", "hk", "50", "FIELDS", "1", "f1"))
	if got := intArray(reply); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected [1], got %v", got)
	}

	time.Sleep(80 * time.Millisecond)
	asserts.AssertNullBulk(t, db.Exec(nil, utils.ToCmdLine("HGET", "hk", "f1")))
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HGET", "hk", "f2")), "v2")

	mu.Lock()
	defer mu.Unlock()
	if !aofContainsHDel(aofLines, "hk", "f1") {
		t.Fatalf("lazy field expire should propagate HDEL, got %v", aofLines)
	}
}

// TestHashFieldActiveExpirePropagatesHDel waits for the time-wheel callback
// (no HGET) and expects an AOF HDEL.
func TestHashFieldActiveExpirePropagatesHDel(t *testing.T) {
	db := makeTestDB()
	var mu sync.Mutex
	var aofLines []CmdLine
	db.addAof = func(line CmdLine) {
		mu.Lock()
		aofLines = append(aofLines, line)
		mu.Unlock()
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "hk", "f1", "v1")), 1)
	reply := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "hk", "1", "FIELDS", "1", "f1"))
	if got := intArray(reply); len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected [1], got %v", got)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		ok := aofContainsHDel(aofLines, "hk", "f1")
		mu.Unlock()
		if ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Fatalf("active field expire should propagate HDEL within 3s, got %v", aofLines)
}

func aofContainsHDel(lines []CmdLine, key, field string) bool {
	for _, line := range lines {
		if len(line) >= 3 && string(line[0]) == "hdel" && string(line[1]) == key && string(line[2]) == field {
			return true
		}
	}
	return false
}
