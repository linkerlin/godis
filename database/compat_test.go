package database

import (
	"testing"
	"time"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Core Redis-compatible behaviors exercised without an external Redis sidecar.
func TestCompatBasicStringAndTTL(t *testing.T) {
	db := makeTestDB()

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "compat:k", "v")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "compat:k")), "v")

	ret := db.Exec(nil, utils.ToCmdLine("EXPIRE", "compat:k", "1"))
	asserts.AssertIntReply(t, ret, 1)

	ttl := db.Exec(nil, utils.ToCmdLine("TTL", "compat:k"))
	intReply, ok := ttl.(*protocol.IntReply)
	if !ok || intReply.Code <= 0 {
		t.Fatalf("expected positive TTL, got %v", ttl)
	}
}

func TestCompatHashAndList(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "compat:h", "f", "1")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HGET", "compat:h", "f")), "1")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LPUSH", "compat:l", "a", "b")), 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("LPOP", "compat:l")), "b")
}

func TestCompatTransactionContinueOnError(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, db.Exec(c, utils.ToCmdLine("MULTI")), "OK")
	_ = db.Exec(c, utils.ToCmdLine("SET", "compat:tx2", "x"))
	_ = db.Exec(c, utils.ToCmdLine("LPUSH", "compat:tx2", "bad"))
	ret := db.Exec(c, utils.ToCmdLine("EXEC"))
	raw, ok := ret.(*protocol.MultiRawReply)
	if !ok || len(raw.Replies) != 2 {
		t.Fatalf("expected MultiRawReply[2], got %T %v", ret, ret)
	}
	asserts.AssertStatusReply(t, raw.Replies[0], "OK")
	if !protocol.IsErrorReply(raw.Replies[1]) {
		t.Fatalf("expected WRONGTYPE element, got %s", raw.Replies[1].ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "compat:tx2")), "x")
}

func TestCompatSlowlogThreshold(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slowlog-log-slower-than", "0")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slowlog-max-len", "10")), "OK")
	server.Exec(c, utils.ToCmdLine("SET", "compat:slow", "v"))

	ret := server.Exec(c, utils.ToCmdLine("SLOWLOG", "GET", "1"))
	multi, ok := ret.(*protocol.MultiRawReply)
	if !ok || len(multi.Replies) == 0 {
		t.Fatalf("expected slowlog entry, got %T", ret)
	}
	time.Sleep(10 * time.Millisecond)
}
