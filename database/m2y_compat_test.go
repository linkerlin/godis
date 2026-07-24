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

func TestM2yHGetExFields(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "h", "a", "1", "b", "2")), 2)
	r := db.Exec(nil, utils.ToCmdLine("HGETEX", "h", "EX", "60", "FIELDS", "2", "a", "missing"))
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 2 {
		t.Fatalf("HGETEX FIELDS: %s", r.ToBytes())
	}
	asserts.AssertBulkReply(t, protocol.MakeBulkReply(mr.Args[0]), "1")
	if mr.Args[1] != nil {
		t.Fatalf("expected null for missing field")
	}
	ttl := db.Exec(nil, utils.ToCmdLine("HTTL", "h", "FIELDS", "1", "a"))
	mrTTL, ok := ttl.(*protocol.MultiRawReply)
	if !ok || len(mrTTL.Replies) < 1 {
		t.Fatalf("HTTL: %s", ttl.ToBytes())
	}
	asserts.AssertIntReplyGreaterThan(t, mrTTL.Replies[0], 0)
}

func TestM2yHSetExFieldsFNX(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"HSETEX", "hx", "EX", "30", "FIELDS", "1", "f", "v",
	)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"HSETEX", "hx", "FNX", "FIELDS", "1", "f", "v2",
	)), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("HGET", "hx", "f")), "v")
}

func TestM2yHGetDelFields(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("HSET", "hd", "x", "1", "y", "2"))
	r := db.Exec(nil, utils.ToCmdLine("HGETDEL", "hd", "FIELDS", "2", "x", "z"))
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 2 || string(mr.Args[0]) != "1" || mr.Args[1] != nil {
		t.Fatalf("HGETDEL: %s", r.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HEXISTS", "hd", "x")), 0)
}

func TestM2yACLDryRunKeys(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "SETUSER", "limited", "on", "nopass", "+@all", "~ok:*",
	)), "OK")
	ok := server.Exec(c, utils.ToCmdLine("ACL", "DRYRUN", "limited", "GET", "ok:1"))
	asserts.AssertStatusReply(t, ok, "OK")
	deny := server.Exec(c, utils.ToCmdLine("ACL", "DRYRUN", "limited", "GET", "deny:1"))
	if !protocol.IsErrorReply(deny) || !strings.Contains(string(deny.ToBytes()), "no permissions to access") {
		t.Fatalf("expected key deny, got %s", deny.ToBytes())
	}
}

func TestM2yClientAgeIdle(t *testing.T) {
	c := connection.NewFakeConn()
	RegisterClient(c)
	defer UnregisterClient(c)
	c.SetClientTimesForTest(time.Now().Add(-5*time.Second), time.Now().Add(-2*time.Second))
	line := formatClientListLine(c)
	if !strings.Contains(line, "age=") || strings.Contains(line, "age=0 ") {
		t.Fatalf("expected non-zero age: %s", line)
	}
	if !strings.Contains(line, "idle=") || strings.Contains(line, "idle=0 ") {
		t.Fatalf("expected non-zero idle: %s", line)
	}
}

func TestM2yPFCountPrepareMultiKey(t *testing.T) {
	cmd := cmdTable["pfcount"]
	if cmd == nil || cmd.prepare == nil {
		t.Fatal("pfcount missing")
	}
	write, read := cmd.prepare([][]byte{[]byte("a"), []byte("b")})
	if len(write) != 0 || len(read) != 2 {
		t.Fatalf("expected 2 read keys, got write=%v read=%v", write, read)
	}
}
