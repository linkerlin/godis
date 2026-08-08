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

func TestM2vShardedPubSub(t *testing.T) {
	server := getTestServer()
	sub := connection.NewFakeConn()
	pub := connection.NewFakeConn()

	_ = server.Exec(sub, utils.ToCmdLine("SSUBSCRIBE", "sch1"))
	if sub.SubsCount() < 1 {
		t.Fatal("expected subscribed state after SSUBSCRIBE")
	}
	out := string(sub.Bytes())
	if !strings.Contains(out, "ssubscribe") || !strings.Contains(out, "sch1") {
		t.Fatalf("missing ssubscribe confirm: %q", out)
	}

	n := server.Exec(pub, utils.ToCmdLine("SPUBLISH", "sch1", "hello"))
	asserts.AssertIntReply(t, n, 1)
	if !strings.Contains(string(sub.Bytes()), "hello") {
		t.Fatalf("subscriber did not receive message: %q", sub.Bytes())
	}

	ch := server.Exec(pub, utils.ToCmdLine("SCHANNELS"))
	multi, ok := ch.(*protocol.MultiBulkReply)
	if !ok || len(multi.Args) < 1 {
		t.Fatalf("SCHANNELS: %s", ch.ToBytes())
	}
	found := false
	for _, a := range multi.Args {
		if string(a) == "sch1" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("SCHANNELS missing sch1: %s", ch.ToBytes())
	}

	bad := server.Exec(sub, utils.ToCmdLine("SET", "k", "v"))
	if !protocol.IsErrorReply(bad) || !strings.Contains(string(bad.ToBytes()), "SUBSCRIBE") {
		t.Fatalf("expected subscribe-context error, got %s", bad.ToBytes())
	}
}

func TestM2vEvalPrepareInMulti(t *testing.T) {
	testDB.Flush()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, testDB.Exec(c, utils.ToCmdLine("MULTI")), "OK")
	queued := testDB.Exec(c, utils.ToCmdLine("EVAL", "return redis.call('SET', KEYS[1], ARGV[1])", "1", "ek", "ev"))
	asserts.AssertStatusReply(t, queued, "QUEUED")
	exec := testDB.Exec(c, utils.ToCmdLine("EXEC"))
	if protocol.IsErrorReply(exec) {
		t.Fatalf("EXEC EVAL: %s", exec.ToBytes())
	}
	got := testDB.Exec(nil, utils.ToCmdLine("GET", "ek"))
	asserts.AssertBulkReply(t, got, "ev")
}

func TestM2vVRandMember(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "vk", "VALUES", "2", "1", "0", "ELE", "a",
	)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "vk", "VALUES", "2", "0", "1", "ELE", "b",
	)), 1)
	one := db.Exec(nil, utils.ToCmdLine("VRANDMEMBER", "vk"))
	bulk, ok := one.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("expected bulk, got %T %s", one, one.ToBytes())
	}
	id := string(bulk.Arg)
	if id != "a" && id != "b" {
		t.Fatalf("unexpected member %q", id)
	}
	many := db.Exec(nil, utils.ToCmdLine("VRANDMEMBER", "vk", "2"))
	mr, ok := many.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 2 {
		t.Fatalf("VRANDMEMBER count: %s", many.ToBytes())
	}
}

func TestM2vSaveAndTCPBacklogConfig(t *testing.T) {
	config.Properties = &config.ServerProperties{Databases: 1}
	server, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "save", "3600 1")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tcp-backlog", "511")), "OK")
	got := server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "save"))
	if val, ok := configReplyValue(got, "save"); !ok || val != "3600 1" {
		t.Fatalf("CONFIG GET save: %s", got.ToBytes())
	}
	got2 := server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "tcp-backlog"))
	if val, ok := configReplyValue(got2, "tcp-backlog"); !ok || val != "511" {
		t.Fatalf("CONFIG GET tcp-backlog: %s", got2.ToBytes())
	}
}
