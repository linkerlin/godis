package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2atACLLiveKeyAndChannel(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	// real Redis 8.10: nopass users reject explicit AUTH — use a password
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "SETUSER", "liveu", "on", ">x", "+@all", "~ok:*", "&news:*",
	)), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("AUTH", "liveu", "x")), "OK")

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "ok:1", "v")), "OK")
	r := server.Exec(c, utils.ToCmdLine("SET", "deny:1", "v"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "No permissions to access a key") {
		t.Fatalf("key deny: %s", r.ToBytes())
	}

	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("PUBLISH", "news:1", "hi")), 0)
	r = server.Exec(c, utils.ToCmdLine("PUBLISH", "other", "hi"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "No permissions to access a channel") {
		t.Fatalf("channel deny: %s", r.ToBytes())
	}

	// liveu holds +@all, so it may remove itself; no default-auth restore
	// needed (a passwordless default user refuses explicit AUTH on 8.10).
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("ACL", "DELUSER", "liveu")), 1)
}

func TestM2atScanInvalidPattern(t *testing.T) {
	db := makeTestDB()
	// Trailing backslash fails wildcard.CompilePattern → nextCursor < 0
	r := db.Exec(nil, utils.ToCmdLine("SCAN", "0", "MATCH", `\`))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("expected error, got %s", r.ToBytes())
	}
	s := string(r.ToBytes())
	if !strings.Contains(s, "ERR") || !strings.Contains(s, "Invalid argument") {
		t.Fatalf("expected ERR Invalid argument, got %s", s)
	}
}

func TestM2atInfoClientsInTimeoutTable(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("INFO", "clients"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO clients: %T %s", r, r.ToBytes())
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "clients_in_timeout_table:") {
		t.Fatalf("missing field: %s", s)
	}
}
