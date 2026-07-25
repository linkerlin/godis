package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2asACLSelectorDryRun(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "SETUSER", "selu", "on", "nopass", "+get", "~key1", "(+set ~key2)",
	)), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "DRYRUN", "selu", "GET", "key1",
	)), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "DRYRUN", "selu", "SET", "key2", "v",
	)), "OK")
	r := server.Exec(c, utils.ToCmdLine("ACL", "DRYRUN", "selu", "GET", "key2"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "no permissions to access") {
		t.Fatalf("GET key2: %s", r.ToBytes())
	}
	r = server.Exec(c, utils.ToCmdLine("ACL", "DRYRUN", "selu", "SET", "key1", "v"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("SET key1: %s", r.ToBytes())
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "SETUSER", "selu", "clearselectors",
	)), "OK")
	r = server.Exec(c, utils.ToCmdLine("ACL", "DRYRUN", "selu", "SET", "key2", "v"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("after clearselectors SET should deny: %s", r.ToBytes())
	}
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("ACL", "DELUSER", "selu")), 1)
}

func TestM2asACLChannelDryRun(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "SETUSER", "pubu", "on", "nopass", "+publish", "&news:*",
	)), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "DRYRUN", "pubu", "PUBLISH", "news:1", "hi",
	)), "OK")
	r := server.Exec(c, utils.ToCmdLine("ACL", "DRYRUN", "pubu", "PUBLISH", "other", "hi"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "channel") {
		t.Fatalf("expected channel deny: %s", r.ToBytes())
	}
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("ACL", "DELUSER", "pubu")), 1)
}
