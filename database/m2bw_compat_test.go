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

func TestM2bwLolwutVersion(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	r := server.Exec(c, utils.ToCmdLine("LOLWUT"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), "Godis") {
		t.Fatalf("LOLWUT: %T %s", r, r.ToBytes())
	}

	r = server.Exec(c, utils.ToCmdLine("LOLWUT", "VERSION", "6"))
	bulk, ok = r.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), "style 6") {
		t.Fatalf("LOLWUT VERSION 6: %s", r.ToBytes())
	}

	bad := server.Exec(c, utils.ToCmdLine("LOLWUT", "VERSION", "x"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want invalid version error")
	}
}

func TestM2bwBusyReplyThreshold(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{Databases: 16}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "busy-reply-threshold")),
		[]string{"busy-reply-threshold", "5000"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "busy-reply-threshold", "1000")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "busy-reply-threshold")),
		[]string{"busy-reply-threshold", "1000"})
	bad := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "busy-reply-threshold", "-1"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want reject negative")
	}
}

func TestM2bwInfoServerShallowFields(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("INFO", "server"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO server: %T", r)
	}
	s := string(bulk.Arg)
	for _, want := range []string{"configured_hz:", "executable:", "multiplexing_api:"} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %s in %s", want, s)
		}
	}
}

func TestM2bwMemoryDoctor(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("MEMORY", "DOCTOR"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), "Hi Sam") {
		t.Fatalf("MEMORY DOCTOR: %s", r.ToBytes())
	}
}

func TestM2bwACLGetUserChannelsSelectors(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "SETUSER", "pubu", "on", "nopass", "+@all", "~*", "&news:*",
	)), "OK")
	r := server.Exec(c, utils.ToCmdLine("ACL", "GETUSER", "pubu"))
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("GETUSER: %T %s", r, r.ToBytes())
	}
	joined := string(bytesJoin(mb.Args))
	if !strings.Contains(joined, "channels") || !strings.Contains(joined, "&news:*") {
		t.Fatalf("want channels &news:*: %s", joined)
	}
	if !strings.Contains(joined, "selectors") {
		t.Fatalf("want selectors key: %s", joined)
	}

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "SETUSER", "selu", "on", "nopass", "clearselectors", "+@all", "~*",
	)), "OK")
	r = server.Exec(c, utils.ToCmdLine("ACL", "GETUSER", "selu"))
	joined = string(bytesJoin(r.(*protocol.MultiBulkReply).Args))
	if !strings.Contains(joined, "selectors") {
		t.Fatalf("want selectors after clearselectors: %s", joined)
	}
}
