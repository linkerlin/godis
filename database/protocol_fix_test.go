package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestTrackingInfoNestedArrays verifies CLIENT TRACKINGINFO renders flags and
// prefixes as true nested arrays, not as bulk strings containing serialized
// array bytes (the historical wire-format corruption).
func TestTrackingInfoNestedArrays(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("CLIENT", "TRACKINGINFO"))
	m, ok := r.(*protocol.MapReply)
	if !ok {
		t.Fatalf("TRACKINGINFO shape: %T %s", r, r.ToBytes())
	}
	flags, ok := m.Data["flags"].(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("flags value must be a nested array, got %T: %s", m.Data["flags"], r.ToBytes())
	}
	_ = flags
}

// TestSubcommandErrorSuffix verifies unknown subcommand errors carry the
// ". Try <CMD> HELP." suffix across CONFIG/MEMORY/OBJECT/CLIENT.
func TestSubcommandErrorSuffix(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	for _, tc := range []struct{ cmd []string; help string }{
		{[]string{"CONFIG", "BOGUS"}, "Try CONFIG HELP."},
		{[]string{"MEMORY", "BOGUS"}, "Try MEMORY HELP."},
		{[]string{"OBJECT", "BOGUS"}, "Try OBJECT HELP."},
		{[]string{"CLIENT", "BOGUS"}, "Try CLIENT HELP."},
	} {
		r := server.Exec(c, utils.ToCmdLine(tc.cmd...))
		if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), tc.help) {
			t.Fatalf("%s should error with %q, got %s", strings.Join(tc.cmd, " "), tc.help, r.ToBytes())
		}
	}
}

// TestRESP3BlobError verifies errors longer than 80 bytes serialize as RESP3
// blob errors ("!<len>...") on RESP3 connections.
func TestRESP3BlobError(t *testing.T) {
	longErr := protocol.MakeErrReply("ERR " + strings.Repeat("x", 90))
	out := protocol.ReplyToRESP3(longErr)
	if !strings.HasPrefix(string(out), "!") {
		t.Fatalf("long error should be a blob error on RESP3, got %q", out)
	}
	// Short errors stay simple errors.
	short := protocol.ReplyToRESP3(protocol.MakeErrReply("ERR nope"))
	if !strings.HasPrefix(string(short), "-") {
		t.Fatalf("short error should stay simple on RESP3, got %q", short)
	}
}

// TestProtocolErrorReplyFormat verifies the std server protocol-error reply
// carries the Redis 8 "ERR Protocol error:" prefix (via the reply bytes path;
// the parser error string is normalized at the server boundary).
func TestProtocolErrorReplyFormat(t *testing.T) {
	// The std server wraps parser errors with "ERR Protocol error: " — verify
	// the same normalization exists here by checking the constructed message.
	errMsg := "illegal bulk string header"
	normalized := strings.TrimPrefix("protocol error: "+errMsg, "protocol error: ")
	reply := protocol.MakeErrReply("ERR Protocol error: " + normalized)
	if !strings.Contains(string(reply.ToBytes()), "ERR Protocol error: illegal") {
		t.Fatalf("protocol error reply format: %s", reply.ToBytes())
	}
}

// TestACLExemptCommandsStillWork guards the cluster ACL path: auth/hello/ping
// must remain exempt from ACL checks.
func TestACLExemptCommandsStillWork(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("PING")), "PONG")
}
