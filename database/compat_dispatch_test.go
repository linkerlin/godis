package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestCommandAliasDispatch verifies that standard multi-word module / function /
// script commands are accepted when sent as separate RESP arguments.
func TestCommandAliasDispatch(t *testing.T) {
	db := makeTestDB()

	cases := []struct {
		name    string
		cmd     []string
		notErr  string // substring that the reply must NOT contain
	}{
		{"function list", []string{"FUNCTION", "LIST"}, "unknown command"},
		{"script exists", []string{"SCRIPT", "EXISTS", "deadbeef"}, "unknown command"},
		{"tdigest create", []string{"TDIGEST", "CREATE", "compat:td"}, "unknown command"},
		{"tdigest info after create", []string{"TDIGEST", "INFO", "compat:td"}, "unknown command"},
		{"vs add arity", []string{"VS", "ADD"}, "unknown command"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ret := db.Exec(nil, utils.ToCmdLine(tc.cmd...))
			if ret == nil {
				t.Fatalf("expected a reply, got nil")
			}
			if strings.Contains(string(ret.ToBytes()), tc.notErr) {
				t.Fatalf("reply unexpectedly contains %q: %s", tc.notErr, ret.ToBytes())
			}
		})
	}
}

// TestHelloProtocolVersion verifies that HELLO negotiates and stores the RESP
// protocol version on the connection.
func TestHelloProtocolVersion(t *testing.T) {
	server := getTestServer()

	for _, version := range []string{"2", "3"} {
		t.Run("hello_"+version, func(t *testing.T) {
			c := connection.NewFakeConn()
			ret := server.Exec(c, utils.ToCmdLine("HELLO", version))
			if ret == nil {
				t.Fatalf("HELLO %s returned nil", version)
			}
			want := 2
			if version == "3" {
				want = 3
			}
			if got := c.GetProtocolVersion(); got != want {
				t.Fatalf("HELLO %s: expected protocol version %d, got %d", version, want, got)
			}
		})
	}
}

// TestClientSetNameGetNameID verifies that CLIENT SETNAME / GETNAME / ID use
// real connection state instead of hard-coded values.
func TestClientSetNameGetNameID(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CLIENT", "SETNAME", "godis-test")), "OK")
	asserts.AssertBulkReply(t, server.Exec(c, utils.ToCmdLine("CLIENT", "GETNAME")), "godis-test")

	idReply := server.Exec(c, utils.ToCmdLine("CLIENT", "ID"))
	intReply, ok := idReply.(*protocol.IntReply)
	if !ok || intReply.Code <= 0 {
		t.Fatalf("expected positive CLIENT ID, got %v", idReply)
	}
}

// TestHGetArity verifies that HGET rejects more than one field argument.
func TestHGetArity(t *testing.T) {
	db := makeTestDB()
	ret := db.Exec(nil, utils.ToCmdLine("HGET", "compat:h", "f1", "f2"))
	asserts.AssertErrReply(t, ret, "ERR wrong number of arguments for 'hget' command")
}

// TestRenameCommandFlag verifies that RENAME is advertised as a write command.
func TestRenameCommandFlag(t *testing.T) {
	server := getTestServer()
	ret := server.Exec(nil, utils.ToCmdLine("COMMAND", "INFO", "RENAME"))
	multi, ok := ret.(*protocol.MultiRawReply)
	if !ok || len(multi.Replies) == 0 {
		t.Fatalf("expected COMMAND INFO reply, got %T", ret)
	}
	raw, ok := multi.Replies[0].(*protocol.MultiRawReply)
	if !ok || len(raw.Replies) == 0 {
		t.Fatalf("expected raw command desc wrapper, got %T", multi.Replies[0])
	}
	flags, ok := raw.Replies[2].(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("expected flags array, got %T", raw.Replies[2])
	}
	found := false
	for _, f := range flags.Args {
		if string(f) == "write" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected RENAME to have 'write' flag, got %v", flags.Args)
	}
}

// TestCommandGetKeysModule verifies that COMMAND GETKEYS works for module
// commands after alias resolution.
func TestCommandGetKeysModule(t *testing.T) {
	server := getTestServer()
	ret := server.Exec(nil, utils.ToCmdLine("COMMAND", "GETKEYS", "JSON.SET", "compat:j", ".", "1"))
	multi, ok := ret.(*protocol.MultiBulkReply)
	if !ok || len(multi.Args) != 1 || string(multi.Args[0]) != "compat:j" {
		t.Fatalf("expected [compat:j], got %v", ret)
	}
}
