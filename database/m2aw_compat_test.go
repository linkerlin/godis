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

func TestM2awJSONGetEnhancedWrap(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"JSON.SET", "j", "$", `{"a":1}`,
	)), "OK")
	// No path: unwrapped document
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("JSON.GET", "j")), `{"a":1}`)
	// Explicit $ path: array wrap
	got := db.Exec(nil, utils.ToCmdLine("JSON.GET", "j", "$.a"))
	asserts.AssertBulkReply(t, got, "[1]")
	// Legacy path: unwrapped
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("JSON.GET", "j", ".a")), "1")

	typ := db.Exec(nil, utils.ToCmdLine("JSON.TYPE", "j", "$.a"))
	mr, ok := typ.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 1 || string(mr.Args[0]) != "number" {
		t.Fatalf("JSON.TYPE $: %T %s", typ, typ.ToBytes())
	}
}

func TestM2awSelectWithinMulti(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("MULTI")), "OK")
	r := server.Exec(c, utils.ToCmdLine("SELECT", "1"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "ERR") {
		t.Fatalf("expected ERR, got %s", r.ToBytes())
	}
	_ = server.Exec(c, utils.ToCmdLine("DISCARD"))
}

func TestM2awClientStubNeedsConn(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("CLIENT", "SETNAME", "x"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "requires a client connection") {
		t.Fatalf("SETNAME stub: %s", r.ToBytes())
	}
	r = db.Exec(nil, utils.ToCmdLine("CLIENT", "ID"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "requires a client connection") {
		t.Fatalf("ID stub: %s", r.ToBytes())
	}
}

func TestM2awInfoHzConfig(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	old := config.Properties.Hz
	defer func() { config.Properties.Hz = old }()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "hz", "25")), "OK")
	r := server.Exec(c, utils.ToCmdLine("INFO", "server"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), "hz:25") {
		t.Fatalf("INFO hz: %s", r.ToBytes())
	}
}
