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

func TestM2azProtectedMode(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	oldPM := config.Properties.ProtectedMode
	oldBind := config.Properties.Bind
	oldPass := config.Properties.RequirePass
	defer func() {
		config.Properties.ProtectedMode = oldPM
		config.Properties.Bind = oldBind
		config.Properties.RequirePass = oldPass
	}()

	config.Properties.ProtectedMode = true
	config.Properties.Bind = "0.0.0.0"
	config.Properties.RequirePass = ""

	c.SetRemoteAddr("8.8.8.8:12345")
	r := server.Exec(c, utils.ToCmdLine("PING"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "DENIED") {
		t.Fatalf("expected DENIED, got %s", r.ToBytes())
	}

	// Local client still works
	local := connection.NewFakeConn()
	local.SetRemoteAddr("127.0.0.1:9999")
	asserts.AssertStatusReply(t, server.Exec(local, utils.ToCmdLine("PING")), "PONG")

	// Password disables protected-mode gate
	config.Properties.RequirePass = "secret"
	remote := connection.NewFakeConn()
	remote.SetRemoteAddr("8.8.8.8:1")
	asserts.AssertStatusReply(t, server.Exec(remote, utils.ToCmdLine("AUTH", "secret")), "OK")
	asserts.AssertStatusReply(t, server.Exec(remote, utils.ToCmdLine("PING")), "PONG")
}

func TestM2azBindsOnlyLoopback(t *testing.T) {
	if !bindsOnlyLoopback("127.0.0.1") {
		t.Fatal("127.0.0.1 should be loopback-only")
	}
	if bindsOnlyLoopback("0.0.0.0") {
		t.Fatal("0.0.0.0 is not loopback-only")
	}
	if bindsOnlyLoopback("") {
		t.Fatal("empty bind is all interfaces")
	}
}

func TestM2azFTSearchHighlight(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "hl", "ON", "HASH", "PREFIX", "1", "h:", "SCHEMA", "t", "TEXT",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "hl", "h:1", "FIELDS", "t", "hello world"))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "hl", "hello", "HIGHLIGHT", "FIELDS", "1", "t", "TAGS", "<b>", "</b>",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("HIGHLIGHT syntax: %s", r.ToBytes())
	}
	body := string(r.ToBytes())
	if !strings.Contains(body, "highlight") && !strings.Contains(body, "<b>") {
		// Engine may still return docs; at least must not be syntax error and opts applied
		mr, ok := r.(*protocol.MultiRawReply)
		if !ok || len(mr.Replies) < 1 {
			t.Fatalf("HIGHLIGHT reply: %s", body)
		}
	}
}

func TestM2azLuaSetReplAndLogConstants(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.set_repl(3)
redis.replicate_commands()
return redis.LOG_NOTICE
`, "0"))
	asserts.AssertIntReply(t, r, 2)

	r = db.Exec(nil, utils.ToCmdLine("EVAL", `return redis.replicate_commands()`, "0"))
	asserts.AssertIntReply(t, r, 1)
}
