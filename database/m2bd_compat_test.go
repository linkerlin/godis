package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2bdConfigLogLevel(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	old := logger.GetMinLevel()
	oldCfg := config.Properties.LogLevel
	defer func() {
		logger.SetMinLevel(old)
		config.Properties.LogLevel = oldCfg
	}()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "loglevel", "warning")), "OK")
	if logger.GetMinLevel() != logger.WARNING {
		t.Fatalf("min level want WARNING, got %v", logger.GetMinLevel())
	}
	bad := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "loglevel", "bogus"))
	if !protocol.IsErrorReply(bad) || !strings.Contains(string(bad.ToBytes()), "Invalid argument") {
		t.Fatalf("bogus loglevel: %s", bad.ToBytes())
	}
}

func TestM2bdFTExplainAndProfile(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "ex", "ON", "HASH", "PREFIX", "1", "e:", "SCHEMA", "t", "TEXT",
	)), "OK")

	r := db.Exec(nil, utils.ToCmdLine("FT.EXPLAIN", "ex", "hello world", "DIALECT", "2"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), "INTERSECT") || !strings.Contains(string(bulk.Arg), "hello") {
		t.Fatalf("EXPLAIN: %s", r.ToBytes())
	}
	r = db.Exec(nil, utils.ToCmdLine("FT.EXPLAIN", "ex", "a|b"))
	bulk, ok = r.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), "UNION") {
		t.Fatalf("EXPLAIN UNION: %s", r.ToBytes())
	}

	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "ex", "e:1", "FIELDS", "t", "hello"))
	r = db.Exec(nil, utils.ToCmdLine("FT.PROFILE", "ex", "SEARCH", "LIMITED", "hello"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) < 2 {
		t.Fatalf("PROFILE: %T %s", r, r.ToBytes())
	}
	prof := string(mr.Replies[1].ToBytes())
	if !strings.Contains(prof, "Parsing time") || !strings.Contains(prof, "Iterators profile") {
		t.Fatalf("PROFILE keys: %s", prof)
	}
}

func TestM2bdClientListUser(t *testing.T) {
	c := connection.NewFakeConn()
	line := formatClientListLine(c)
	if !strings.Contains(line, "user=default") {
		t.Fatalf("expected user=default, got %q", line)
	}
}

func TestM2bdLuaErrorHandlerCallback(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.set_error_handler(function(err) return 'handled' end)
return redis.call('GET')
`, "0"))
	asserts.AssertBulkReply(t, r, "handled")
}
