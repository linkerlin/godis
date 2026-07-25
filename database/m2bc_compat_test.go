package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2bcJSONClearToggleDebugWrap(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"JSON.SET", "j", "$", `{"a":[1,2],"b":true}`,
	)), "OK")

	r := db.Exec(nil, utils.ToCmdLine("JSON.CLEAR", "j", "$.a"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("CLEAR $: %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], 1)

	r = db.Exec(nil, utils.ToCmdLine("JSON.TOGGLE", "j", "$.b"))
	mr, ok = r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("TOGGLE $: %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], 0)

	r = db.Exec(nil, utils.ToCmdLine("JSON.DEBUG", "MEMORY", "j", "$"))
	mr, ok = r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("DEBUG MEMORY $: %T %s", r, r.ToBytes())
	}
}

func TestM2bcFTAggregateLoadStarAndUnknown(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "agg", "ON", "HASH", "PREFIX", "1", "a:", "SCHEMA", "n", "NUMERIC",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "agg", "a:1", "FIELDS", "n", "1"))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "agg", "*", "LOAD", "*", "VERBATIM", "TIMEOUT", "50",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("LOAD *: %s", r.ToBytes())
	}

	bad := db.Exec(nil, utils.ToCmdLine("FT.AGGREGATE", "agg", "*", "NOTANOPTION"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("expected syntax error, got %s", bad.ToBytes())
	}
}

func TestM2bcInfoRedisModeAndClientListSsub(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("INFO", "server"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), "redis_mode:standalone") {
		t.Fatalf("INFO redis_mode: %s", r.ToBytes())
	}

	line := formatClientListLine(c)
	if !strings.Contains(line, "ssub=") {
		t.Fatalf("formatClientListLine missing ssub: %q", line)
	}
}

func TestM2bcLuaSetErrorHandler(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	r := db.Exec(nil, utils.ToCmdLine(
		"EVAL", `redis.set_error_handler(function(err) end); return 1`, "0",
	))
	asserts.AssertIntReply(t, r, 1)
	r = db.Exec(nil, utils.ToCmdLine(
		"EVAL", `redis.set_error_handler(nil); return 2`, "0",
	))
	asserts.AssertIntReply(t, r, 2)
}
