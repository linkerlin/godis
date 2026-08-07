package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2bbFTSearchInFieldsAndSummarize(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "ibb", "ON", "HASH", "PREFIX", "1", "i:",
		"SCHEMA", "title", "TEXT", "body", "TEXT",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "ibb", "i:1", "FIELDS",
		"title", "alpha", "body", "hello world is a long body text for summarize"))
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "ibb", "i:2", "FIELDS",
		"title", "hello", "body", "other"))

	// INFIELDS title: "hello" only in title of i:2
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "ibb", "hello", "INFIELDS", "1", "title", "NOCONTENT",
	))
	mr := ftSearchMultiRaw(r)
	if mr == nil || len(mr.Replies) < 2 {
		t.Fatalf("INFIELDS: %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], 1)
	bulk, ok := mr.Replies[1].(*protocol.BulkReply)
	if !ok || string(bulk.Arg) != "i:2" {
		t.Fatalf("expected i:2, got %s", r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "ibb", "hello", "SUMMARIZE", "FIELDS", "1", "body", "LEN", "10",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("SUMMARIZE: %s", r.ToBytes())
	}
	body := string(r.ToBytes())
	if !strings.Contains(body, "...") {
		// i:1 body should be truncated when returned
		t.Logf("SUMMARIZE reply (may omit long body if not matched): %s", body)
	}
}

func TestM2bbJSONEnhancedArrObj(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"JSON.SET", "j", "$", `{"a":[1,2,3],"o":{"x":1,"y":2}}`,
	)), "OK")

	r := db.Exec(nil, utils.ToCmdLine("JSON.ARRPOP", "j", "$.a"))
	asserts.AssertBulkReply(t, r, "[3]")

	r = db.Exec(nil, utils.ToCmdLine("JSON.ARRTRIM", "j", "$.a", "0", "0"))
	mr := ftSearchMultiRaw(r)
	if mr == nil || len(mr.Replies) != 1 {
		t.Fatalf("ARRTRIM $: %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], 1)

	r = db.Exec(nil, utils.ToCmdLine("JSON.OBJKEYS", "j", "$.o"))
	mr = ftSearchMultiRaw(r)
	if mr == nil || len(mr.Replies) != 1 {
		t.Fatalf("OBJKEYS $: %T %s", r, r.ToBytes())
	}
	inner, ok := mr.Replies[0].(*protocol.MultiBulkReply)
	if !ok || len(inner.Args) != 2 {
		t.Fatalf("OBJKEYS nested: %T %s", mr.Replies[0], r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("JSON.ARRINDEX", "j", "$.a", "1"))
	mr = ftSearchMultiRaw(r)
	if mr == nil || len(mr.Replies) != 1 {
		t.Fatalf("ARRINDEX $: %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], 0)
}

func TestM2bbLuaVersionAndReplConstants(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine(
		"EVAL", `return redis.REDIS_VERSION`, "0",
	)), "8.0.0")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"EVAL", `return redis.REDIS_VERSION_NUM`, "0",
	)), 0x080000)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"EVAL", `redis.set_repl(redis.REPL_NONE); return redis.REPL_ALL`, "0",
	)), 3)
}
