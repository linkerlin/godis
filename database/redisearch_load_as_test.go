package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// TestFTAggregateLoadAsAndKey aligns LOAD … AS and LOAD @__key with Redis 8.x
// (nargs counts AS+alias; FILTER can use the alias; ADDSCORES keeps __score).
func TestFTAggregateLoadAsAndKey(t *testing.T) {
	db := makeTestDB()
	create := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "loadas", "ON", "HASH", "PREFIX", "1", "la:",
		"SCHEMA", "title", "TEXT", "price", "NUMERIC",
	))
	if protocol.IsErrorReply(create) {
		t.Fatalf("FT.CREATE: %s", create.ToBytes())
	}
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "la:1", "title", "hello", "price", "10"))
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "la:2", "title", "world", "price", "20"))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "loadas", "*",
		"LOAD", "3", "@title", "AS", "t",
		"LIMIT", "0", "10",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("LOAD AS: %s", r.ToBytes())
	}
	body := string(r.ToBytes())
	if !strings.Contains(body, "t") || !strings.Contains(body, "hello") {
		t.Fatalf("want aliased field t=hello, got %s", body)
	}
	if strings.Contains(body, "title") {
		t.Fatalf("source field title must be renamed away: %s", body)
	}
	if strings.Contains(body, "price") {
		t.Fatalf("LOAD projection must drop unloaded price: %s", body)
	}

	both := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "loadas", "*",
		"LOAD", "6", "@title", "AS", "t", "@price", "AS", "p",
		"FILTER", "@p > 15",
		"LIMIT", "0", "10",
	))
	if protocol.IsErrorReply(both) {
		t.Fatalf("LOAD AS + FILTER: %s", both.ToBytes())
	}
	bothBody := string(both.ToBytes())
	if !strings.Contains(bothBody, "p") || !strings.Contains(bothBody, "20") {
		t.Fatalf("FILTER on alias want p=20, got %s", bothBody)
	}
	if strings.Contains(bothBody, "10") {
		t.Fatalf("FILTER @p>15 should drop price 10: %s", bothBody)
	}

	key := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "loadas", "*",
		"LOAD", "1", "@__key",
		"LIMIT", "0", "10",
	))
	if protocol.IsErrorReply(key) {
		t.Fatalf("LOAD __key: %s", key.ToBytes())
	}
	keyBody := string(key.ToBytes())
	if !strings.Contains(keyBody, "__key") || !strings.Contains(keyBody, "la:1") {
		t.Fatalf("want __key=la:1, got %s", keyBody)
	}

	add := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "loadas", "hello",
		"ADDSCORES",
		"LOAD", "1", "@title",
		"LIMIT", "0", "1",
	))
	if protocol.IsErrorReply(add) {
		t.Fatalf("ADDSCORES+LOAD: %s", add.ToBytes())
	}
	addBody := string(add.ToBytes())
	if !strings.Contains(addBody, "__score") || !strings.Contains(addBody, "title") {
		t.Fatalf("ADDSCORES+LOAD must keep __score and title: %s", addBody)
	}

	miss := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "loadas", "*",
		"LOAD", "2", "@title", "AS",
	))
	if !protocol.IsErrorReply(miss) || !strings.Contains(string(miss.ToBytes()), "LOAD path AS name") {
		t.Fatalf("LOAD AS without name: %s", miss.ToBytes())
	}
}

// TestFTAggregateWithCursorMaxIdle accepts COUNT/MAXIDLE in either order and
// rejects Redis 8.x out-of-bounds / missing / non-numeric MAXIDLE wording.
func TestFTAggregateWithCursorMaxIdle(t *testing.T) {
	db := makeTestDB()
	create := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "curidle", "ON", "HASH", "PREFIX", "1", "ci:",
		"SCHEMA", "n", "NUMERIC",
	))
	if protocol.IsErrorReply(create) {
		t.Fatalf("FT.CREATE: %s", create.ToBytes())
	}
	for _, v := range []string{"1", "2", "3"} {
		_ = db.Exec(nil, utils.ToCmdLine("HSET", "ci:"+v, "n", v))
	}

	ok := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "curidle", "*",
		"LOAD", "1", "@n",
		"WITHCURSOR", "MAXIDLE", "5000", "COUNT", "1",
	))
	if protocol.IsErrorReply(ok) {
		t.Fatalf("MAXIDLE before COUNT: %s", ok.ToBytes())
	}

	ok2 := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "curidle", "*",
		"LOAD", "1", "@n",
		"WITHCURSOR", "COUNT", "1", "MAXIDLE", "5000",
	))
	if protocol.IsErrorReply(ok2) {
		t.Fatalf("COUNT before MAXIDLE: %s", ok2.ToBytes())
	}

	neg := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "curidle", "*", "WITHCURSOR", "MAXIDLE", "0",
	))
	if !protocol.IsErrorReply(neg) || !strings.Contains(string(neg.ToBytes()), "outside acceptable bounds") {
		t.Fatalf("MAXIDLE 0: %s", neg.ToBytes())
	}

	miss := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "curidle", "*", "WITHCURSOR", "MAXIDLE",
	))
	if !protocol.IsErrorReply(miss) || !strings.Contains(string(miss.ToBytes()), "Expected an argument") {
		t.Fatalf("MAXIDLE missing: %s", miss.ToBytes())
	}

	abc := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "curidle", "*", "WITHCURSOR", "MAXIDLE", "abc",
	))
	if !protocol.IsErrorReply(abc) || !strings.Contains(string(abc.ToBytes()), "Could not convert") {
		t.Fatalf("MAXIDLE abc: %s", abc.ToBytes())
	}
}
