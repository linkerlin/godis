package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2avJSONSetUnknownOption(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("JSON.SET", "k", "$", `{"a":1}`, "BAD"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("expected syntax error, got %s", r.ToBytes())
	}
}

func TestM2avFTDialectAndWithSortKeys(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "avs", "ON", "HASH", "PREFIX", "1", "av:",
		"SCHEMA", "n", "NUMERIC", "SORTABLE", "t", "TEXT",
	)), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "av:1", "n", "10", "t", "hello")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "av:2", "n", "20", "t", "hello")), 2)

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "avs", "hello", "SORTBY", "n", "ASC", "WITHSORTKEYS", "DIALECT", "2",
	))
	mr := ftSearchMultiRaw(r)
	if mr == nil || len(mr.Replies) < 3 {
		t.Fatalf("FT.SEARCH: %T %s", r, r.ToBytes())
	}
	// [total, id, sortkey, fields, ...]
	sk, ok := mr.Replies[2].(*protocol.BulkReply)
	if !ok || string(sk.Arg) != "#10" {
		t.Fatalf("expected sortkey #10 at replies[2], got %T %v / %s", mr.Replies[2], mr.Replies[2], r.ToBytes())
	}
}

func TestM2avSlaveOfWithinMulti(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("MULTI")), "OK")
	r := server.Exec(c, utils.ToCmdLine("SLAVEOF", "no", "one"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "ERR") {
		t.Fatalf("expected ERR reply, got %s", r.ToBytes())
	}
	_ = server.Exec(c, utils.ToCmdLine("DISCARD"))
}
