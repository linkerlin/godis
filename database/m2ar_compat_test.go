package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2arACLKeyRWSelectors(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "SETUSER", "rwu", "on", "nopass", "+@all", "%R~read:*", "%W~write:*",
	)), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "DRYRUN", "rwu", "GET", "read:1",
	)), "OK")
	r := server.Exec(c, utils.ToCmdLine("ACL", "DRYRUN", "rwu", "SET", "read:1", "x"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("SET on %%R~ key should deny: %s", r.ToBytes())
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "DRYRUN", "rwu", "SET", "write:1", "x",
	)), "OK")
	r = server.Exec(c, utils.ToCmdLine("ACL", "DRYRUN", "rwu", "GET", "write:1"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("GET on %%W~ key should deny: %s", r.ToBytes())
	}
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("ACL", "DELUSER", "rwu")), 1)
}

func TestM2arJSONFTAutoIndex(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "jidx", "ON", "JSON", "PREFIX", "1", "j:", "SCHEMA", "title", "TEXT",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"JSON.SET", "j:1", "$", `{"title":"hello world"}`,
	)), "OK")
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "jidx", "hello"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) < 2 {
		t.Fatalf("FT.SEARCH after JSON.SET: %T %s", r, r.ToBytes())
	}
	total, ok := mr.Replies[0].(*protocol.IntReply)
	if !ok || total.Code < 1 {
		t.Fatalf("expected hits, got %s", r.ToBytes())
	}
	db.Exec(nil, utils.ToCmdLine("JSON.DEL", "j:1"))
	r = db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "jidx", "hello"))
	mr, ok = r.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("FT.SEARCH after DEL: %T", r)
	}
	total, ok = mr.Replies[0].(*protocol.IntReply)
	if !ok || total.Code != 0 {
		t.Fatalf("expected 0 hits after DEL, got %s", r.ToBytes())
	}
}
