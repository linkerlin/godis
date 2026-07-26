package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2brSlowlogHelp(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("SLOWLOG", "HELP"))
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("SLOWLOG HELP: %T %s", r, r.ToBytes())
	}
	joined := string(bytesJoin(mb.Args))
	if !strings.Contains(joined, "GET") || !strings.Contains(joined, "RESET") {
		t.Fatalf("help missing GET/RESET: %s", joined)
	}
	bad := server.Exec(c, utils.ToCmdLine("SLOWLOG", "NOPE"))
	if !protocol.IsErrorReply(bad) || !strings.Contains(string(bad.ToBytes()), "SLOWLOG HELP") {
		t.Fatalf("want Try SLOWLOG HELP: %s", bad.ToBytes())
	}
}

func TestM2brCommandHelp(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("COMMAND", "HELP"))
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("COMMAND HELP: %T %s", r, r.ToBytes())
	}
	joined := string(bytesJoin(mb.Args))
	if !strings.Contains(joined, "DOCS") || !strings.Contains(joined, "GETKEYS") {
		t.Fatalf("help missing DOCS/GETKEYS: %s", joined)
	}
}

func TestM2brClientHelpExtras(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("CLIENT", "HELP"))
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("CLIENT HELP: %T %s", r, r.ToBytes())
	}
	joined := string(bytesJoin(mb.Args))
	for _, want := range []string{"TRACKINGINFO", "SETINFO", "HELP"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("CLIENT HELP missing %s: %s", want, joined)
		}
	}
}

func TestM2brObjectListListpack(t *testing.T) {
	db := makeTestDB()
	_ = db.Exec(nil, utils.ToCmdLine("LPUSH", "l", "a", "b"))
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "l")), "listpack")

	for i := 0; i < 130; i++ {
		_ = db.Exec(nil, utils.ToCmdLine("LPUSH", "lbig", "x"+utils.RandString(4)))
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "lbig")), "quicklist")
}

func TestM2brLuaZUnionKeysSetresp(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z1", "1", "a", "2", "b"))
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z2", "1", "a", "3", "c"))
	_ = db.Exec(nil, utils.ToCmdLine("SET", "k1", "v"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('ZUNION', 2, KEYS[1], KEYS[2], 'WITHSCORES')
return tostring(t['a']) .. ':' .. tostring(t['c'] ~= nil)
`, "2", "z1", "z2"))
	asserts.AssertBulkReply(t, r, "2:true")

	r = db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('KEYS', 'k*')
return tostring(t['k1'] == true)
`, "0"))
	asserts.AssertBulkReply(t, r, "true")
}
