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

func TestM2bxLuaHKeysSetresp(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "h", "a", "1", "b", "2"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('HKEYS', KEYS[1])
local n = 0
for _ in ipairs(t) do n = n + 1 end
table.sort(t)
return tostring(n) .. ':' .. t[1] .. ':' .. t[2]
`, "1", "h"))
	asserts.AssertBulkReply(t, r, "2:a:b")
}

func TestM2bxCommandDocsGroupFlags(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("COMMAND", "DOCS", "set"))
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("COMMAND DOCS set: %T %s", r, r.ToBytes())
	}
	joined := string(bytesJoin(mb.Args))
	if !strings.Contains(joined, "group") || !strings.Contains(joined, "string") {
		t.Fatalf("want group string: %s", joined)
	}
	if !strings.Contains(joined, "doc_flags") {
		t.Fatalf("want doc_flags: %s", joined)
	}
}

func TestM2bxDynamicHz(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{Databases: 16, DynamicHz: true}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "dynamic-hz")),
		[]string{"dynamic-hz", "yes"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "dynamic-hz", "no")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "dynamic-hz")),
		[]string{"dynamic-hz", "no"})
	bad := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "dynamic-hz", "maybe"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want reject invalid")
	}
}

func TestM2bxACLDryRunSubscribe(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "SETUSER", "subu", "on", "nopass", "+subscribe", "+psubscribe", "+ssubscribe", "resetchannels", "&news:*",
	)), "OK")

	ok := server.Exec(c, utils.ToCmdLine("ACL", "DRYRUN", "subu", "SUBSCRIBE", "news:1"))
	asserts.AssertStatusReply(t, ok, "OK")

	deny := server.Exec(c, utils.ToCmdLine("ACL", "DRYRUN", "subu", "SUBSCRIBE", "other"))
	if !protocol.IsErrorReply(deny) || !strings.Contains(string(deny.ToBytes()), "channel") {
		t.Fatalf("want channel deny: %s", deny.ToBytes())
	}
}

func TestM2bxCommandGetKeysEvalBoundary(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("COMMAND", "GETKEYS", "EVAL", "return 1", "2", "k1"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "Number of keys") {
		t.Fatalf("want numkeys error: %s", r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("COMMAND", "GETKEYS", "EVAL", "return 1", "1", "k1"))
	asserts.AssertMultiBulkReply(t, r, []string{"k1"})

	r = db.Exec(nil, utils.ToCmdLine("COMMAND", "GETKEYS", "PING"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "no key arguments") {
		t.Fatalf("want no key arguments: %s", r.ToBytes())
	}
}
