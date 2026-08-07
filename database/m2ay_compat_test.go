package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2ayJSONEnhancedNumAndLen(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"JSON.SET", "j", "$", `{"a":1,"b":[1,2,3],"c":{"x":1,"y":2}}`,
	)), "OK")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("JSON.NUMINCRBY", "j", "$.a", "2")), "[3]")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("JSON.NUMMULTBY", "j", "$.a", "2")), "[6]")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("JSON.NUMINCRBY", "j", ".a", "1")), "7")

	arr := db.Exec(nil, utils.ToCmdLine("JSON.ARRLEN", "j", "$.b"))
	mr := ftSearchMultiRaw(arr)
	if mr == nil || len(mr.Replies) != 1 {
		t.Fatalf("ARRLEN $: %T %s", arr, arr.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], 3)

	obj := db.Exec(nil, utils.ToCmdLine("JSON.OBJLEN", "j", "$.c"))
	mr = ftSearchMultiRaw(obj)
	if mr == nil || len(mr.Replies) != 1 {
		t.Fatalf("OBJLEN $: %T %s", obj, obj.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], 2)
}

func TestM2ayVAddNXXxSetAttr(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "v", "VALUES", "2", "1", "0", "ELE", "e1", "SETATTR", `{"k":1}`,
	)), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("VGETATTR", "v", "e1")), `{"k":1}`)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "v", "VALUES", "2", "0", "1", "ELE", "e1", "NX",
	)), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "v", "VALUES", "2", "0", "1", "ELE", "missing", "XX",
	)), 0)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "v", "VALUES", "2", "0", "1", "ELE", "e1", "XX", "SETATTR", `{"k":2}`,
	)), 0)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("VGETATTR", "v", "e1")), `{"k":2}`)
}

func TestM2ayACLCheckCmd(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	// DeniedCommands takes precedence over AllCommands.
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "SETUSER", "default", "-set",
	)), "OK")
	defer func() {
		_ = server.Exec(c, utils.ToCmdLine("ACL", "SETUSER", "default", "+set"))
	}()

	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine(
		"EVAL", `return redis.acl_check_cmd('set', 'k', 'v')`, "0",
	)), 0)
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine(
		"EVAL", `return redis.acl_check_cmd('get', 'k')`, "0",
	)), 1)
}

func TestM2ayFTSearchInKeys(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "ink", "ON", "HASH", "PREFIX", "1", "d:", "SCHEMA", "t", "TEXT",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "ink", "d:1", "FIELDS", "t", "hello world"))
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "ink", "d:2", "FIELDS", "t", "hello there"))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "ink", "hello", "INKEYS", "1", "d:1", "NOCONTENT",
	))
	mr := ftSearchMultiRaw(r)
	if mr == nil || len(mr.Replies) < 2 {
		t.Fatalf("INKEYS: %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, mr.Replies[0], 1)
	bulk, ok := mr.Replies[1].(*protocol.BulkReply)
	if !ok || string(bulk.Arg) != "d:1" {
		t.Fatalf("expected d:1, got %s", r.ToBytes())
	}
}
