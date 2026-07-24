package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2xCopyDeepCopy(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "src", "hello")), "OK")
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("COPY", "src", "dst")), 1)
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "src", "changed")), "OK")
	asserts.AssertBulkReply(t, server.Exec(c, utils.ToCmdLine("GET", "dst")), "hello")

	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("RPUSH", "l1", "a", "b")), 2)
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("COPY", "l1", "l2")), 1)
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("RPUSH", "l1", "c")), 3)
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("LLEN", "l2")), 2)
}

func TestM2xVSimWithAttribs(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "vx", "VALUES", "2", "1", "0", "ELE", "a",
	)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VSETATTR", "vx", "a", `{"k":1}`,
	)), 1)
	r := db.Exec(nil, utils.ToCmdLine(
		"VSIM", "vx", "VALUES", "2", "1", "0", "COUNT", "1", "WITHATTRIBS",
	))
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) < 2 {
		t.Fatalf("VSIM WITHATTRIBS: %s", r.ToBytes())
	}
	if string(mr.Args[0]) != "a" || string(mr.Args[1]) != `{"k":1}` {
		t.Fatalf("unexpected: %s", r.ToBytes())
	}
}

func TestM2xXSetIDMaxDeleted(t *testing.T) {
	db := makeTestDB()
	id := db.Exec(nil, utils.ToCmdLine("XADD", "s", "*", "f", "v"))
	bulk, ok := id.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("XADD: %s", id.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"XSETID", "s", string(bulk.Arg), "MAXDELETEDID", "1-0", "ENTRIESADDED", "5",
	)), "OK")
	info := db.Exec(nil, utils.ToCmdLine("XINFO", "STREAM", "s"))
	raw := string(info.ToBytes())
	if !strings.Contains(raw, "max-deleted-entry-id") || !strings.Contains(raw, "1-0") {
		t.Fatalf("XINFO missing max-deleted: %s", raw)
	}
	if !strings.Contains(raw, "entries-added") {
		t.Fatalf("XINFO missing entries-added: %s", raw)
	}
}

func TestM2xUnknownSubcommandPhrase(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("CONFIG", "NOPE"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "wrong number of arguments") {
		t.Fatalf("expected Redis-style unknown subcommand, got %s", r.ToBytes())
	}
}
