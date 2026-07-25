package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/parser"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2axCommandList(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("COMMAND", "LIST"))
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) < 10 {
		t.Fatalf("COMMAND LIST: %T %s", r, r.ToBytes())
	}
	r = db.Exec(nil, utils.ToCmdLine("COMMAND", "LIST", "FILTERBY", "PATTERN", "get*"))
	mr, ok = r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) < 1 {
		t.Fatalf("COMMAND LIST FILTERBY: %T %s", r, r.ToBytes())
	}
	for _, a := range mr.Args {
		if !strings.HasPrefix(string(a), "get") {
			t.Fatalf("unexpected name %q", a)
		}
	}
}

func TestM2axGetRangeMissingKey(t *testing.T) {
	db := makeTestDB()
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GETRANGE", "missing", "0", "1")), "")
}

func TestM2axObjectEncodingInt(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "n", "42")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "n")), "int")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "s", "hello")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "s")), "raw")
}

func TestM2axProtoMaxBulkLen(t *testing.T) {
	old := config.Properties.ProtoMaxBulkLen
	config.Properties.ProtoMaxBulkLen = 16
	defer func() { config.Properties.ProtoMaxBulkLen = old }()

	// Bulk longer than 16 should fail parse
	payload := "$20\r\n01234567890123456789\r\n"
	ch := parser.ParseStream(strings.NewReader(payload))
	p := <-ch
	if p.Err == nil {
		t.Fatal("expected proto-max-bulk-len error")
	}
}

func TestM2axLuaErrorStatusReply(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("EVAL", `return redis.error_reply("ERR boom")`, "0"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "boom") {
		t.Fatalf("error_reply: %s", r.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("EVAL", `return redis.status_reply("PONG")`, "0")), "PONG")
	r = db.Exec(nil, utils.ToCmdLine("EVAL", `redis.setresp(3); return 1`, "0"))
	asserts.AssertIntReply(t, r, 1)
}
