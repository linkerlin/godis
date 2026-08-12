package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps15HGetExHSetExHGetDelFieldsOnly(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("HSET", "h", "f", "v"))

	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HGETEX", "h", "f")),
		"ERR wrong number of arguments for 'hgetex' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HSETEX", "h", "f", "v")),
		"ERR wrong number of arguments for 'hsetex' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("HGETDEL", "h", "f")),
		"ERR wrong number of arguments for 'hgetdel' command")

	r := db.Exec(nil, utils.ToCmdLine("HGETEX", "h", "FIELDS", "1", "f"))
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 1 || string(mr.Args[0]) != "v" {
		t.Fatalf("HGETEX FIELDS: %T %s", r, r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("HSETEX", "h", "EX", "60", "FIELDS", "1", "f2", "v2"))
	asserts.AssertIntReply(t, r, 1)

	r = db.Exec(nil, utils.ToCmdLine("HGETDEL", "h", "FIELDS", "1", "f"))
	mr, ok = r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 1 || string(mr.Args[0]) != "v" {
		t.Fatalf("HGETDEL FIELDS: %T %s", r, r.ToBytes())
	}
}
