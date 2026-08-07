package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2auFTSchemaASJSONPath(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "jas", "ON", "JSON", "PREFIX", "1", "doc:",
		"SCHEMA", "$.meta.title", "AS", "title", "TEXT",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"JSON.SET", "doc:1", "$", `{"meta":{"title":"hello world"}}`,
	)), "OK")
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "jas", "hello"))
	mr := ftSearchMultiRaw(r)
	if mr == nil || len(mr.Replies) < 2 {
		t.Fatalf("FT.SEARCH: %T %s", r, r.ToBytes())
	}
	total, ok := mr.Replies[0].(*protocol.IntReply)
	if !ok || total.Code < 1 {
		t.Fatalf("expected hits, got %s", r.ToBytes())
	}
}
