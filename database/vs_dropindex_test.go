package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestVSDropIndex(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("VSDROPINDEX", "missing")), 0)

	r := db.Exec(nil, utils.ToCmdLine("VSADD", "vs", "a", "[1,0,0]"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("VSADD: %s", r.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("VSLEN", "vs")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("VSDROPINDEX", "vs")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("VSLEN", "vs")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("VS.DROPINDEX", "vs")), 0)

	_ = db.Exec(nil, utils.ToCmdLine("SET", "s", "v"))
	wrong := db.Exec(nil, utils.ToCmdLine("VSDROPINDEX", "s"))
	if _, ok := wrong.(*protocol.WrongTypeErrReply); !ok {
		t.Fatalf("want WRONGTYPE, got %T %s", wrong, wrong.ToBytes())
	}
}
