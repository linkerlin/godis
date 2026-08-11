package database

import (
	"testing"

	"github.com/linkerlin/godis/datastruct/vector"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestVAddBINTrueQuant(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "vb", "VALUES", "4", "1.2", "0.3", "-0.5", "-1.0", "ELE", "e1", "BIN",
	)), 1)
	info := db.Exec(nil, utils.ToCmdLine("VINFO", "vb"))
	mr, ok := info.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) < 2 {
		t.Fatalf("VINFO: %T %s", info, info.ToBytes())
	}
	qt, ok := mr.Replies[1].(*protocol.BulkReply)
	if !ok || string(qt.Arg) != "bin" {
		t.Fatalf("want quant-type bin, got %s", info.ToBytes())
	}
	entity, ok := db.GetEntity("vb")
	if !ok {
		t.Fatal("missing key")
	}
	vs := entity.Data.(*vector.VectorSet)
	item, ok := vs.Get("e1")
	if !ok || len(item.Bin) == 0 {
		t.Fatalf("want stored BIN bits, item=%#v", item)
	}
	sim := db.Exec(nil, utils.ToCmdLine(
		"VSIM", "vb", "VALUES", "4", "1", "1", "-1", "-1", "COUNT", "1",
	))
	mb, ok := sim.(*protocol.MultiBulkReply)
	if !ok || len(mb.Args) < 1 || string(mb.Args[0]) != "e1" {
		t.Fatalf("VSIM: %s", sim.ToBytes())
	}
	bad := db.Exec(nil, utils.ToCmdLine(
		"VADD", "vb", "VALUES", "4", "0", "0", "0", "0", "ELE", "e2", "Q8",
	))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want quant mismatch ERR, got %s", bad.ToBytes())
	}
}
