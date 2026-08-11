package database

import (
	"testing"

	"github.com/linkerlin/godis/datastruct/vector"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestVAddQ8TrueQuant(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "vq", "VALUES", "2", "1.262185", "1.958231", "ELE", "e1", "Q8",
	)), 1)
	info := db.Exec(nil, utils.ToCmdLine("VINFO", "vq"))
	mr, ok := info.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) < 2 {
		t.Fatalf("VINFO: %T %s", info, info.ToBytes())
	}
	// pairs: quant-type, int8, ...
	qt, ok := mr.Replies[1].(*protocol.BulkReply)
	if !ok || string(qt.Arg) != "int8" {
		t.Fatalf("want quant-type int8, got %s", info.ToBytes())
	}
	entity, ok := db.GetEntity("vq")
	if !ok {
		t.Fatal("missing key")
	}
	vs := entity.Data.(*vector.VectorSet)
	item, ok := vs.Get("e1")
	if !ok || len(item.Q8) != 2 {
		t.Fatalf("want stored Q8 codes, item=%#v", item)
	}
	// VSIM still works on dequantized data.
	sim := db.Exec(nil, utils.ToCmdLine(
		"VSIM", "vq", "VALUES", "2", "1.26", "1.96", "COUNT", "1",
	))
	mb, ok := sim.(*protocol.MultiBulkReply)
	if !ok || len(mb.Args) < 1 || string(mb.Args[0]) != "e1" {
		t.Fatalf("VSIM: %s", sim.ToBytes())
	}
	// Format mismatch: NOQUANT after Q8.
	bad := db.Exec(nil, utils.ToCmdLine(
		"VADD", "vq", "VALUES", "2", "0", "1", "ELE", "e2", "NOQUANT",
	))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want quant mismatch ERR, got %s", bad.ToBytes())
	}
}
