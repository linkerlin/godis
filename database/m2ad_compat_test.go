package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2adTSDuplicatePolicy(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"TS.CREATE", "ts", "DUPLICATE_POLICY", "BLOCK",
	)), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TS.ADD", "ts", "100", "1")), 100)
	blocked := db.Exec(nil, utils.ToCmdLine("TS.ADD", "ts", "100", "2"))
	if !protocol.IsErrorReply(blocked) {
		t.Fatalf("BLOCK should reject duplicate: %s", blocked.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"TS.CREATE", "ts2", "DUPLICATE_POLICY", "LAST",
	)), "OK")
	db.Exec(nil, utils.ToCmdLine("TS.ADD", "ts2", "100", "1"))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("TS.ADD", "ts2", "100", "9")), 100)
	got := db.Exec(nil, utils.ToCmdLine("TS.GET", "ts2"))
	mr, ok := got.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) < 2 || string(mr.Args[1]) != "9" {
		t.Fatalf("LAST policy: %s", got.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"TS.ADD", "ts", "200", "1", "ON_DUPLICATE", "SUM",
	)), 200)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"TS.ADD", "ts", "200", "3", "ON_DUPLICATE", "SUM",
	)), 200)
	g2 := db.Exec(nil, utils.ToCmdLine("TS.GET", "ts"))
	mr2, ok := g2.(*protocol.MultiBulkReply)
	if !ok || len(mr2.Args) < 2 || string(mr2.Args[0]) != "200" {
		t.Fatalf("SUM GET: %s", g2.ToBytes())
	}
	if string(mr2.Args[1]) != "4" {
		t.Fatalf("SUM want 4 got %q", mr2.Args[1])
	}
}

func TestM2adXGroupSetIDEntriesRead(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("XADD", "s", "*", "f", "1"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"XGROUP", "CREATE", "s", "g", "0-0",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"XGROUP", "SETID", "s", "g", "0-0", "ENTRIESREAD", "7",
	)), "OK")
	s, _ := db.getAsStream("s")
	group, err := s.GetGroup("g")
	if err != nil || group.EntriesRead != 7 {
		t.Fatalf("ENTRIESREAD: err=%v entries=%d", err, group.EntriesRead)
	}
}

func TestM2adVSimFilter(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "vx", "VALUES", "2", "1", "0", "ELE", "a",
	)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "vx", "VALUES", "2", "0", "1", "ELE", "b",
	)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VSETATTR", "vx", "a", `{"tag":"keep"}`,
	)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VSETATTR", "vx", "b", `{"tag":"drop"}`,
	)), 1)
	r := db.Exec(nil, utils.ToCmdLine(
		"VSIM", "vx", "VALUES", "2", "1", "0", "COUNT", "10", "FILTER", `.tag=="keep"`,
	))
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 1 || string(mr.Args[0]) != "a" {
		t.Fatalf("FILTER: %s", r.ToBytes())
	}
}
