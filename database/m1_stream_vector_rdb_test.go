package database

import (
	"testing"

	"github.com/linkerlin/godis/aof"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM1XInfoAndXGroupSpaced(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("XADD", "s:m1", "1-0", "a", "1"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "s:m1", "g", "$")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATECONSUMER", "s:m1", "g", "c1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATECONSUMER", "s:m1", "g", "c1")), 0)

	info := db.Exec(nil, utils.ToCmdLine("XINFO", "STREAM", "s:m1"))
	if protocol.IsErrorReply(info) {
		t.Fatalf("XINFO STREAM: %s", info.ToBytes())
	}
	groups := db.Exec(nil, utils.ToCmdLine("XINFO", "GROUPS", "s:m1"))
	if _, ok := groups.(*protocol.MultiRawReply); !ok {
		t.Fatalf("XINFO GROUPS: got %T %s", groups, groups.ToBytes())
	}
	consumers := db.Exec(nil, utils.ToCmdLine("XINFO", "CONSUMERS", "s:m1", "g"))
	if _, ok := consumers.(*protocol.MultiRawReply); !ok {
		t.Fatalf("XINFO CONSUMERS: got %T %s", consumers, consumers.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "DELCONSUMER", "s:m1", "g", "c1")), 0)
}

func TestM1VectorRedisAliases(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "v:m1", "VALUES", "3", "1", "0", "0", "ELE", "a",
	)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "v:m1", "VALUES", "3", "0.9", "0.1", "0", "ELE", "b",
	)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("VCARD", "v:m1")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("VDIM", "v:m1")), 3)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("VISMEMBER", "v:m1", "a")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("VISMEMBER", "v:m1", "z")), 0)

	sim := db.Exec(nil, utils.ToCmdLine("VSIM", "v:m1", "VALUES", "3", "1", "0", "0", "COUNT", "2"))
	mb, ok := sim.(*protocol.MultiBulkReply)
	if !ok || len(mb.Args) == 0 {
		t.Fatalf("VSIM: got %T %s", sim, sim.ToBytes())
	}
	if string(mb.Args[0]) != "a" {
		t.Fatalf("VSIM top hit want a, got %s", mb.Args[0])
	}

	scored := db.Exec(nil, utils.ToCmdLine("VSIM", "v:m1", "ELE", "a", "COUNT", "2", "WITHSCORES"))
	if smb, ok := scored.(*protocol.MultiBulkReply); !ok || len(smb.Args) < 2 {
		t.Fatalf("VSIM WITHSCORES: got %T %s", scored, scored.ToBytes())
	}
}

func TestM1OpaqueRoundTrip(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("XADD", "s:op", "1-0", "f", "v"))
	db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "s:op", "g", "$"))
	entity, ok := db.GetEntity("s:op")
	if !ok {
		t.Fatal("missing stream")
	}
	payload, ok := aof.EncodeOpaque(entity)
	if !ok {
		t.Fatal("EncodeOpaque failed")
	}
	cmd := aof.EntityToCmd("s:op2", entity)
	if cmd == nil || string(cmd.Args[0]) != "godis.restore" {
		t.Fatalf("EntityToCmd: %#v", cmd)
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine3("godis.restore", []byte("s:op2"), payload)), "OK")
	s, errReply := db.getAsStream("s:op2")
	if errReply != nil || s == nil {
		t.Fatalf("restored stream missing: %v", errReply)
	}
	if s.Len() != 1 {
		t.Fatalf("len=%d", s.Len())
	}
	if _, err := s.GetGroup("g"); err != nil {
		t.Fatalf("group missing: %v", err)
	}
}
