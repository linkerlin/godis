package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 86 R4-1 extras: miss HGET/HMGET/SCARD/LLEN/ZCARD, MGET partial, PFADD idempotent, BITOP OR.
func TestR41Batch86Extras(t *testing.T) {
	db := makeTestDB()

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SCARD", "b86smiss")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("LLEN", "b86lmiss")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("ZCARD", "b86zmiss")), 0)

	hget := db.Exec(nil, utils.ToCmdLine("HGET", "b86hmiss", "f"))
	if _, ok := hget.(*protocol.NullBulkReply); !ok {
		t.Fatalf("HGET miss want null bulk: %T %s", hget, hget.ToBytes())
	}
	hm := db.Exec(nil, utils.ToCmdLine("HMGET", "b86hmiss", "f", "g"))
	multi, ok := hm.(*protocol.MultiBulkReply)
	if !ok || len(multi.Args) != 2 {
		t.Fatalf("HMGET miss shape: %T %s", hm, hm.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b86m1", "a")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "b86m2", "b")), "OK")
	mget := db.Exec(nil, utils.ToCmdLine("MGET", "b86m1", "b86miss", "b86m2"))
	mb, ok := mget.(*protocol.MultiBulkReply)
	if !ok || len(mb.Args) != 3 || string(mb.Args[0]) != "a" || string(mb.Args[2]) != "b" {
		t.Fatalf("MGET partial: %s", mget.ToBytes())
	}
	if mb.Args[1] != nil {
		t.Fatalf("MGET middle miss want nil arg, got %q", mb.Args[1])
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "b86pf", "a")), 1)
	_ = db.Exec(nil, utils.ToCmdLine("PFADD", "b86pf", "a"))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "b86pf")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b86b1", "0", "1")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SETBIT", "b86b2", "1", "1")), 0)
	bitop := db.Exec(nil, utils.ToCmdLine("BITOP", "OR", "b86bout", "b86b1", "b86b2"))
	if protocol.IsErrorReply(bitop) {
		t.Fatalf("BITOP OR: %s", bitop.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b86bout", "0")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("GETBIT", "b86bout", "1")), 1)
}
