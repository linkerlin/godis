package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2dSInterStoreClearsDest(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SADD", "a", "1"))
	db.Exec(nil, utils.ToCmdLine("SADD", "b", "2"))
	db.Exec(nil, utils.ToCmdLine("SET", "dest", "old"))
	db.Exec(nil, utils.ToCmdLine("EXPIRE", "dest", "100"))

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("SINTERSTORE", "dest", "a", "b")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "dest")), 0)
}

func TestM2dSPopSRandMemberMissingWithCount(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("SPOP", "nosuch", "2"))
	asserts.AssertMultiBulkReplySize(t, r, 0)
	r = db.Exec(nil, utils.ToCmdLine("SRANDMEMBER", "nosuch", "3"))
	asserts.AssertMultiBulkReplySize(t, r, 0)
	asserts.AssertNullBulk(t, db.Exec(nil, utils.ToCmdLine("SPOP", "nosuch")))
}

func TestM2dZInterAggregateMinMax(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("ZADD", "z1", "1", "a", "5", "b"))
	db.Exec(nil, utils.ToCmdLine("ZADD", "z2", "2", "a", "3", "b"))

	r := db.Exec(nil, utils.ToCmdLine("ZINTER", "2", "z1", "z2", "AGGREGATE", "MIN", "WITHSCORES"))
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 4 {
		t.Fatalf("ZINTER MIN: %T %s", r, r.ToBytes())
	}
	// scores: a=min(1,2)=1, b=min(5,3)=3 — ordered by score
	asserts.AssertBulkReply(t, protocol.MakeBulkReply(mr.Args[0]), "a")
	asserts.AssertBulkReply(t, protocol.MakeBulkReply(mr.Args[1]), "1")
	asserts.AssertBulkReply(t, protocol.MakeBulkReply(mr.Args[2]), "b")
	asserts.AssertBulkReply(t, protocol.MakeBulkReply(mr.Args[3]), "3")

	r = db.Exec(nil, utils.ToCmdLine("ZINTERSTORE", "out", "2", "z1", "z2", "AGGREGATE", "MAX"))
	asserts.AssertIntReply(t, r, 2)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "out", "a")), "2")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("ZSCORE", "out", "b")), "5")
}

func TestM2dBitPosMissingAndInvalidRange(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "missing", "0")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "missing", "1")), -1)

	db.Exec(nil, utils.ToCmdLine("SET", "b", "\x00"))
	// start past end → invalid range → -1
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BITPOS", "b", "0", "2", "3")), -1)
}

func TestM2dPFMergeSingleDestAndPFAddDirty(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFMERGE", "only")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("EXISTS", "only")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "h", "x")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "h", "x")), 0)
}

func TestM2dHGetDelUndo(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("HSET", "h", "f", "v"))
	undo := undoHGetDel(db, [][]byte{[]byte("h"), []byte("f")})
	if len(undo) == 0 {
		t.Fatal("expected undo commands for HGETDEL")
	}
	found := false
	for _, cmd := range undo {
		if len(cmd) >= 4 && string(cmd[0]) == "HSET" && string(cmd[2]) == "f" && string(cmd[3]) == "v" {
			found = true
		}
	}
	if !found {
		t.Fatalf("undo missing HSET restore: %#v", undo)
	}
}
