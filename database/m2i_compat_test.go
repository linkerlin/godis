package database

import (
	"testing"
	"time"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2iTSMAddAndMRevRange(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine(
		"TS.MADD", "ts1", "1000", "1.5", "ts2", "2000", "2.5"))
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 2 {
		t.Fatalf("TS.MADD: %T %s", r, r.ToBytes())
	}
	asserts.AssertBulkReply(t, protocol.MakeBulkReply(mr.Args[0]), "1000")
	asserts.AssertBulkReply(t, protocol.MakeBulkReply(mr.Args[1]), "2000")

	db.Exec(nil, utils.ToCmdLine("TS.CREATE", "tsm", "LABELS", "a", "1"))
	db.Exec(nil, utils.ToCmdLine("TS.ADD", "tsm", "10", "1"))
	db.Exec(nil, utils.ToCmdLine("TS.ADD", "tsm", "20", "2"))
	r = db.Exec(nil, utils.ToCmdLine("TS.MREVRANGE", "-", "+", "FILTER", "a=1"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("TS.MREVRANGE: %s", r.ToBytes())
	}
}

func TestM2iFTSynAdd(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("FT.SYNADD", "idx", "hello", "hi", "hey"))
	asserts.AssertIntReplyGreaterThan(t, r, 0)
	dump := db.Exec(nil, utils.ToCmdLine("FT.SYNDUMP", "idx"))
	if protocol.IsErrorReply(dump) {
		t.Fatalf("FT.SYNDUMP: %s", dump.ToBytes())
	}
}

func TestM2iJSONClearToggle(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("JSON.SET", "j", ".", `{"a":[1,2],"b":true}`))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("JSON.CLEAR", "j", ".a")), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("JSON.GET", "j", ".a")), "[]")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("JSON.TOGGLE", "j", ".b")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("JSON.TOGGLE", "j", ".b")), 1)
}

func TestM2iXAutoClaim(t *testing.T) {
	db := makeTestDB()
	key := "s:ac"
	idReply := db.Exec(nil, utils.ToCmdLine("XADD", key, "*", "f", "v"))
	id := string(idReply.(*protocol.BulkReply).Arg)
	db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", key, "g", "0-0"))
	read := db.Exec(nil, utils.ToCmdLine(
		"XREADGROUP", "GROUP", "g", "c1", "STREAMS", key, ">"))
	if protocol.IsErrorReply(read) {
		t.Fatalf("XREADGROUP: %s", read.ToBytes())
	}
	time.Sleep(5 * time.Millisecond)
	r := db.Exec(nil, utils.ToCmdLine(
		"XAUTOCLAIM", key, "g", "c2", "0", "0-0", "COUNT", "10"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("XAUTOCLAIM: %s", r.ToBytes())
	}
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) < 2 {
		t.Fatalf("XAUTOCLAIM format: %T %s", r, r.ToBytes())
	}
	_ = id
}
