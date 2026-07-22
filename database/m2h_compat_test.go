package database

import (
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2hGeoAddNXCH(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"GEOADD", "g", "13.361389", "38.115556", "Palermo")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"GEOADD", "g", "NX", "15.087269", "37.502669", "Palermo")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"GEOADD", "g", "NX", "15.087269", "37.502669", "Catania")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"GEOADD", "g", "CH", "12.496366", "41.902782", "Palermo")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"GEOADD", "g", "XX", "0", "0", "Missing")), 0)

	r := db.Exec(nil, utils.ToCmdLine("GEOADD", "g", "200", "0", "bad"))
	asserts.AssertErrReply(t, r, "ERR invalid longitude,latitude pair 200,0")
}

func TestM2hJSONArrInsertMGet(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("JSON.SET", "j", ".", `[1,3]`))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"JSON.ARRINSERT", "j", ".", "1", "2")), 3)
	r := db.Exec(nil, utils.ToCmdLine("JSON.GET", "j"))
	asserts.AssertBulkReply(t, r, "[1,2,3]")

	db.Exec(nil, utils.ToCmdLine("JSON.SET", "a", ".", `{"x":1}`))
	db.Exec(nil, utils.ToCmdLine("JSON.SET", "b", ".", `{"x":2}`))
	r = db.Exec(nil, utils.ToCmdLine("JSON.MGET", "a", "b", "missing", ".x"))
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 3 {
		t.Fatalf("JSON.MGET: %T %s", r, r.ToBytes())
	}
	if string(mr.Args[0]) != "1" || string(mr.Args[1]) != "2" || mr.Args[2] != nil {
		t.Fatalf("JSON.MGET unexpected: %q %q %v", mr.Args[0], mr.Args[1], mr.Args[2])
	}
}

func TestM2hProbBFTopKCF(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("BF.ADD", "bf", "a"))
	r := db.Exec(nil, utils.ToCmdLine("BF.MEXISTS", "bf", "a", "b"))
	mr := r.(*protocol.MultiBulkReply)
	if string(mr.Args[0]) != "1" || string(mr.Args[1]) != "0" {
		t.Fatalf("BF.MEXISTS: %s,%s", mr.Args[0], mr.Args[1])
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BF.CARD", "bf")), 1)

	db.Exec(nil, utils.ToCmdLine("TOPK.RESERVE", "tk", "3"))
	db.Exec(nil, utils.ToCmdLine("TOPK.ADD", "tk", "x", "y", "z"))
	r = db.Exec(nil, utils.ToCmdLine("TOPK.COUNT", "tk", "x", "missing"))
	mr = r.(*protocol.MultiBulkReply)
	if string(mr.Args[0]) == "0" || string(mr.Args[1]) != "0" {
		t.Fatalf("TOPK.COUNT: %s,%s", mr.Args[0], mr.Args[1])
	}
	r = db.Exec(nil, utils.ToCmdLine("TOPK.INFO", "tk"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("TOPK.INFO: %s", r.ToBytes())
	}

	db.Exec(nil, utils.ToCmdLine("CF.RESERVE", "cf", "100"))
	db.Exec(nil, utils.ToCmdLine("CF.ADD", "cf", "item"))
	db.Exec(nil, utils.ToCmdLine("CF.ADD", "cf", "item"))
	n := db.Exec(nil, utils.ToCmdLine("CF.COUNT", "cf", "item"))
	ir, ok := n.(*protocol.IntReply)
	if !ok || ir.Code < 1 {
		t.Fatalf("CF.COUNT expected >=1, got %T %s", n, n.ToBytes())
	}
}

func TestM2hTSDecrByAOFCmd(t *testing.T) {
	db := makeTestDB()
	var aofCmds []string
	db.addAof = func(cmdLine CmdLine) {
		if len(cmdLine) > 0 {
			aofCmds = append(aofCmds, strings.ToLower(string(cmdLine[0])))
		}
	}
	db.Exec(nil, utils.ToCmdLine("TS.CREATE", "ts"))
	db.Exec(nil, utils.ToCmdLine("TS.ADD", "ts", "*", "10"))
	db.Exec(nil, utils.ToCmdLine("TS.DECRBY", "ts", "3"))
	found := false
	for _, c := range aofCmds {
		if c == "ts.decrby" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("TS.DECRBY AOF expected ts.decrby, got %v", aofCmds)
	}
}

func TestM2hXClaim(t *testing.T) {
	db := makeTestDB()
	key := "s:claim"
	idReply := db.Exec(nil, utils.ToCmdLine("XADD", key, "*", "f", "v"))
	id := string(idReply.(*protocol.BulkReply).Arg)
	db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", key, "g", "0-0"))
	read := db.Exec(nil, utils.ToCmdLine(
		"XREADGROUP", "GROUP", "g", "c1", "STREAMS", key, ">"))
	if protocol.IsErrorReply(read) {
		t.Fatalf("XREADGROUP: %s", read.ToBytes())
	}

	r := db.Exec(nil, utils.ToCmdLine("XCLAIM", key, "g", "c2", "60000", id))
	if replyLen(r) != 0 {
		t.Fatalf("XCLAIM with high idle should be empty, got %s", r.ToBytes())
	}

	time.Sleep(5 * time.Millisecond)
	r = db.Exec(nil, utils.ToCmdLine("XCLAIM", key, "g", "c2", "0", id))
	if protocol.IsErrorReply(r) {
		t.Fatalf("XCLAIM: %s", r.ToBytes())
	}
	if replyLen(r) != 1 {
		t.Fatalf("XCLAIM expected 1 entry, got %T %s", r, r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("XCLAIM", key, "g", "c3", "0", id, "JUSTID"))
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mb.Args) != 1 || string(mb.Args[0]) != id {
		t.Fatalf("XCLAIM JUSTID: %T %s", r, r.ToBytes())
	}
}

func replyLen(r redis.Reply) int {
	switch v := r.(type) {
	case *protocol.MultiRawReply:
		return len(v.Replies)
	case *protocol.MultiBulkReply:
		return len(v.Args)
	case *protocol.EmptyMultiBulkReply:
		return 0
	default:
		return -1
	}
}
