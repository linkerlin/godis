package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2oFTAliasAndTagVals(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx", "SCHEMA", "tags", "TAG", "title", "TEXT",
	)), "OK")
	add := db.Exec(nil, utils.ToCmdLine(
		"FT.ADD", "idx", "d1", "FIELDS", "tags", "red,blue", "title", "hello",
	))
	if protocol.IsErrorReply(add) {
		t.Fatalf("FT.ADD: %s", add.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.ALIASADD", "a1", "idx")), "OK")
	info := db.Exec(nil, utils.ToCmdLine("FT.INFO", "a1"))
	if protocol.IsErrorReply(info) {
		t.Fatalf("FT.INFO via alias: %s", info.ToBytes())
	}

	tags := db.Exec(nil, utils.ToCmdLine("FT.TAGVALS", "a1", "tags"))
	multi, ok := tags.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("FT.TAGVALS: %T %s", tags, tags.ToBytes())
	}
	found := map[string]bool{}
	for _, a := range multi.Args {
		found[string(a)] = true
	}
	if !found["blue"] || !found["red"] {
		t.Fatalf("FT.TAGVALS missing tags: %v", found)
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.ALIASUPDATE", "a1", "idx")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.ALIASDEL", "a1")), "OK")
}

func TestM2oTDigestByRankMergeTrimmed(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TDIGEST.CREATE", "td")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TDIGEST.ADD", "td", "1", "2", "3", "4", "5")), "OK")

	by := db.Exec(nil, utils.ToCmdLine("TDIGEST.BYRANK", "td", "0"))
	if _, ok := by.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("BYRANK: %T %s", by, by.ToBytes())
	}
	tm := db.Exec(nil, utils.ToCmdLine("TDIGEST.TRIMMED_MEAN", "td", "0", "1"))
	if _, ok := tm.(*protocol.BulkReply); !ok {
		t.Fatalf("TRIMMED_MEAN: %T %s", tm, tm.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TDIGEST.CREATE", "td2")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TDIGEST.ADD", "td2", "10")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TDIGEST.MERGE", "tdm", "2", "td", "td2")), "OK")
}

func TestM2oLolwutAndPFDebug(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("LOLWUT"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), "Godis") {
		t.Fatalf("LOLWUT: %T %s", r, r.ToBytes())
	}

	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "h", "a", "b")), 1)
	regs := db.Exec(nil, utils.ToCmdLine("PFDEBUG", "GETREG", "h"))
	multi, ok := regs.(*protocol.MultiBulkReply)
	if !ok || len(multi.Args) != 16384 {
		n := -1
		if ok {
			n = len(multi.Args)
		}
		t.Fatalf("PFDEBUG GETREG: %T len=%d %s", regs, n, regs.ToBytes())
	}
}
