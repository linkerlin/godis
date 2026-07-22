package database

import (
	"path/filepath"
	"testing"

	"github.com/linkerlin/godis/aof"
	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2jCMSInitByProbMergeInfo(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"CMS.INITBYPROB", "cms", "0.001", "0.99")), "OK")
	info := db.Exec(nil, utils.ToCmdLine("CMS.INFO", "cms"))
	if protocol.IsErrorReply(info) {
		t.Fatalf("CMS.INFO: %s", info.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"CMS.INITBYDIM", "cms2", "2000", "5")), "OK")
	db.Exec(nil, utils.ToCmdLine("CMS.INCRBY", "cms2", "a", "3"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"CMS.INITBYDIM", "dest", "2000", "5")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"CMS.MERGE", "dest", "1", "cms2")), "OK")
}

func TestM2jCFInsertMExists(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine(
		"CF.INSERT", "cf", "CAPACITY", "100", "ITEMS", "x", "y"))
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 2 {
		t.Fatalf("CF.INSERT: %T %s", r, r.ToBytes())
	}
	r = db.Exec(nil, utils.ToCmdLine("CF.MEXISTS", "cf", "x", "z"))
	mr = r.(*protocol.MultiBulkReply)
	if string(mr.Args[0]) != "1" || string(mr.Args[1]) != "0" {
		t.Fatalf("CF.MEXISTS: %s,%s", mr.Args[0], mr.Args[1])
	}
	r = db.Exec(nil, utils.ToCmdLine(
		"CF.INSERTNX", "cf", "ITEMS", "x", "z"))
	mr = r.(*protocol.MultiBulkReply)
	if string(mr.Args[0]) != "0" || string(mr.Args[1]) != "1" {
		t.Fatalf("CF.INSERTNX: %s,%s", mr.Args[0], mr.Args[1])
	}
}

func TestM2jSynonymPerIndex(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("FT.SYNADD", "idx1", "a", "b"))
	db.Exec(nil, utils.ToCmdLine("FT.SYNADD", "idx2", "c", "d"))
	d1 := db.Exec(nil, utils.ToCmdLine("FT.SYNDUMP", "idx1"))
	d2 := db.Exec(nil, utils.ToCmdLine("FT.SYNDUMP", "idx2"))
	if protocol.IsErrorReply(d1) || protocol.IsErrorReply(d2) {
		t.Fatalf("SYNDUMP: %s / %s", d1.ToBytes(), d2.ToBytes())
	}
	if string(d1.ToBytes()) == string(d2.ToBytes()) {
		t.Fatalf("synonym DBs not isolated")
	}
}

func TestM2jJSONMSet(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"JSON.MSET", "j1", ".", `{"a":1}`, "j2", ".", `{"b":2}`)), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("JSON.GET", "j1", ".a")), "1")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("JSON.GET", "j2", ".b")), "2")
}

func TestM2jXSetID(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("XADD", "s", "1-0", "f", "v"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"XSETID", "s", "2-0", "ENTRIESADDED", "5")), "OK")
	r := db.Exec(nil, utils.ToCmdLine("XSETID", "s", "1-0"))
	asserts.AssertErrReply(t, r, "ERR The ID specified in XSETID is smaller than the current top-level ID")
}

func TestM2jSaveWithoutAOF(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "save-k", "v")), "OK")

	oldPersister := server.persister
	server.persister = nil
	defer func() { server.persister = oldPersister }()

	oldName := ""
	if config.Properties != nil {
		oldName = config.Properties.RDBFilename
		config.Properties.RDBFilename = filepath.Join(t.TempDir(), "dump.rdb")
		defer func() { config.Properties.RDBFilename = oldName }()
	}

	r := SaveRDB(server, nil)
	if protocol.IsErrorReply(r) {
		t.Fatalf("SAVE without AOF: %s", r.ToBytes())
	}
	asserts.AssertStatusReply(t, r, "OK")

	ls := server.Exec(c, utils.ToCmdLine("LASTSAVE"))
	ir, ok := ls.(*protocol.IntReply)
	if !ok || ir.Code <= 0 {
		t.Fatalf("LASTSAVE: %T %s", ls, ls.ToBytes())
	}

	// also exercise WriteRDBFromDB directly
	out := filepath.Join(t.TempDir(), "direct.rdb")
	if err := aof.WriteRDBFromDB(out, server); err != nil {
		t.Fatalf("WriteRDBFromDB: %v", err)
	}
}
