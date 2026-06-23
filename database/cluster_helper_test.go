package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestExistIn(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	key2 := utils.RandString(10)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value))
	result := testDB.Exec(nil, utils.ToCmdLine("ExistIn", key, key2))
	asserts.AssertMultiBulkReply(t, result, []string{key})
	key3 := utils.RandString(10)
	result = testDB.Exec(nil, utils.ToCmdLine("ExistIn", key2, key3))
	asserts.AssertMultiBulkReplySize(t, result, 0)
}

func TestDumpKeyAndRenameTo(t *testing.T) {
	testDB.Flush()
	key := utils.RandString(10)
	value := utils.RandString(10)
	newKey := key + utils.RandString(2)
	testDB.Exec(nil, utils.ToCmdLine("set", key, value, "ex", "1000"))

	result := testDB.Exec(nil, utils.ToCmdLine("DumpKey", key))
	if protocol.IsErrorReply(result) {
		t.Error("dump key error")
		return
	}
	dumpResult := result.(*protocol.MultiBulkReply)
	result = testDB.Exec(nil, utils.ToCmdLine("RenameTo", newKey,
		string(dumpResult.Args[0]), string(dumpResult.Args[1])))
	asserts.AssertNotError(t, result)
	result = testDB.Exec(nil, utils.ToCmdLine("RenameFrom", key))
	asserts.AssertNotError(t, result)

	result = testDB.Exec(nil, utils.ToCmdLine("exists", key))
	asserts.AssertIntReply(t, result, 0)
	result = testDB.Exec(nil, utils.ToCmdLine("exists", newKey))
	asserts.AssertIntReply(t, result, 1)
	// check ttl
	result = testDB.Exec(nil, utils.ToCmdLine("ttl", newKey))
	intResult, ok := result.(*protocol.IntReply)
	if !ok {
		t.Errorf("expected int protocol, actually %s", result.ToBytes())
		return
	}
	if intResult.Code <= 0 {
		t.Errorf("expected ttl more than 0, actual: %d", intResult.Code)
		return
	}
}

func TestClusterCopyCommands(t *testing.T) {
	testDB.Flush()
	src := utils.RandString(8)
	dst := src + "-copy"
	testDB.Exec(nil, utils.ToCmdLine("SET", src, "copy-val", "EX", "1000"))

	dumpResult := testDB.Exec(nil, utils.ToCmdLine("DumpKey", src))
	dumpMulti, ok := dumpResult.(*protocol.MultiBulkReply)
	if !ok || len(dumpMulti.Args) < 2 {
		t.Fatalf("DumpKey: got %s", dumpResult.ToBytes())
	}

	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("CopyFrom", src)), "OK")
	reply := execCopyTo(testDB, [][]byte{
		[]byte(dst),
		dumpMulti.Args[0],
		dumpMulti.Args[1],
	})
	asserts.AssertStatusReply(t, reply, "OK")
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("GET", dst)), "copy-val")
}

func TestExecRenameNxToDirect(t *testing.T) {
	db := makeTestDB()
	key := "rn-src"
	newKey := "rn-dst"
	db.Exec(nil, utils.ToCmdLine("SET", key, "v"))

	dump := db.Exec(nil, utils.ToCmdLine("DumpKey", key)).(*protocol.MultiBulkReply)
	reply := execRenameNxTo(db, [][]byte{
		[]byte(newKey),
		dump.Args[0],
		dump.Args[1],
	})
	asserts.AssertStatusReply(t, reply, "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", newKey)), "v")
}
