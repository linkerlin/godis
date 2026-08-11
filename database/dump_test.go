package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestDumpRestoreRoundTrip(t *testing.T) {
	db := makeTestDB()

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "dump-k", "payload")), "OK")
	dump := db.Exec(nil, utils.ToCmdLine("DUMP", "dump-k"))
	bulk, ok := dump.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) == 0 {
		t.Fatalf("DUMP: got %T %s", dump, dump.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DEL", "dump-k")), 1)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RESTORE", "dump-k", "0", string(bulk.Arg))), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "dump-k")), "payload")
}

func TestRestoreAskingAndReplace(t *testing.T) {
	db := makeTestDB()

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "ask-k", "old")), "OK")
	dump := db.Exec(nil, utils.ToCmdLine("DUMP", "ask-k")).(*protocol.BulkReply)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"RESTORE-ASKING", "ask-k", "0", string(dump.Arg),
	)), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("GET", "ask-k")), "old")

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "rep-k", "x")), "OK")
	newDump := db.Exec(nil, utils.ToCmdLine("DUMP", "rep-k")).(*protocol.BulkReply)
	reply := db.Exec(nil, utils.ToCmdLine("RESTORE", "rep-k", "0", string(newDump.Arg)))
	if !protocol.IsErrorReply(reply) {
		t.Fatalf("expected BUSYKEY, got %s", reply.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"RESTORE", "rep-k", "0", string(newDump.Arg), "REPLACE",
	)), "OK")
}

func TestRestoreRejectsNonGodisOpaqueGarbage(t *testing.T) {
	db := makeTestDB()
	// Fake Redis-module-ish / corrupt DUMP: wrong version+CRC → standard ERR (not silently accepted).
	garbage := make([]byte, 20)
	copy(garbage, []byte("REDIS0009mod\x00\x00"))
	r := db.Exec(nil, utils.ToCmdLine("RESTORE", "bad", "0", string(garbage)))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("want ERR, got %s", r.ToBytes())
	}
	if !strings.Contains(string(r.ToBytes()), "DUMP payload version or checksum are wrong") {
		t.Fatalf("want checksum/version ERR, got %s", r.ToBytes())
	}
}
