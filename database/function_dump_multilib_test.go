package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// Batch 60: GODISFN1 multi-library DUMP ↔ RESTORE self-consistency (not Redis wire).
func TestGodisFN1MultiLibFlushRoundTrip(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("FUNCTION", "FLUSH"))

	libA := "#!lua name=b60a\nredis.register_function('fa', function(keys, args) return 'A' end)"
	libB := "#!lua name=b60b\nredis.register_function('fb', function(keys, args) return 'B' end)"
	if protocol.IsErrorReply(db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", libA))) {
		t.Fatal("LOAD A")
	}
	if protocol.IsErrorReply(db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", libB))) {
		t.Fatal("LOAD B")
	}

	dump := db.Exec(nil, utils.ToCmdLine("FUNCTION", "DUMP"))
	bulk, ok := dump.(*protocol.BulkReply)
	if !ok || !strings.HasPrefix(string(bulk.Arg), "GODISFN1") {
		t.Fatalf("DUMP want GODISFN1, got %T %s", dump, dump.ToBytes())
	}

	_ = db.Exec(nil, utils.ToCmdLine("FUNCTION", "FLUSH"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(bulk.Arg), "FLUSH")), "OK")

	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("FCALL", "fa", "0")), "A")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("FCALL", "fb", "0")), "B")
}

func TestGodisFN1MultiLibReplaceAndAppend(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("FUNCTION", "FLUSH"))

	libA := "#!lua name=b60ra\nredis.register_function('ra', function(keys, args) return 'old' end)"
	libB := "#!lua name=b60rb\nredis.register_function('rb', function(keys, args) return 'B' end)"
	if protocol.IsErrorReply(db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", libA))) {
		t.Fatal("LOAD A")
	}
	if protocol.IsErrorReply(db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", libB))) {
		t.Fatal("LOAD B")
	}
	dump := db.Exec(nil, utils.ToCmdLine("FUNCTION", "DUMP"))
	bulk := dump.(*protocol.BulkReply)

	// REPLACE: collision updates A; keep unrelated local lib C.
	_ = db.Exec(nil, utils.ToCmdLine("FUNCTION", "FLUSH"))
	libC := "#!lua name=b60rc\nredis.register_function('rc', function(keys, args) return 'C' end)"
	libA2 := "#!lua name=b60ra\nredis.register_function('ra', function(keys, args) return 'new' end)"
	if protocol.IsErrorReply(db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", libC))) {
		t.Fatal("LOAD C")
	}
	if protocol.IsErrorReply(db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", libA2))) {
		t.Fatal("LOAD A2")
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(bulk.Arg), "REPLACE")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("FCALL", "ra", "0")), "old") // replaced from dump
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("FCALL", "rb", "0")), "B")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("FCALL", "rc", "0")), "C")

	// APPEND with collision must ERR and leave existing libs intact.
	r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(bulk.Arg), "APPEND"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("APPEND collision want ERR, got %s", r.ToBytes())
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("FCALL", "rc", "0")), "C")
}
