package database

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

func TestGodisFN1EmptyDumpRoundTrip(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("FUNCTION", "FLUSH"))
	dump := db.Exec(nil, utils.ToCmdLine("FUNCTION", "DUMP"))
	bulk, ok := dump.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) < 12 || string(bulk.Arg[:8]) != "GODISFN1" {
		t.Fatalf("empty DUMP want GODISFN1+count0, got %v", dump.ToBytes())
	}
	if n := binary.BigEndian.Uint32(bulk.Arg[8:12]); n != 0 {
		t.Fatalf("count=%d want 0", n)
	}
	r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(bulk.Arg), "FLUSH"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("RESTORE empty GODISFN1: %s", r.ToBytes())
	}
}

func TestGodisFN1TruncatedPayloadERR(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	// Magic + incomplete count / truncated library record.
	cases := [][]byte{
		[]byte("GODISFN1"),
		append([]byte("GODISFN1"), 0, 0, 0),               // short count
		append([]byte("GODISFN1"), 0, 0, 0, 1),            // count=1, no records
		append([]byte("GODISFN1"), 0, 0, 0, 1, 0, 0, 0, 3, 'a', 'b'), // truncated name
	}
	for i, p := range cases {
		r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(p), "FLUSH"))
		if !protocol.IsErrorReply(r) {
			t.Fatalf("case %d want ERR, got %s", i, r.ToBytes())
		}
		msg := string(r.ToBytes())
		if !strings.Contains(msg, "GODISFN1") || !strings.Contains(strings.ToLower(msg), "corrupt") && !strings.Contains(msg, "truncated") {
			t.Fatalf("case %d want truncated/corrupt GODISFN1 msg, got %s", i, msg)
		}
	}
}

func TestGodisFN1RejectNonGodisBinary(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	// Fake Redis-ish / opaque binary (NUL + high bytes) — must not silently OK.
	fake := []byte{0x52, 0x45, 0x44, 0x49, 0x53, 0x00, 0xff, 0xfe, 0x01, 0x02}
	r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(fake), "FLUSH"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("want reject non-GODISFN1 binary, got %s", r.ToBytes())
	}
	msg := string(r.ToBytes())
	if !strings.Contains(msg, "GODISFN1") || !strings.Contains(msg, "not Redis official") {
		t.Fatalf("want clear non-interop ERR, got %s", msg)
	}
}

func TestGodisFN1LegacyTextStillWorks(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	code := "#!lua name=legacylib\nredis.register_function('lf', function(keys, args) return 1 end)"
	r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", code, "FLUSH"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("legacy text RESTORE: %s", r.ToBytes())
	}
	list := db.Exec(nil, utils.ToCmdLine("FUNCTION", "LIST"))
	if !strings.Contains(string(list.ToBytes()), "legacylib") {
		t.Fatalf("legacy lib missing: %s", list.ToBytes())
	}
}

func TestGodisFN1BadPolicyERR(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	payload := append([]byte("GODISFN1"), 0, 0, 0, 0)
	r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(payload), "MERGE"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "FLUSH") {
		t.Fatalf("want policy ERR, got %s", r.ToBytes())
	}
}

func TestGodisFN1TrailingGarbageERR(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	code := "#!lua name=trailib\nredis.register_function('tf', function(keys, args) return 1 end)"
	if protocol.IsErrorReply(db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", code))) {
		t.Fatal("LOAD")
	}
	dump := db.Exec(nil, utils.ToCmdLine("FUNCTION", "DUMP"))
	bulk := dump.(*protocol.BulkReply)
	bad := append(append([]byte{}, bulk.Arg...), 0x00, 0x01)
	r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(bad), "FLUSH"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("trailing garbage should ERR, got %s", r.ToBytes())
	}
}
