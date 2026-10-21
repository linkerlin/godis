package database

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestFunctionDumpRDBStringEncodings(t *testing.T) {
	values := [][]byte{
		[]byte("short"),
		bytes.Repeat([]byte("x"), 100),
		bytes.Repeat([]byte("y"), 20000),
	}
	for _, want := range values {
		encoded := appendRDBString(nil, want)
		got, next, err := readRDBString(encoded, 0)
		if err != nil {
			t.Fatalf("len %d: %v", len(want), err)
		}
		if next != len(encoded) || !bytes.Equal(got, want) {
			t.Fatalf("len %d: next=%d encoded=%d equal=%v", len(want), next, len(encoded), bytes.Equal(got, want))
		}
	}
}

func TestFunctionDumpEmptyRoundTrip(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("FUNCTION", "FLUSH"))

	dump := db.Exec(nil, utils.ToCmdLine("FUNCTION", "DUMP"))
	bulk, ok := dump.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) != 10 {
		t.Fatalf("empty DUMP want 10-byte footer, got %T %s", dump, dump.ToBytes())
	}
	if ver := binary.LittleEndian.Uint16(bulk.Arg[:2]); ver != dumpRDBVersion {
		t.Fatalf("empty DUMP version=%d want %d", ver, dumpRDBVersion)
	}
	r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(bulk.Arg), "FLUSH"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("RESTORE empty official dump: %s", r.ToBytes())
	}
	list := db.Exec(nil, utils.ToCmdLine("FUNCTION", "LIST"))
	if got := string(list.ToBytes()); strings.Contains(got, "library_name") {
		t.Fatalf("empty dump restored libraries: %s", got)
	}
}

func TestFunctionDumpOfficialRoundTrip(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("FUNCTION", "FLUSH"))

	libA := "#!lua name=rdbdumpa\nredis.register_function('rda', function(keys, args) return 'A' end)"
	libB := "#!lua name=rdbdumpb\nredis.register_function('rdb', function(keys, args) return 'B' end)"
	for _, code := range []string{libA, libB} {
		if r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", code)); protocol.IsErrorReply(r) {
			t.Fatalf("LOAD: %s", r.ToBytes())
		}
	}

	dump := db.Exec(nil, utils.ToCmdLine("FUNCTION", "DUMP"))
	bulk, ok := dump.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) < 10 {
		t.Fatalf("DUMP: %T %s", dump, dump.ToBytes())
	}
	if bulk.Arg[0] != rdbOpcodeFunction2 {
		t.Fatalf("DUMP should start with RDB_OPCODE_FUNCTION2 0xF5, got 0x%02x", bulk.Arg[0])
	}

	_ = db.Exec(nil, utils.ToCmdLine("FUNCTION", "FLUSH"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(bulk.Arg), "FLUSH")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("FCALL", "rda", "0")), "A")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("FCALL", "rdb", "0")), "B")
}

// This fixture was produced by Redis 8.10.0 with:
//
//	FUNCTION LOAD "#!lua name=redislib\nredis.register_function(\"rf\", function(keys, args) return \"redis\" end)"
//
// The library code is LZF-compressed inside the payload, so it exercises the
// official decoder path rather than only our own raw-string output.
func TestFunctionDumpOfficialRedisFixture(t *testing.T) {
	const fixture = "9cNAUkBaEyMhbHVhIG5hbWU9cmVkaXNsaWIKYAgYLnJlZ2lzdGVyX2Z1bmN0aW9uKCJyZiIsIOAADhNrZXlzLCBhcmdzKSByZXR1cm4gImA6BSIgZW5kKQ8AF3f5PNVQUv8="
	payload, err := base64.StdEncoding.DecodeString(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if payload[0] != rdbOpcodeFunction2 {
		t.Fatalf("fixture opcode=0x%02x", payload[0])
	}
	libs, err := decodeFunctionDump(payload)
	if err != nil {
		t.Fatalf("decode official Redis fixture: %v", err)
	}
	if len(libs) != 1 || libs[0].Name != "redislib" || !strings.Contains(libs[0].Code, "#!lua name=redislib") {
		t.Fatalf("decoded fixture: %+v", libs)
	}

	db := makeTestDB()
	InitFunctionsEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("FUNCTION", "FLUSH"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(payload), "FLUSH")), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("FCALL", "rf", "0")), "redis")
}

func TestFunctionDumpBadFooterAndTruncation(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("FUNCTION", "FLUSH"))
	code := "#!lua name=badfoot\nredis.register_function('bf', function(keys, args) return 1 end)"
	if r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", code)); protocol.IsErrorReply(r) {
		t.Fatal("LOAD")
	}
	dump := db.Exec(nil, utils.ToCmdLine("FUNCTION", "DUMP")).(*protocol.BulkReply)

	badCRC := append([]byte{}, dump.Arg...)
	badCRC[len(badCRC)-1] ^= 0xff
	for name, payload := range map[string][]byte{
		"bad_checksum": badCRC,
		"truncated":    dump.Arg[:len(dump.Arg)-1],
		"too_short":    {0xF5, 0x00},
	} {
		r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(payload), "FLUSH"))
		if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "checksum") {
			t.Fatalf("%s: want checksum ERR, got %s", name, r.ToBytes())
		}
	}
}

func TestFunctionDumpMalformedLZF(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	body := []byte{rdbOpcodeFunction2, 0xC3, 0x01, 0x10, 0xFF}
	payload := appendDumpFooter(body)
	r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(payload), "FLUSH"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "LZF") {
		t.Fatalf("malformed LZF want ERR, got %s", r.ToBytes())
	}
}

func TestFunctionDumpPreGARejected(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	payload := appendDumpFooter([]byte{rdbOpcodeFunctionPreGA})
	r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(payload), "FLUSH"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "Pre-GA") {
		t.Fatalf("pre-GA want Pre-GA ERR, got %s", r.ToBytes())
	}
}

func TestFunctionRestoreRejectsForeignBinary(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	keep := "#!lua name=keeplib\nredis.register_function('kf', function(keys, args) return 1 end)"
	if r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", keep)); protocol.IsErrorReply(r) {
		t.Fatal("LOAD keep")
	}

	cases := [][]byte{
		{0x00, 0xff, 0xfe, 0x01, 0x02, 0xde, 0xad},
		[]byte("REDIS0011\xff\x00fake"),
	}
	for i, payload := range cases {
		r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(payload), "FLUSH"))
		if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "expects a Redis FUNCTION DUMP") {
			t.Fatalf("case %d: want foreign ERR, got %s", i, r.ToBytes())
		}
		list := db.Exec(nil, utils.ToCmdLine("FUNCTION", "LIST"))
		if !strings.Contains(string(list.ToBytes()), "keeplib") {
			t.Fatalf("case %d: rejected dump must not FLUSH existing libs: %s", i, list.ToBytes())
		}
	}
}

func TestGodisFN1LegacyRestoreStillWorks(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("FUNCTION", "FLUSH"))

	code := "#!lua name=legacylib\nredis.register_function('lf', function(keys, args) return 1 end)"
	payload := append([]byte{}, functionDumpMagic...)
	var count [4]byte
	binary.BigEndian.PutUint32(count[:], 1)
	payload = append(payload, count[:]...)
	payload = appendLengthPrefixed(payload, []byte("legacylib"))
	payload = appendLengthPrefixed(payload, []byte("LUA"))
	payload = appendLengthPrefixed(payload, []byte(code))

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(payload), "FLUSH")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("FCALL", "lf", "0")), 1)
}

func TestGodisFN1TruncatedPayloadERR(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	cases := [][]byte{
		[]byte("GODISFN1"),
		append([]byte("GODISFN1"), 0, 0, 0),
		append([]byte("GODISFN1"), 0, 0, 0, 1),
		append([]byte("GODISFN1"), 0, 0, 0, 1, 0, 0, 0, 3, 'a', 'b'),
	}
	for i, p := range cases {
		r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(p), "FLUSH"))
		if !protocol.IsErrorReply(r) {
			t.Fatalf("case %d want ERR, got %s", i, r.ToBytes())
		}
		msg := string(r.ToBytes())
		if !strings.Contains(msg, "GODISFN1") || !strings.Contains(strings.ToLower(msg), "truncated") && !strings.Contains(msg, "corrupt") {
			t.Fatalf("case %d want truncated/corrupt GODISFN1 msg, got %s", i, msg)
		}
	}
}

func TestFunctionDumpBadPolicyERR(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	dump := db.Exec(nil, utils.ToCmdLine("FUNCTION", "DUMP")).(*protocol.BulkReply)
	r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(dump.Arg), "MERGE"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "FLUSH") {
		t.Fatalf("want policy ERR, got %s", r.ToBytes())
	}
}

func TestFunctionDumpTrailingGarbageERR(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	code := "#!lua name=trailib\nredis.register_function('tf', function(keys, args) return 1 end)"
	if protocol.IsErrorReply(db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", code))) {
		t.Fatal("LOAD")
	}
	dump := db.Exec(nil, utils.ToCmdLine("FUNCTION", "DUMP")).(*protocol.BulkReply)
	bad := append(append([]byte{}, dump.Arg...), 0x00, 0x01)
	r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(bad), "FLUSH"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("trailing garbage should ERR, got %s", r.ToBytes())
	}
}
