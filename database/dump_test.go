package database

import (
	"encoding/binary"
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

// TestRestoreRejectMatrix locks the honest RESTORE reject surface: truncated,
// bad RDB version, bad CRC, Redis-module-looking bytes — all ERR (never silent OK).
// Godis does not accept official module RDB; Godis↔Godis uses GODIS1 opaque.
func TestRestoreRejectMatrix(t *testing.T) {
	db := makeTestDB()
	// Build a known-good DUMP for mutation cases.
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("SET", "ok-k", "v")), "OK")
	good := db.Exec(nil, utils.ToCmdLine("DUMP", "ok-k")).(*protocol.BulkReply).Arg
	if len(good) < 12 {
		t.Fatalf("DUMP too short: %d", len(good))
	}

	cases := []struct {
		name    string
		payload []byte
	}{
		{"empty", nil},
		{"too_short", []byte{0x00, 0x01, 0x02}},
		{"version_zero", func() []byte {
			p := append([]byte(nil), good...)
			// Footer: last 10 = version(2 LE) + crc(8). Force version 0.
			binary.LittleEndian.PutUint16(p[len(p)-10:], 0)
			return p
		}()},
		{"version_too_high", func() []byte {
			p := append([]byte(nil), good...)
			binary.LittleEndian.PutUint16(p[len(p)-10:], 99)
			return p
		}()},
		{"bad_crc", func() []byte {
			p := append([]byte(nil), good...)
			binary.LittleEndian.PutUint64(p[len(p)-8:], 0xdeadbeefcafebabe)
			return p
		}()},
		{"truncated_body", good[:len(good)-4]},
		{"module_looking", []byte("\x05REDISMOD\x00\x0b\x00\x00\x00\x00\x00\x00\x00\x00\x00")},
		{"godis1_truncated", []byte("GODIS1\x00{\"t\":\"json\"")}, // no RDB footer → reject
		{"null_heavy", []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := db.Exec(nil, utils.ToCmdLine("RESTORE", "rej-"+tc.name, "0", string(tc.payload)))
			if !protocol.IsErrorReply(r) {
				t.Fatalf("want ERR, got %s", r.ToBytes())
			}
			msg := string(r.ToBytes())
			if !strings.Contains(msg, "DUMP payload version or checksum are wrong") {
				t.Fatalf("want reject phrasing, got %s", msg)
			}
			if strings.Contains(msg, "jemalloc") || strings.Contains(strings.ToLower(msg), "module rdb accepted") {
				t.Fatalf("must not claim module/jemalloc support: %s", msg)
			}
		})
	}
}
