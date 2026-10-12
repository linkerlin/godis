package database

// R5 第三波（首批）：HOTKEYS（8.6）与 HIMPORT（8.10）。Array/JSONPath 见 TODO。

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestR5W3Hotkeys(t *testing.T) {
	defer testDB.Exec(nil, utils.ToCmdLine("HOTKEYS", "RESET"))

	// Before any session: null reply.
	asserts.AssertNullBulk(t, testDB.Exec(nil, utils.ToCmdLine("HOTKEYS", "GET")))

	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"HOTKEYS", "START", "METRICS", "1", "CPU")), "OK")
	// Double start is rejected.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"HOTKEYS", "START", "METRICS", "1", "CPU")),
		"ERR Hotkeys tracking is already active")

	// Generate load: the recorded keys must appear in by-cpu-time-us.
	for i := 0; i < 20; i++ {
		testDB.Exec(nil, utils.ToCmdLine("SET", "r5w3:hot:a", "v"))
		testDB.Exec(nil, utils.ToCmdLine("GET", "r5w3:hot:a"))
	}
	r := testDB.Exec(nil, utils.ToCmdLine("HOTKEYS", "GET"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("HOTKEYS GET: %s", r.ToBytes())
	}
	b := string(r.ToBytes())
	if !strings.Contains(b, "by-cpu-time-us") || !strings.Contains(b, "r5w3:hot:a") {
		t.Errorf("GET missing cpu section/key: %q", b)
	}
	if strings.Contains(b, "by-net-bytes") {
		t.Errorf("NET section must be absent when only CPU tracked: %q", b)
	}

	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("HOTKEYS", "STOP")), "OK")
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("HOTKEYS", "STOP")),
		"ERR Hotkeys tracking is not active")
	// GET after STOP still reports (tracking-active 0).
	r = testDB.Exec(nil, utils.ToCmdLine("HOTKEYS", "GET"))
	if !strings.Contains(string(r.ToBytes()), "tracking-active") {
		t.Errorf("GET after STOP should report metadata: %q", r.ToBytes())
	}

	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine("HOTKEYS", "RESET")), "OK")
	asserts.AssertNullBulk(t, testDB.Exec(nil, utils.ToCmdLine("HOTKEYS", "GET")))

	// Validation paths.
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine("HOTKEYS", "START", "METRICS", "1")),
		"ERR At least one of CPU or NET must be specified")
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"HOTKEYS", "START", "METRICS", "2", "CPU")),
		"ERR METRICS count must match the number of selected metrics")
	asserts.AssertErrReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"HOTKEYS", "START", "METRICS", "1", "CPU", "SLOTS", "1", "0")),
		"ERR SLOTS is supported only in cluster mode")
	testDB.Exec(nil, utils.ToCmdLine("HOTKEYS", "RESET"))

	// Both metrics.
	asserts.AssertStatusReply(t, testDB.Exec(nil, utils.ToCmdLine(
		"HOTKEYS", "START", "METRICS", "2", "CPU", "NET", "COUNT", "3")), "OK")
	testDB.Exec(nil, utils.ToCmdLine("SET", "r5w3:hot:b", "v"))
	r = testDB.Exec(nil, utils.ToCmdLine("HOTKEYS", "GET"))
	b = string(r.ToBytes())
	if !strings.Contains(b, "by-net-bytes") || !strings.Contains(b, "r5w3:hot:b") {
		t.Errorf("GET missing net section/key: %q", b)
	}
	testDB.Exec(nil, utils.ToCmdLine("HOTKEYS", "RESET"))
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w3:hot:a", "r5w3:hot:b"))
}

func TestR5W3HImport(t *testing.T) {
	c := connection.NewFakeConn()
	defer ClearHImportSession(c.GetClientID())
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w3:u1", "r5w3:u2"))

	asserts.AssertStatusReply(t, testDB.Exec(c, utils.ToCmdLine(
		"HIMPORT", "PREPARE", "u", "name", "email", "age")), "OK")
	// Duplicate field rejected.
	asserts.AssertErrReply(t, testDB.Exec(c, utils.ToCmdLine(
		"HIMPORT", "PREPARE", "bad", "a", "a")),
		"ERR Field names must be unique within a fieldset")

	asserts.AssertStatusReply(t, testDB.Exec(c, utils.ToCmdLine(
		"HIMPORT", "SET", "r5w3:u1", "u", "alice", "a@example.com", "30")), "OK")
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("HGET", "r5w3:u1", "name")), "alice")
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("HGET", "r5w3:u1", "age")), "30")
	asserts.AssertIntReply(t, testDB.Exec(nil, utils.ToCmdLine("HLEN", "r5w3:u1")), 3)

	// Overwrite via a second SET.
	asserts.AssertStatusReply(t, testDB.Exec(c, utils.ToCmdLine(
		"HIMPORT", "SET", "r5w3:u1", "u", "bob", "b@example.com", "25")), "OK")
	asserts.AssertBulkReply(t, testDB.Exec(nil, utils.ToCmdLine("HGET", "r5w3:u1", "name")), "bob")

	// Value count mismatch and unknown fieldset.
	asserts.AssertErrReply(t, testDB.Exec(c, utils.ToCmdLine(
		"HIMPORT", "SET", "r5w3:u2", "u", "x")),
		"ERR Value count does not match the fieldset")
	asserts.AssertErrReply(t, testDB.Exec(c, utils.ToCmdLine(
		"HIMPORT", "SET", "r5w3:u2", "nosuch", "x")),
		"ERR No such fieldset")

	// Session scoping: another connection has no fieldset.
	c2 := connection.NewFakeConn()
	defer ClearHImportSession(c2.GetClientID())
	asserts.AssertErrReply(t, testDB.Exec(c2, utils.ToCmdLine(
		"HIMPORT", "SET", "r5w3:u2", "u", "x", "y", "z")),
		"ERR No such fieldset")

	// DISCARD removes one fieldset; DISCARDALL clears everything.
	asserts.AssertStatusReply(t, testDB.Exec(c, utils.ToCmdLine("HIMPORT", "DISCARD", "u")), "OK")
	asserts.AssertErrReply(t, testDB.Exec(c, utils.ToCmdLine(
		"HIMPORT", "SET", "r5w3:u2", "u", "x", "y", "z")),
		"ERR No such fieldset")
	testDB.Exec(c, utils.ToCmdLine("HIMPORT", "PREPARE", "v", "f"))
	asserts.AssertStatusReply(t, testDB.Exec(c, utils.ToCmdLine("HIMPORT", "DISCARDALL")), "OK")
	asserts.AssertErrReply(t, testDB.Exec(c, utils.ToCmdLine(
		"HIMPORT", "SET", "r5w3:u2", "v", "x")),
		"ERR No such fieldset")

	// Unknown subcommand.
	asserts.AssertErrReply(t, testDB.Exec(c, utils.ToCmdLine("HIMPORT", "NOPE")),
		"ERR Unknown subcommand or wrong number of arguments for 'nope'. Try HIMPORT HELP.")
	testDB.Exec(nil, utils.ToCmdLine("DEL", "r5w3:u1", "r5w3:u2"))
}
