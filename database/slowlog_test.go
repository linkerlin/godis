package database

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestSlowLogger_Record(t *testing.T) {
	logger := NewSlowLogger(5, 1000)

	start := time.Now().Add(-time.Minute)
	for i := 0; i < 10; i++ {
		logger.Record(start, utils.ToCmdLine("GET", strconv.Itoa(i)), "127.0.0.1:12345", "")
	}

	if logger.Len() != 5 {
		t.Errorf("Expected 1 entry, got %d", logger.Len())
	}
	for i, e := range logger.entries {
		actual := string(e.Command[1])
		expect := strconv.Itoa(i + 5)
		if actual != expect {
			t.Errorf("Expected %s, got %s", expect, actual)
		}
	}

	logger2 := NewSlowLogger(5, 1000)
	start = time.Now()
	logger2.Record(start, utils.ToCmdLine("SET", "key2"), "127.0.0.1:12346", "")
	if logger2.Len() != 0 {
		t.Errorf("Below threshold query should not be recorded, got %d entries", logger.Len())
	}
}

func TestSlowLogger_GetEntries(t *testing.T) {
	logger := NewSlowLogger(10, 1000) // 10条最大，1ms阈值

	for i := 0; i < 5; i++ {
		start := time.Now()
		time.Sleep(2 * time.Millisecond)
		logger.Record(start, utils.ToCmdLine("CMD", strconv.Itoa(i)), "client", "")
	}

	entries := logger.GetEntries(10)
	if len(entries) != 5 {
		t.Errorf("Expected 5 entries, got %d", len(entries))
	}

	entries = logger.GetEntries(3)
	if len(entries) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(entries))
	}

	// Verify the order of entries (should be the latest first)
	if string(entries[0].Command[1]) != "4" {
		t.Errorf("Expected newest entry first, got %s", entries[0].Command[1])
	}
	if string(entries[2].Command[1]) != "2" {
		t.Errorf("Expected oldest of the three to be 2, got %s", entries[2].Command[1])
	}

	entries = logger.GetEntries(0)
	if len(entries) != 0 {
		t.Errorf("Expected 0 entries when requesting 0, got %d", len(entries))
	}

}

func TestSlowLogger_Len(t *testing.T) {
	logger := NewSlowLogger(10, 1000)
	if logger.Len() != 0 {
		t.Errorf("New logger should have 0 entries, got %d", logger.Len())
	}

	for i := 0; i < 3; i++ {
		start := time.Now()
		time.Sleep(2 * time.Millisecond)
		logger.Record(start, utils.ToCmdLine("LEN_CMD", strconv.Itoa(i)), "client", "")
	}

	if logger.Len() != 3 {
		t.Errorf("Expected 3 entries, got %d", logger.Len())
	}
}

func TestSlowLogger_Reset(t *testing.T) {
	logger := NewSlowLogger(10, 1000)

	for i := 0; i < 5; i++ {
		start := time.Now()
		time.Sleep(2 * time.Millisecond)
		logger.Record(start, utils.ToCmdLine("RESET_CMD", strconv.Itoa(i)), "client", "")
	}

	// Before resetting
	if logger.Len() != 5 {
		t.Errorf("Pre-reset should have 5 entries, got %d", logger.Len())
	}

	logger.Reset()

	// After resetting
	if logger.Len() != 0 {
		t.Errorf("Post-reset should have 0 entries, got %d", logger.Len())
	}

	// Verify ID reset
	start := time.Now()
	time.Sleep(2 * time.Millisecond)
	logger.Record(start, utils.ToCmdLine("NEW_CMD", "after reset"), "client", "")
	if logger.GetEntries(1)[0].ID != 1 {
		t.Errorf("After reset, ID should start from 1, got %d", logger.GetEntries(1)[0].ID)
	}
}

func TestSlowLogger_MaxEntries(t *testing.T) {
	logger := NewSlowLogger(3, 1000)

	for i := 0; i < 5; i++ {
		start := time.Now()
		time.Sleep(2 * time.Millisecond)
		logger.Record(start, utils.ToCmdLine("MAX_CMD", strconv.Itoa(i)), "client", "")
	}

	if logger.Len() != 3 {
		t.Errorf("Expected max 3 entries, got %d", logger.Len())
	}

	entries := logger.GetEntries(5)
	if len(entries) != 3 {
		t.Fatalf("Expected 3 entries, got %d", len(entries))
	}

	expected := []string{"4", "3", "2"}
	for i, entry := range entries {
		if string(entry.Command[1]) != expected[i] {
			t.Errorf("Position %d: expected %s, got %s", i, expected[i], entry.Command[1])
		}
	}
}

func TestSlowLogger_EntryLimits(t *testing.T) {
	logger := NewSlowLogger(10, 0) // record everything
	logger.SetEntryMaxArgc(3)
	logger.SetEntryMaxString(5)

	// Expectations pinned to Redis 8.10 output (docker redis:8.10-alpine):
	// SET k abcdefghijklmno (15 bytes) → "SET","k","abcde... (10 more bytes)"
	logger.Record(time.Now(), utils.ToCmdLine("SET", "k", "abcdefghijklmno"), "127.0.0.1:1", "")
	// MSET a 1 b 2 c 3 d 4 e 5 (11 args) → "MSET","a","... (9 more arguments)"
	logger.Record(time.Now(), utils.ToCmdLine("MSET", "a", "1", "b", "2", "c", "3", "d", "4", "e", "5"), "127.0.0.1:1", "")

	entries := logger.GetEntries(2)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	mset := entries[0] // newest first
	if len(mset.Command) != 3 ||
		string(mset.Command[0]) != "MSET" || string(mset.Command[1]) != "a" ||
		string(mset.Command[2]) != "... (9 more arguments)" {
		t.Errorf("MSET truncated args: %q", mset.Command)
	}
	if mset.Argc != 11 {
		t.Errorf("MSET argc = %d, want 11", mset.Argc)
	}
	set := entries[1]
	if len(set.Command) != 3 || string(set.Command[2]) != "abcde... (10 more bytes)" {
		t.Errorf("SET truncated value: %q", set.Command)
	}
	if set.Argc != 3 {
		t.Errorf("SET argc = %d, want 3", set.Argc)
	}
}

func TestConfigSetSlowlogEntryLimits(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	oldArgc := config.Properties.SlowLogEntryMaxArgc
	oldStr := config.Properties.SlowLogEntryMaxStringLen
	oldSlower := config.Properties.SlowLogSlowerThan
	defer func() {
		server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slowlog-entry-max-argc", strconv.Itoa(oldArgc)))
		server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slowlog-entry-max-string-len", strconv.FormatInt(oldStr, 10)))
		server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slowlog-log-slower-than", strconv.FormatInt(oldSlower, 10)))
	}()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slowlog-entry-max-argc", "3")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slowlog-entry-max-string-len", "5")), "OK")
	ret := server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "slowlog-entry-max-argc", "slowlog-entry-max-string-len"))
	got := string(ret.ToBytes())
	if !strings.Contains(got, "slowlog-entry-max-argc\r\n$1\r\n3") ||
		!strings.Contains(got, "slowlog-entry-max-string-len\r\n$1\r\n5") {
		t.Fatalf("CONFIG GET: %q", got)
	}

	// Hot effect on recorded entries.
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slowlog-log-slower-than", "0")), "OK")
	server.Exec(c, utils.ToCmdLine("SET", "sl-trunc", "abcdefghijklmno"))
	slr := server.Exec(c, utils.ToCmdLine("SLOWLOG", "GET", "1"))
	mr, ok := slr.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("SLOWLOG GET 1: %s", slr.ToBytes())
	}
	fields, ok := mr.Replies[0].(*protocol.MultiRawReply)
	if !ok || len(fields.Replies) != 7 {
		t.Fatalf("entry fields: %s", mr.Replies[0].ToBytes())
	}
	cmds, ok := fields.Replies[3].(*protocol.MultiBulkReply)
	if !ok || len(cmds.Args) != 3 || string(cmds.Args[2]) != "abcde... (10 more bytes)" {
		t.Fatalf("truncated entry: %s", fields.Replies[3].ToBytes())
	}

	// Range validation pinned to Redis 8.10 messages.
	bad := [][2]string{
		{"slowlog-entry-max-argc", "1"},
		{"slowlog-entry-max-argc", "0"},
		{"slowlog-entry-max-argc", "-1"},
		{"slowlog-entry-max-string-len", "0"},
		{"slowlog-entry-max-string-len", "-5"},
	}
	for _, tc := range bad {
		r := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", tc[0], tc[1]))
		if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "argument must be between") {
			t.Errorf("CONFIG SET %s %s: want range error, got %s", tc[0], tc[1], r.ToBytes())
		}
	}
}

func TestSlowLogger_HandleSlowlogCommand(t *testing.T) {
	logger := NewSlowLogger(10, 1000)

	for i := 0; i < 5; i++ {
		start := time.Now()
		time.Sleep(2 * time.Millisecond)
		logger.Record(start, utils.ToCmdLine("CMD", strconv.Itoa(i)), "client", "")
	}

	// get
	t.Run("GET_Subcommand", func(t *testing.T) {
		reply := logger.HandleSlowlogCommand(utils.ToCmdLine("SLOWLOG", "GET", "3"))

		multiReply, ok := reply.(*protocol.MultiRawReply)
		if !ok {
			t.Fatalf("Expected MultiRawReply, got %T", reply)
		}

		if len(multiReply.Replies) != 3 {
			t.Errorf("Expected 3 entries, got %d", len(multiReply.Replies))
		}

		for i, entryReply := range multiReply.Replies {
			entryMulti, ok := entryReply.(*protocol.MultiRawReply)
			if !ok {
				t.Fatalf("Entry %d: Expected MultiRawReply, got %T", i, entryReply)
			}

			if len(entryMulti.Replies) != 7 {
				t.Errorf("Entry %d: Expected 7 fields, got %d", i, len(entryMulti.Replies))
			}

			// Verify that the first field is ID
			idReply, ok := entryMulti.Replies[0].(*protocol.IntReply)
			if !ok {
				t.Errorf("Entry %d: ID field should be IntReply", i)
			}
			if idReply.Code <= 0 {
				t.Errorf("Entry %d: Invalid ID %d", i, idReply.Code)
			}
		}
	})

	// len
	t.Run("LEN_Subcommand", func(t *testing.T) {
		reply := logger.HandleSlowlogCommand(utils.ToCmdLine("SLOWLOG", "LEN"))

		intReply, ok := reply.(*protocol.IntReply)
		if !ok {
			t.Fatalf("Expected IntReply, got %T", reply)
		}

		if intReply.Code != 5 {
			t.Errorf("Expected 5 entries, got %d", intReply.Code)
		}
	})

	// reset
	t.Run("RESET_Subcommand", func(t *testing.T) {
		reply := logger.HandleSlowlogCommand(utils.ToCmdLine("SLOWLOG", "RESET"))

		_, ok := reply.(*protocol.OkReply)
		if !ok {
			t.Fatalf("Expected OkReply, got %T", reply)
		}

		// Verify if the log has been reset
		if logger.Len() != 0 {
			t.Errorf("After reset, expected 0 entries, got %d", logger.Len())
		}
	})

	// Invalid sub command
	t.Run("Invalid_Subcommand", func(t *testing.T) {
		reply := logger.HandleSlowlogCommand(utils.ToCmdLine("SLOWLOG", "INVALID"))

		errReply, ok := reply.(*protocol.StandardErrReply)
		if !ok {
			t.Fatalf("Expected StandardErrReply, got %T", reply)
		}

		expectedErr := "ERR Unknown subcommand or wrong number of arguments for 'invalid'"
		if !strings.Contains(errReply.Error(), expectedErr) {
			t.Errorf("Expected error '%s', got '%s'", expectedErr, errReply.Error())
		}
	})

	// The number of test parameters is incorrect
	t.Run("Invalid_Argument_Count", func(t *testing.T) {
		reply := logger.HandleSlowlogCommand(utils.ToCmdLine("SLOWLOG"))

		errReply, ok := reply.(*protocol.StandardErrReply)
		if !ok {
			t.Fatalf("Expected StandardErrReply, got %T", reply)
		}

		expectedErr := "ERR wrong number of arguments for 'SLOWLOG' command"
		if !strings.Contains(errReply.Error(), expectedErr) {
			t.Errorf("Expected error '%s', got '%s'", expectedErr, errReply.Error())
		}
	})
}
