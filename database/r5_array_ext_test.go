package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

func r5ArrayInts(t *testing.T, r redisReply, want ...int64) {
	t.Helper()
	raw, ok := r.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("want multi raw reply, got %T %s", r, r.ToBytes())
	}
	if len(raw.Replies) != len(want) {
		t.Fatalf("want %d replies, got %d: %s", len(want), len(raw.Replies), r.ToBytes())
	}
	for i, w := range want {
		ir, ok := raw.Replies[i].(*protocol.IntReply)
		if !ok || ir.Code != w {
			t.Fatalf("reply[%d]=%T, want %d: %s", i, raw.Replies[i], w, r.ToBytes())
		}
	}
}

func r5ArrayBulks(t *testing.T, r redisReply, want ...string) {
	t.Helper()
	raw, ok := r.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("want multi raw reply, got %T %s", r, r.ToBytes())
	}
	if len(raw.Replies) != len(want) {
		t.Fatalf("want %d replies, got %d: %s", len(want), len(raw.Replies), r.ToBytes())
	}
	for i, w := range want {
		br, ok := raw.Replies[i].(*protocol.BulkReply)
		if !ok || string(br.Arg) != w {
			t.Fatalf("reply[%d]=%T, want %q: %s", i, raw.Replies[i], w, r.ToBytes())
		}
	}
}

func TestR5ArrayInsertNextSeekInfo(t *testing.T) {
	db := makeTestDB()
	if r := db.Exec(nil, utils.ToCmdLine("ARINSERT", "myarray", "a")); protocol.IsErrorReply(r) {
		t.Fatalf("ARINSERT: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("ARINSERT", "myarray", "b")); protocol.IsErrorReply(r) {
		t.Fatalf("ARINSERT: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("ARNEXT", "myarray")); protocol.IsErrorReply(r) {
		t.Fatalf("ARNEXT: %s", r.ToBytes())
	} else if ir, ok := r.(*protocol.IntReply); !ok || ir.Code != 2 {
		t.Fatalf("ARNEXT want 2, got %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("ARSEEK", "myarray", "10")); protocol.IsErrorReply(r) {
		t.Fatalf("ARSEEK: %s", r.ToBytes())
	} else if ir, ok := r.(*protocol.IntReply); !ok || ir.Code != 1 {
		t.Fatalf("ARSEEK want 1, got %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("ARINSERT", "myarray", "c")); protocol.IsErrorReply(r) {
		t.Fatalf("ARINSERT: %s", r.ToBytes())
	} else if ir, ok := r.(*protocol.IntReply); !ok || ir.Code != 10 {
		t.Fatalf("ARINSERT want 10, got %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("ARNEXT", "myarray")); protocol.IsErrorReply(r) {
		t.Fatalf("ARNEXT: %s", r.ToBytes())
	} else if ir, ok := r.(*protocol.IntReply); !ok || ir.Code != 11 {
		t.Fatalf("ARNEXT want 11, got %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("ARGET", "myarray", "10")); protocol.IsErrorReply(r) {
		t.Fatalf("ARGET: %s", r.ToBytes())
	} else if br, ok := r.(*protocol.BulkReply); !ok || string(br.Arg) != "c" {
		t.Fatalf("ARGET want c, got %s", r.ToBytes())
	}

	// ARNEXT on a missing key is 0; ARSEEK on a missing key is 0.
	if r := db.Exec(nil, utils.ToCmdLine("ARNEXT", "nokey")); protocol.IsErrorReply(r) {
		t.Fatalf("ARNEXT missing: %s", r.ToBytes())
	} else if ir, ok := r.(*protocol.IntReply); !ok || ir.Code != 0 {
		t.Fatalf("ARNEXT missing want 0, got %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("ARSEEK", "nokey", "3")); protocol.IsErrorReply(r) {
		t.Fatalf("ARSEEK missing: %s", r.ToBytes())
	} else if ir, ok := r.(*protocol.IntReply); !ok || ir.Code != 0 {
		t.Fatalf("ARSEEK missing want 0, got %s", r.ToBytes())
	}

	db.Exec(nil, utils.ToCmdLine("DEL", "myarray"))
	if r := db.Exec(nil, utils.ToCmdLine("ARMSET", "myarray", "0", "a", "1", "b", "100", "c")); protocol.IsErrorReply(r) {
		t.Fatalf("ARMSET: %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("ARINSERT", "myarray", "d")); protocol.IsErrorReply(r) {
		t.Fatalf("ARINSERT: %s", r.ToBytes())
	}
	r := db.Exec(nil, utils.ToCmdLine("ARINFO", "myarray"))
	m, ok := r.(*protocol.MapReply)
	if !ok {
		t.Fatalf("ARINFO type %T: %s", r, r.ToBytes())
	}
	want := map[string]int64{
		"count": 3, "len": 101, "next-insert-index": 1,
		"slices": 1, "directory-size": 1, "super-dir-entries": 0, "slice-size": 4096,
	}
	for k, v := range want {
		ir, ok := m.Data[k].(*protocol.IntReply)
		if !ok || ir.Code != v {
			t.Fatalf("ARINFO %s want %d, got %v", k, v, m.Data[k])
		}
	}
	if r := db.Exec(nil, utils.ToCmdLine("ARINFO", "nokey")); protocol.IsErrorReply(r) {
		t.Fatalf("ARINFO nokey: %s", r.ToBytes())
	} else if _, ok := r.(*protocol.EmptyMultiBulkReply); !ok {
		t.Fatalf("ARINFO nokey want empty array, got %T", r)
	}
	if r := db.Exec(nil, utils.ToCmdLine("ARINFO", "myarray", "FULL")); protocol.IsErrorReply(r) {
		t.Fatalf("ARINFO FULL: %s", r.ToBytes())
	} else if _, ok := r.(*protocol.MapReply); !ok {
		t.Fatalf("ARINFO FULL want map, got %T", r)
	}
}

func TestR5ArrayLastItems(t *testing.T) {
	db := makeTestDB()
	for _, v := range []string{"first", "second", "third"} {
		if r := db.Exec(nil, utils.ToCmdLine("ARINSERT", "log", v)); protocol.IsErrorReply(r) {
			t.Fatalf("ARINSERT: %s", r.ToBytes())
		}
	}
	r5ArrayBulks(t, db.Exec(nil, utils.ToCmdLine("ARLASTITEMS", "log", "2")), "second", "third")
	r5ArrayBulks(t, db.Exec(nil, utils.ToCmdLine("ARLASTITEMS", "log", "2", "REV")), "third", "second")
	// Overwriting an index makes it the most recent item.
	db.Exec(nil, utils.ToCmdLine("ARSET", "log", "0", "first-updated"))
	r5ArrayBulks(t, db.Exec(nil, utils.ToCmdLine("ARLASTITEMS", "log", "1")), "first-updated")
}

func intReplyValue(t *testing.T, r redisReply) int64 {
	t.Helper()
	ir, ok := r.(*protocol.IntReply)
	if !ok {
		t.Fatalf("want int reply, got %T %s", r, r.ToBytes())
	}
	return ir.Code
}

func TestR5ArrayAggregate(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("ARMSET", "a", "0", "10", "1", "20", "2", "30"))
	if r := db.Exec(nil, utils.ToCmdLine("AROP", "a", "0", "2", "SUM")); protocol.IsErrorReply(r) {
		t.Fatalf("SUM: %s", r.ToBytes())
	} else if br, ok := r.(*protocol.BulkReply); !ok || string(br.Arg) != "60" {
		t.Fatalf("SUM want 60, got %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("AROP", "a", "0", "2", "MIN")); protocol.IsErrorReply(r) {
		t.Fatalf("MIN: %s", r.ToBytes())
	} else if br, ok := r.(*protocol.BulkReply); !ok || string(br.Arg) != "10" {
		t.Fatalf("MIN want 10, got %s", r.ToBytes())
	}
	if r := db.Exec(nil, utils.ToCmdLine("AROP", "a", "0", "2", "MAX")); protocol.IsErrorReply(r) {
		t.Fatalf("MAX: %s", r.ToBytes())
	} else if br, ok := r.(*protocol.BulkReply); !ok || string(br.Arg) != "30" {
		t.Fatalf("MAX want 30, got %s", r.ToBytes())
	}
	if got := intReplyValue(t, db.Exec(nil, utils.ToCmdLine("AROP", "a", "0", "2", "MATCH", "10"))); got != 1 {
		t.Fatalf("MATCH want 1, got %d", got)
	}
	if got := intReplyValue(t, db.Exec(nil, utils.ToCmdLine("AROP", "a", "0", "2", "USED"))); got != 3 {
		t.Fatalf("USED want 3, got %d", got)
	}
	db.Exec(nil, utils.ToCmdLine("ARMSET", "flags", "0", "255", "1", "15", "2", "240"))
	if got := intReplyValue(t, db.Exec(nil, utils.ToCmdLine("AROP", "flags", "0", "2", "AND"))); got != 0 {
		t.Fatalf("AND want 0, got %d", got)
	}
	if got := intReplyValue(t, db.Exec(nil, utils.ToCmdLine("AROP", "flags", "0", "2", "OR"))); got != 255 {
		t.Fatalf("OR want 255, got %d", got)
	}
	// Reversed bounds scan lower-to-higher by definition.
	if r := db.Exec(nil, utils.ToCmdLine("AROP", "a", "2", "0", "SUM")); protocol.IsErrorReply(r) {
		t.Fatalf("SUM reversed: %s", r.ToBytes())
	} else if br, ok := r.(*protocol.BulkReply); !ok || string(br.Arg) != "60" {
		t.Fatalf("SUM reversed want 60, got %s", r.ToBytes())
	}
	// No numeric elements → nil.
	db.Exec(nil, utils.ToCmdLine("ARSET", "s", "0", "x"))
	if r := db.Exec(nil, utils.ToCmdLine("AROP", "s", "0", "0", "SUM")); !protocol.IsErrorReply(r) {
		if _, ok := r.(*protocol.NullBulkReply); !ok {
			t.Fatalf("SUM non-numeric want nil, got %T %s", r, r.ToBytes())
		}
	}
}

func TestR5ArrayGrep(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("ARMSET", "log",
		"0", "boot: ok", "1", "warn: disk", "2", "ERROR: cpu", "3", "info: ready", "4", "error: net"))

	r5ArrayInts(t, db.Exec(nil, utils.ToCmdLine("ARGREP", "log", "-", "+", "MATCH", "error", "NOCASE")), 2, 4)
	r5ArrayInts(t, db.Exec(nil, utils.ToCmdLine("ARGREP", "log", "0", "4", "GLOB", "warn:*", "OR", "GLOB", "error:*")), 1, 4)
	r5ArrayInts(t, db.Exec(nil, utils.ToCmdLine("ARGREP", "log", "0", "4", "EXACT", "info: ready")), 3)
	r5ArrayInts(t, db.Exec(nil, utils.ToCmdLine("ARGREP", "log", "0", "4", "MATCH", "error", "AND", "MATCH", "cpu", "NOCASE")), 2)
	r5ArrayInts(t, db.Exec(nil, utils.ToCmdLine("ARGREP", "log", "-", "+", "MATCH", "error", "NOCASE", "LIMIT", "1")), 2)

	// WITHVALUES: [[idx, value], ...]
	r := db.Exec(nil, utils.ToCmdLine("ARGREP", "log", "0", "4", "RE", "^[A-Za-z]+: (cpu|net)$", "NOCASE", "WITHVALUES"))
	raw, ok := r.(*protocol.MultiRawReply)
	if !ok || len(raw.Replies) != 2 {
		t.Fatalf("WITHVALUES: %T %s", r, r.ToBytes())
	}
	for i, want := range []struct {
		idx int64
		val string
	}{{2, "ERROR: cpu"}, {4, "error: net"}} {
		pair, ok := raw.Replies[i].(*protocol.MultiRawReply)
		if !ok || len(pair.Replies) != 2 {
			t.Fatalf("pair[%d]: %T %s", i, raw.Replies[i], r.ToBytes())
		}
		ir, _ := pair.Replies[0].(*protocol.IntReply)
		br, _ := pair.Replies[1].(*protocol.BulkReply)
		if ir == nil || ir.Code != want.idx || br == nil || string(br.Arg) != want.val {
			t.Fatalf("pair[%d] want [%d %q], got %s", i, want.idx, want.val, pair.ToBytes())
		}
	}

	// Reverse traversal order is descending index.
	r5ArrayInts(t, db.Exec(nil, utils.ToCmdLine("ARGREP", "log", "4", "0", "MATCH", ":")), 4, 3, 2, 1, 0)
	// Missing key returns an empty array.
	r = db.Exec(nil, utils.ToCmdLine("ARGREP", "nokey", "-", "+", "EXACT", "x"))
	if _, ok := r.(*protocol.EmptyMultiBulkReply); !ok {
		t.Fatalf("ARGREP missing want empty, got %T", r)
	}
}

func TestR5ArrayRing(t *testing.T) {
	db := makeTestDB()
	for i, v := range []string{"v0", "v1", "v2", "v3"} {
		r := db.Exec(nil, utils.ToCmdLine("ARRING", "readings", "3", v))
		if protocol.IsErrorReply(r) {
			t.Fatalf("ARRING: %s", r.ToBytes())
		}
		ir, ok := r.(*protocol.IntReply)
		if !ok || ir.Code != int64(i%3) {
			t.Fatalf("ARRING %s want %d, got %s", v, i%3, r.ToBytes())
		}
	}
	if r := db.Exec(nil, utils.ToCmdLine("ARGET", "readings", "0")); protocol.IsErrorReply(r) {
		t.Fatalf("ARGET: %s", r.ToBytes())
	} else if br, ok := r.(*protocol.BulkReply); !ok || string(br.Arg) != "v3" {
		t.Fatalf("ARGET 0 want v3, got %s", r.ToBytes())
	}
	if got := intReplyValue(t, db.Exec(nil, utils.ToCmdLine("ARCOUNT", "readings"))); got != 3 {
		t.Fatalf("ARCOUNT want 3, got %d", got)
	}
	r5ArrayBulks(t, db.Exec(nil, utils.ToCmdLine("ARLASTITEMS", "readings", "3")), "v1", "v2", "v3")
}

func TestR5ArrayExtendedDumpRestore(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("ARMSET", "a", "0", "x", "1", "y"))
	db.Exec(nil, utils.ToCmdLine("ARSEEK", "a", "7"))
	db.Exec(nil, utils.ToCmdLine("ARINSERT", "a", "z"))
	dump := db.Exec(nil, utils.ToCmdLine("DUMP", "a"))
	payload, ok := dump.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("DUMP: %T %s", dump, dump.ToBytes())
	}
	db2 := makeTestDB()
	if r := db2.Exec(nil, utils.ToCmdLine("RESTORE", "a", "0", string(payload.Arg))); protocol.IsErrorReply(r) {
		t.Fatalf("RESTORE: %s", r.ToBytes())
	}
	if got := intReplyValue(t, db2.Exec(nil, utils.ToCmdLine("ARNEXT", "a"))); got != 8 {
		t.Fatalf("restored ARNEXT want 8, got %d", got)
	}
	r5ArrayBulks(t, db2.Exec(nil, utils.ToCmdLine("ARLASTITEMS", "a", "3")), "x", "y", "z")
}
