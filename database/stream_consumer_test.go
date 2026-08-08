package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func streamEntryID(t *testing.T, db *DB, key string) string {
	t.Helper()
	reply := db.Exec(nil, utils.ToCmdLine("XADD", key, "*", "f", "v"))
	bulk, ok := reply.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) == 0 {
		t.Fatalf("XADD %s: got %T %s", key, reply, reply.ToBytes())
	}
	return string(bulk.Arg)
}

func TestStreamConsumerGroupWorkflow(t *testing.T) {
	db := makeTestDB()
	const key = "s:cg"

	entryID := streamEntryID(t, db, key)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", key, "g1", "0-0")), "OK")

	read := db.Exec(nil, utils.ToCmdLine(
		"XREADGROUP", "GROUP", "g1", "c1", "STREAMS", key, ">",
	))
	sr, ok := read.(*StreamReadReply)
	if !ok || len(sr.buckets) == 0 || sr.buckets[0].key != key {
		t.Fatalf("XREADGROUP: got %T %s", read, read.ToBytes())
	}

	pending := db.Exec(nil, utils.ToCmdLine("XPENDING", key, "g1"))
	if _, ok := pending.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("XPENDING summary: got %s", pending.ToBytes())
	}

	detail := db.Exec(nil, utils.ToCmdLine("XPENDING", key, "g1", "0-0", entryID, "10"))
	if _, ok := detail.(*protocol.MultiRawReply); !ok {
		t.Fatalf("XPENDING detail: got %s", detail.ToBytes())
	}

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XACK", key, "g1", entryID)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XACK", key, "g1", entryID)), 0)
}

func TestStreamXReadNonBlocking(t *testing.T) {
	db := makeTestDB()
	const key = "s:read"

	streamEntryID(t, db, key)

	read := db.Exec(nil, utils.ToCmdLine("XREAD", "COUNT", "1", "STREAMS", key, "0-0"))
	sr, ok := read.(*StreamReadReply)
	if !ok || len(sr.buckets) == 0 || sr.buckets[0].key != key {
		t.Fatalf("XREAD: got %T %s", read, read.ToBytes())
	}

	empty := db.Exec(nil, utils.ToCmdLine("XREAD", "BLOCK", "1", "STREAMS", "missing", "0-0"))
	if _, ok := empty.(*protocol.NullBulkReply); !ok {
		t.Fatalf("XREAD BLOCK timeout: got %s", empty.ToBytes())
	}
}

func TestStreamXReadGroupNoAck(t *testing.T) {
	db := makeTestDB()
	const key = "s:noack"

	streamEntryID(t, db, key)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", key, "g", "0-0")), "OK")

	read := db.Exec(nil, utils.ToCmdLine(
		"XREADGROUP", "GROUP", "g", "c", "NOACK", "STREAMS", key, ">",
	))
	if _, ok := read.(*StreamReadReply); !ok {
		t.Fatalf("XREADGROUP NOACK: got %T %s", read, read.ToBytes())
	}

	pending := db.Exec(nil, utils.ToCmdLine("XPENDING", key, "g"))
	multi, ok := pending.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("XPENDING: got %s", pending.ToBytes())
	}
	if string(multi.Args[0]) != "0" {
		t.Fatalf("expected 0 pending with NOACK, got %s", pending.ToBytes())
	}
}

func TestStreamGroupConsumerLifecycle(t *testing.T) {
	db := makeTestDB()
	const key = "s:cons"

	streamEntryID(t, db, key)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", key, "g", "0-0")), "OK")

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XGROUPCREATECONSUMER", key, "g", "c1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XGROUPCREATECONSUMER", key, "g", "c1")), 0)

	_ = db.Exec(nil, utils.ToCmdLine("XREADGROUP", "GROUP", "g", "c1", "STREAMS", key, ">"))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XGROUPDELCONSUMER", key, "g", "c1")), 1)
}

func TestStreamXReadInvalidKey(t *testing.T) {
	db := makeTestDB()
	reply := db.Exec(nil, utils.ToCmdLine("XREAD", "STREAMS", oversizedKey(), "0-0"))
	if !protocol.IsErrorReply(reply) || !strings.Contains(string(reply.ToBytes()), "key too large") {
		t.Fatalf("expected key too large, got %s", reply.ToBytes())
	}
}

func TestStreamXInfoStreamDirect(t *testing.T) {
	db := makeTestDB()
	streamEntryID(t, db, "s:xinfo")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "s:xinfo", "g", "$")), "OK")

	reply := execXInfoStream(db, [][]byte{[]byte("s:xinfo")})
	m, ok := reply.(*protocol.MapReply)
	if !ok || len(m.Data) == 0 {
		t.Fatalf("execXInfoStream: got %T %s", reply, reply.ToBytes())
	}

	full := execXInfoStream(db, [][]byte{[]byte("s:xinfo"), []byte("FULL"), []byte("COUNT"), []byte("1")})
	fm, ok := full.(*protocol.MapReply)
	if !ok {
		t.Fatalf("execXInfoStream FULL: got %T %s", full, full.ToBytes())
	}
	if _, ok := fm.Data["entries"]; !ok {
		t.Fatalf("execXInfoStream FULL missing entries: %s", full.ToBytes())
	}
}
