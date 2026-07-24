package database

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/aof"
	"github.com/linkerlin/godis/datastruct/dict"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2afConfigResetStat(t *testing.T) {
	server, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	c := connection.NewFakeConn()
	_ = server.Exec(c, utils.ToCmdLine("SET", "a", "1"))
	_ = server.Exec(c, utils.ToCmdLine("GET", "a"))
	if serverStats.TotalCommandsProcessed == 0 {
		t.Fatal("expected commands_processed > 0 before reset")
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "RESETSTAT")), "OK")
	if serverStats.TotalCommandsProcessed != 0 {
		t.Fatalf("after RESETSTAT got %d", serverStats.TotalCommandsProcessed)
	}
}

func TestM2afExpireTimeErrorStrings(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("SET", "k", "v", "EX", "-1"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "invalid expire time in 'set' command") {
		t.Fatalf("SET: %s", r.ToBytes())
	}
	db.Exec(nil, utils.ToCmdLine("SET", "k", "v"))
	r = db.Exec(nil, utils.ToCmdLine("GETEX", "k", "EX", "-1"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "invalid expire time in 'getex' command") {
		t.Fatalf("GETEX: %s", r.ToBytes())
	}
}

func TestM2afHashFieldTTLOpaqueRoundTrip(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("HSET", "h", "f", "v")), 1)
	hexp := db.Exec(nil, utils.ToCmdLine("HEXPIRE", "h", "3600", "FIELDS", "1", "f"))
	if protocol.IsErrorReply(hexp) {
		t.Fatalf("HEXPIRE: %s", hexp.ToBytes())
	}

	entity, ok := db.GetEntity("h")
	if !ok {
		t.Fatal("missing hash")
	}
	if _, ok := entity.Data.(*dict.ExpireDict); !ok {
		t.Fatalf("expected ExpireDict, got %T", entity.Data)
	}
	payload, ok := aof.EncodeOpaque(entity)
	if !ok {
		t.Fatal("EncodeOpaque ExpireDict")
	}
	restored, ok := aof.DecodeOpaque(payload)
	if !ok {
		t.Fatal("DecodeOpaque")
	}
	ed := restored.Data.(*dict.ExpireDict)
	val, exists := ed.Get("f")
	if !exists || string(val.([]byte)) != "v" {
		t.Fatalf("field value: exists=%v val=%v", exists, val)
	}
	if _, has := ed.GetExpireTime("f"); !has {
		t.Fatal("field TTL lost after opaque round-trip")
	}

	// DUMP/RESTORE path
	dump := db.Exec(nil, utils.ToCmdLine("DUMP", "h"))
	bulk, ok := dump.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) == 0 {
		t.Fatalf("DUMP: %s", dump.ToBytes())
	}
	db.Exec(nil, utils.ToCmdLine("DEL", "h"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("RESTORE", "h2", "0", string(bulk.Arg))), "OK")
	ttl := db.Exec(nil, utils.ToCmdLine("HPTTL", "h2", "FIELDS", "1", "f"))
	var ms int64
	switch r := ttl.(type) {
	case *protocol.MultiBulkReply:
		if len(r.Args) != 1 {
			t.Fatalf("HPTTL: %s", ttl.ToBytes())
		}
		ms, _ = strconv.ParseInt(string(r.Args[0]), 10, 64)
	case *protocol.MultiRawReply:
		if len(r.Replies) != 1 {
			t.Fatalf("HPTTL: %s", ttl.ToBytes())
		}
		ir, ok := r.Replies[0].(*protocol.IntReply)
		if !ok {
			t.Fatalf("HPTTL elem: %T", r.Replies[0])
		}
		ms = ir.Code
	default:
		t.Fatalf("HPTTL type: %T %s", ttl, ttl.ToBytes())
	}
	if ms <= 0 {
		t.Fatalf("expected positive field TTL after RESTORE, got %d", ms)
	}
}

func TestM2afXPendingTimeFilter(t *testing.T) {
	db := makeTestDB()
	key := "s-time"
	idReply := db.Exec(nil, utils.ToCmdLine("XADD", key, "*", "a", "1"))
	bulk, ok := idReply.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("XADD: %s", idReply.ToBytes())
	}
	entryID := string(bulk.Arg)
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", key, "g", "0-0")), "OK")
	read := db.Exec(nil, utils.ToCmdLine("XREADGROUP", "GROUP", "g", "c1", "STREAMS", key, ">"))
	if protocol.IsErrorReply(read) {
		t.Fatalf("XREADGROUP: %s", read.ToBytes())
	}
	// baseline without TIME must see the pending entry
	base := db.Exec(nil, utils.ToCmdLine("XPENDING", key, "g", "0-0", entryID, "10"))
	bm, ok := base.(*protocol.MultiRawReply)
	if !ok || len(bm.Replies) != 1 {
		t.Fatalf("baseline XPENDING: %T %s (read=%s)", base, base.ToBytes(), read.ToBytes())
	}

	future := strconv.FormatInt(time.Now().Add(time.Hour).UnixMilli(), 10)
	empty := db.Exec(nil, utils.ToCmdLine("XPENDING", key, "g", "TIME", future, "0-0", entryID, "10"))
	fm, ok := empty.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("XPENDING TIME future type: %T %s", empty, empty.ToBytes())
	}
	if len(fm.Replies) != 0 {
		t.Fatalf("TIME future should filter all: %s", empty.ToBytes())
	}

	all := db.Exec(nil, utils.ToCmdLine("XPENDING", key, "g", "TIME", "0", "0-0", entryID, "10"))
	mr, ok := all.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 1 {
		t.Fatalf("TIME 0: %s", all.ToBytes())
	}
}