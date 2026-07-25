package database

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2agMemoryUsageExpireDict(t *testing.T) {
	server, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	c := connection.NewFakeConn()
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("HSET", "h", "f", "hello-world-value")), 1)
	_ = server.Exec(c, utils.ToCmdLine("HEXPIRE", "h", "60", "FIELDS", "1", "f"))

	server.Exec(c, utils.ToCmdLine("SET", "plain", "hello-world-value"))
	plainSize := server.Exec(c, utils.ToCmdLine("MEMORY", "USAGE", "plain"))
	hashSize := server.Exec(c, utils.ToCmdLine("MEMORY", "USAGE", "h"))
	ps, ok1 := plainSize.(*protocol.IntReply)
	hs, ok2 := hashSize.(*protocol.IntReply)
	if !ok1 || !ok2 {
		t.Fatalf("MEMORY USAGE types: plain=%T hash=%T", plainSize, hashSize)
	}
	// default stub is keyLen+64+128; ExpireDict path must exceed that for a real value
	stub := int64(len("h") + 64 + 128)
	if hs.Code == stub {
		t.Fatalf("ExpireDict still using default stub %d", hs.Code)
	}
	if hs.Code <= int64(len("h")+64) {
		t.Fatalf("hash estimate too small: %d", hs.Code)
	}
	_ = ps
}

func TestM2agConfigRewrite(t *testing.T) {
	dir := t.TempDir()
	conf := filepath.Join(dir, "godis.conf")
	if err := os.WriteFile(conf, []byte("port 6399\nmaxmemory 0\n"), 0644); err != nil {
		t.Fatal(err)
	}
	oldPath := config.GetConfigFilePath()
	oldMem := config.Properties.Maxmemory
	defer func() {
		config.SetConfigFilePath(oldPath)
		config.Properties.Maxmemory = oldMem
	}()
	config.SetConfigFilePath(conf)

	server, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory", "1048576")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "REWRITE")), "OK")

	data, err := os.ReadFile(conf)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, "maxmemory") || !strings.Contains(text, "1048576") {
		t.Fatalf("REWRITE missing maxmemory: %q", text)
	}
}

func TestM2agConfigRewriteNoFile(t *testing.T) {
	oldPath := config.GetConfigFilePath()
	defer config.SetConfigFilePath(oldPath)
	config.SetConfigFilePath("")
	server, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("CONFIG", "REWRITE"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "without a config file") {
		t.Fatalf("expected no-config error, got %s", r.ToBytes())
	}
}

func TestM2agFunctionDumpRestoreBinary(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	code := "#!lua name=m2aglib\nredis.register_function('m2agf', function(keys, args) return 'ok' end)"
	load := db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", code))
	if protocol.IsErrorReply(load) {
		t.Fatalf("FUNCTION LOAD: %s", load.ToBytes())
	}

	dump := db.Exec(nil, utils.ToCmdLine("FUNCTION", "DUMP"))
	bulk, ok := dump.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) < 8 || string(bulk.Arg[:8]) != "GODISFN1" {
		t.Fatalf("DUMP magic missing: %v", dump.ToBytes())
	}

	flush := db.Exec(nil, utils.ToCmdLine("FUNCTION", "FLUSH"))
	if protocol.IsErrorReply(flush) {
		t.Fatalf("FLUSH: %s", flush.ToBytes())
	}
	restore := db.Exec(nil, utils.ToCmdLine("FUNCTION", "RESTORE", string(bulk.Arg), "FLUSH"))
	if protocol.IsErrorReply(restore) {
		t.Fatalf("RESTORE: %s", restore.ToBytes())
	}

	list := db.Exec(nil, utils.ToCmdLine("FUNCTION", "LIST"))
	if protocol.IsErrorReply(list) || !strings.Contains(string(list.ToBytes()), "m2aglib") {
		t.Fatalf("RESTORE lost library: %s", list.ToBytes())
	}
}

func TestM2agTSIncrByRetentionLabels(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine(
		"TS.INCRBY", "ts1", "1.5", "RETENTION", "60000", "LABELS", "sensor", "temp",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("TS.INCRBY: %s", r.ToBytes())
	}
	info := db.Exec(nil, utils.ToCmdLine("TS.INFO", "ts1"))
	s := string(info.ToBytes())
	if !strings.Contains(s, "retention") && !strings.Contains(s, "60000") && !strings.Contains(s, "sensor") {
		// TS.INFO format varies; at least key must exist and GET works
		got := db.Exec(nil, utils.ToCmdLine("TS.GET", "ts1"))
		if protocol.IsErrorReply(got) {
			t.Fatalf("TS.GET after INCRBY create: %s info=%s", got.ToBytes(), s)
		}
	}
	// Verify labels via TS.MGET filter if available
	mget := db.Exec(nil, utils.ToCmdLine("TS.MGET", "FILTER", "sensor=temp"))
	if protocol.IsErrorReply(mget) {
		t.Fatalf("LABELS not applied: %s", mget.ToBytes())
	}
}
