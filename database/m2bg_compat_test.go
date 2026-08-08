package database

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2bgInfoKeyspaceAndExpired(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	atomic.StoreUint64(&serverStats.ExpiredKeys, 0)
	atomic.StoreUint64(&serverStats.KeyspaceHits, 0)
	atomic.StoreUint64(&serverStats.KeyspaceMisses, 0)
	defer func() {
		atomic.StoreUint64(&serverStats.ExpiredKeys, 0)
		atomic.StoreUint64(&serverStats.KeyspaceHits, 0)
		atomic.StoreUint64(&serverStats.KeyspaceMisses, 0)
	}()

	_ = server.Exec(c, utils.ToCmdLine("SET", "hit", "1"))
	_ = server.Exec(c, utils.ToCmdLine("GET", "hit"))
	_ = server.Exec(c, utils.ToCmdLine("GET", "missing"))
	if atomic.LoadUint64(&serverStats.KeyspaceHits) == 0 {
		t.Fatal("expected keyspace_hits > 0")
	}
	if atomic.LoadUint64(&serverStats.KeyspaceMisses) == 0 {
		t.Fatal("expected keyspace_misses > 0")
	}

	_ = server.Exec(c, utils.ToCmdLine("SET", "exp", "1", "PX", "30"))
	time.Sleep(50 * time.Millisecond)
	_ = server.Exec(c, utils.ToCmdLine("GET", "exp"))
	if atomic.LoadUint64(&serverStats.ExpiredKeys) == 0 {
		t.Fatal("expected expired_keys > 0 after lazy expire")
	}

	r := server.Exec(c, utils.ToCmdLine("INFO", "stats"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO: %T", r)
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "expired_keys:") || !strings.Contains(s, "keyspace_hits:") || !strings.Contains(s, "keyspace_misses:") {
		t.Fatalf("INFO stats missing counters: %s", s)
	}

	_ = server.Exec(c, utils.ToCmdLine("CONFIG", "RESETSTAT"))
	if atomic.LoadUint64(&serverStats.KeyspaceHits) != 0 || atomic.LoadUint64(&serverStats.ExpiredKeys) != 0 {
		t.Fatal("RESETSTAT should clear counters")
	}
}

func TestM2bgClientListMulti(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	RegisterClient(c)
	defer UnregisterClient(c)

	line := formatClientListLine(c)
	if !strings.Contains(line, "multi=-1") {
		t.Fatalf("want multi=-1, got %q", line)
	}

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("MULTI")), "OK")
	_ = server.Exec(c, utils.ToCmdLine("SET", "a", "1"))
	_ = server.Exec(c, utils.ToCmdLine("SET", "b", "2"))
	line = formatClientListLine(c)
	if !strings.Contains(line, "multi=2") || !strings.Contains(line, "flags=x") {
		t.Fatalf("want multi=2 flags=x, got %q", line)
	}
	_ = server.Exec(c, utils.ToCmdLine("DISCARD"))
	line = formatClientListLine(c)
	if !strings.Contains(line, "multi=-1") {
		t.Fatalf("after DISCARD want multi=-1, got %q", line)
	}
}

func TestM2bgConfigDirDbfilename(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	oldDir := config.Properties.Dir
	oldRDB := config.Properties.RDBFilename
	defer func() {
		config.Properties.Dir = oldDir
		config.Properties.RDBFilename = oldRDB
	}()

	dir := t.TempDir()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "dir", dir)), "OK")
	if config.Properties.Dir != dir {
		t.Fatalf("Dir want %q got %q", dir, config.Properties.Dir)
	}
	if _, err := os.Stat(filepath.Join(dir, "tmp")); err != nil {
		t.Fatalf("tmp under dir: %v", err)
	}
	if config.GetTmpDir() != filepath.Join(dir, "tmp") && config.GetTmpDir() != dir+"/tmp" {
		// GetTmpDir uses string concat Dir+"/tmp"
		if !strings.HasPrefix(config.GetTmpDir(), dir) {
			t.Fatalf("GetTmpDir=%q not under %q", config.GetTmpDir(), dir)
		}
	}

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "dbfilename", "m2bg.rdb")), "OK")
	r := server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "dbfilename"))
	if val, ok := configReplyValue(r, "dbfilename"); !ok || val != "m2bg.rdb" {
		t.Fatalf("CONFIG GET dbfilename: %s", r.ToBytes())
	}
	r = server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "rdbfilename"))
	if val, ok := configReplyValue(r, "rdbfilename"); !ok || val != "m2bg.rdb" {
		t.Fatalf("alias rdbfilename: %s", r.ToBytes())
	}
}

func TestM2bgFTAggregateFilterAt(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "m2bg", "ON", "HASH", "PREFIX", "1", "g:", "SCHEMA", "cat", "TAG", "n", "NUMERIC",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "m2bg", "g:1", "FIELDS", "cat", "a", "n", "5"))
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "m2bg", "g:2", "FIELDS", "cat", "a", "n", "20"))
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "m2bg", "g:3", "FIELDS", "cat", "b", "n", "3"))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.AGGREGATE", "m2bg", "*",
		"GROUPBY", "1", "@cat",
		"REDUCE", "SUM", "1", "@n", "AS", "sum",
		"FILTER", "@sum > 10",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("FILTER @: %s", r.ToBytes())
	}
	s := string(r.ToBytes())
	// total groups after filter should be 1 (cat a sum=25)
	if !strings.Contains(s, "*1\r\n") && !strings.HasPrefix(s, "*2\r\n$1\r\n1\r\n") {
		// reply starts with total count bulk
		if !strings.Contains(s, "$1\r\n1\r\n") {
			t.Fatalf("expected 1 group after filter, got %s", s)
		}
	}
}

func TestM2bgLuaSetReplNoneSkipsAOF(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	var aofN int
	db.addAof = func(CmdLine) { aofN++ }

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.set_repl(redis.REPL_NONE)
redis.call('SET', KEYS[1], 'v')
return 'ok'
`, "1", "m2bg:lua"))
	asserts.AssertBulkReply(t, r, "ok")
	if aofN != 0 {
		t.Fatalf("REPL_NONE should skip AOF, got %d writes", aofN)
	}

	aofN = 0
	r = db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.call('SET', KEYS[1], 'v2')
return 'ok'
`, "1", "m2bg:lua2"))
	asserts.AssertBulkReply(t, r, "ok")
	if aofN == 0 {
		t.Fatal("default REPL_ALL should write AOF")
	}
}
