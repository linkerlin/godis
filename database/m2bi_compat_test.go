package database

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/linkerlin/godis/aof"
	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2biClientListNoEvictFlag(t *testing.T) {
	c := connection.NewFakeConn()
	c.SetNoEvict(true)
	line := formatClientListLine(c)
	for _, part := range strings.Fields(line) {
		if strings.HasPrefix(part, "flags=") {
			if !strings.Contains(part, "e") {
				t.Fatalf("want flag e, got %q", part)
			}
			return
		}
	}
	t.Fatalf("missing flags=: %q", line)
}

func TestM2biAppendOnlyColdStart(t *testing.T) {
	dir := t.TempDir()
	aofPath := filepath.Join(dir, "m2bi.aof")
	oldProps := config.Properties
	config.Properties = &config.ServerProperties{
		AppendOnly:     false,
		AppendFilename: aofPath,
		AppendFsync:    aof.FsyncAlways,
		Databases:      16,
	}
	defer func() { config.Properties = oldProps }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	if server.persister != nil {
		t.Fatal("expected no persister when appendonly no")
	}

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "appendfilename", aofPath)), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "appendonly", "yes")), "OK")
	if server.persister == nil {
		t.Fatal("expected persister after appendonly yes")
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "m2bi", "1")), "OK")
	data, err := os.ReadFile(aofPath)
	if err != nil || !strings.Contains(string(data), "m2bi") {
		t.Fatalf("AOF missing write: err=%v content=%q", err, string(data))
	}

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "appendonly", "no")), "OK")
	before, _ := os.Stat(aofPath)
	_ = server.Exec(c, utils.ToCmdLine("SET", "m2bi2", "2"))
	after, _ := os.Stat(aofPath)
	if after.Size() != before.Size() {
		t.Fatalf("appendonly no should not grow AOF: %d -> %d", before.Size(), after.Size())
	}
}

func TestM2biInfoPubsubAndWatchingClients(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	RegisterClient(c)
	defer UnregisterClient(c)

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("WATCH", "w1")), "OK")
	r := server.Exec(c, utils.ToCmdLine("INFO", "clients"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO clients: %T", r)
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "watching_clients:1") || !strings.Contains(s, "pubsub_clients:") {
		t.Fatalf("INFO clients: %s", s)
	}
	_ = server.Exec(c, utils.ToCmdLine("UNWATCH"))
}

func TestM2biFTGeoFilter(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "m2bi", "ON", "HASH", "PREFIX", "1", "g:",
		"SCHEMA", "name", "TEXT", "loc", "GEO",
	)), "OK")
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "m2bi", "g:near", "FIELDS", "name", "a", "loc", "13.36,52.52"))
	_ = db.Exec(nil, utils.ToCmdLine("FT.ADD", "m2bi", "g:far", "FIELDS", "name", "b", "loc", "0,0"))

	r := db.Exec(nil, utils.ToCmdLine(
		"FT.SEARCH", "m2bi", "*", "GEOFILTER", "loc", "13.4", "52.5", "50", "km",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("GEOFILTER: %s", r.ToBytes())
	}
	s := string(r.ToBytes())
	if !strings.Contains(s, "g:near") {
		t.Fatalf("expected g:near hit: %s", s)
	}
	if strings.Contains(s, "g:far") {
		t.Fatalf("g:far should be filtered out: %s", s)
	}
}

func TestM2biLuaSetReplAOFReplica(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	var aofN int
	db.addAof = func(CmdLine) { aofN++ }

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.set_repl(redis.REPL_REPLICA)
redis.call('SET', KEYS[1], 'v')
return 'ok'
`, "1", "m2bi:repl"))
	asserts.AssertBulkReply(t, r, "ok")
	if aofN != 0 {
		t.Fatalf("REPL_REPLICA should skip AOF, got %d", aofN)
	}

	aofN = 0
	r = db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.set_repl(redis.REPL_AOF)
redis.call('SET', KEYS[1], 'v2')
return 'ok'
`, "1", "m2bi:aof"))
	asserts.AssertBulkReply(t, r, "ok")
	if aofN == 0 {
		t.Fatal("REPL_AOF should write AOF")
	}
}
