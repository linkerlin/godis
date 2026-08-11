package database

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/linkerlin/godis/aof"
	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/datastruct/redisearch"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestP8FTPersistence verifies FT index definitions, indexed data, FT.CONFIG
// settings, and dictionaries all survive an AOF replay (the godis persistence
// model: FT.CREATE + HSET auto-index + CONFIG SET + DICTADD are all AOF'd, so a
// restart rebuilds everything). This is the end-to-end validation that the P8-a
// addAof fixes for CONFIG SET / DICTADD/DEL actually persist.
func TestP8FTPersistence(t *testing.T) {
	skipHeavyTests(t)
	tmpDir, err := os.MkdirTemp("", "godis-ft")
	if err != nil {
		t.Fatal(err)
	}
	aofFilename := filepath.Join(tmpDir, "ft.aof")
	oldProps := config.Properties
	t.Cleanup(func() {
		config.Properties = oldProps
		_ = os.RemoveAll(tmpDir)
	})
	config.Properties = &config.ServerProperties{
		AppendOnly:        true,
		AppendFilename:    aofFilename,
		AofUseRdbPreamble: false,
		AppendFsync:       aof.FsyncAlways,
	}

	// --- Server 1: create the index, add data, set config, add a dict term.
	writeSrv := MustNewStandaloneServer()
	wc := connection.NewFakeConn()
	asserts.AssertStatusReply(t, writeSrv.Exec(wc, utils.ToCmdLine(
		"FT.CREATE", "p8idx", "ON", "HASH", "PREFIX", "1", "p8:", "SKIPINITIALSCAN", "SCHEMA", "t", "TEXT",
	)), "OK")
	asserts.AssertIntReply(t, writeSrv.Exec(wc, utils.ToCmdLine("HSET", "p8:1", "t", "persisted")), 1)
	asserts.AssertStatusReply(t, writeSrv.Exec(wc, utils.ToCmdLine("FT.CONFIG", "SET", "MINPREFIX", "3")), "OK")
	asserts.AssertIntReply(t, writeSrv.Exec(wc, utils.ToCmdLine("FT.DICTADD", "p8dict", "persisted")), 1)
	writeSrv.Close() // flush AOF

	// --- Server 2: restart, AOF replays.
	readSrv := MustNewStandaloneServer()
	defer readSrv.Close()
	rc := connection.NewFakeConn()

	// Index definition survived: search finds the doc.
	r := readSrv.Exec(rc, utils.ToCmdLine("FT.SEARCH", "p8idx", "persisted", "NOCONTENT"))
	if !searchTotalIs(t, r, 1) {
		t.Fatalf("after restart: expected 1 hit for 'persisted', got %s", r.ToBytes())
	}

	// FT.CONFIG setting survived.
	r = readSrv.Exec(rc, utils.ToCmdLine("FT.CONFIG", "GET", "MINPREFIX"))
	m, ok := r.(*protocol.MapReply)
	if !ok {
		t.Fatalf("FT.CONFIG GET shape: %T %s", r, r.ToBytes())
	}
	v, ok := m.Data["MINPREFIX"].(*protocol.BulkReply)
	if !ok || string(v.Arg) != "3" {
		t.Fatalf("FT.CONFIG MINPREFIX not restored to 3: %s", r.ToBytes())
	}

	// Dictionary survived.
	r = readSrv.Exec(rc, utils.ToCmdLine("FT.DICTDUMP", "p8dict"))
	if mb, ok := r.(*protocol.MultiBulkReply); !ok || len(mb.Args) == 0 {
		t.Fatalf("FT.DICTDUMP after restart should list terms, got %s", r.ToBytes())
	}
}

// TestP8FTAofRewritePersistsIndexDef verifies pure AOF rewrite emits FT.CREATE
// so an index definition survives rewrite + restart (minimal replayable path;
// RDB / RDB-preamble still do not encode FT engines).
func TestP8FTAofRewritePersistsIndexDef(t *testing.T) {
	skipHeavyTests(t)
	tmpFile, err := os.CreateTemp(config.GetTmpDir(), "ft-rewrite-*.aof")
	if err != nil {
		t.Fatal(err)
	}
	aofFilename := tmpFile.Name()
	_ = tmpFile.Close()
	defer os.Remove(aofFilename)

	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:         16,
		AppendOnly:        true,
		AppendFilename:    aofFilename,
		AofUseRdbPreamble: false,
		AppendFsync:       aof.FsyncAlways,
	}
	defer func() { config.Properties = old }()

	writeSrv := MustNewStandaloneServer()
	conn := connection.NewFakeConn()
	asserts.AssertStatusReply(t, writeSrv.Exec(conn, utils.ToCmdLine(
		"FT.CREATE", "rwidx", "ON", "HASH", "PREFIX", "1", "rw:", "SKIPINITIALSCAN", "SCHEMA", "t", "TEXT",
	)), "OK")
	asserts.AssertIntReply(t, writeSrv.Exec(conn, utils.ToCmdLine("HSET", "rw:1", "t", "rewritten")), 1)

	ctx, err := writeSrv.persister.StartRewrite()
	if err != nil {
		t.Fatalf("StartRewrite: %v", err)
	}
	if err := writeSrv.persister.DoRewrite(ctx); err != nil {
		t.Fatalf("DoRewrite: %v", err)
	}
	if err := writeSrv.persister.FinishRewrite(ctx); err != nil {
		t.Fatalf("FinishRewrite: %v", err)
	}
	writeSrv.Close()

	// Simulate cold restart: drop in-memory index registry before LoadAof.
	searchEnginesMu.Lock()
	searchEngines = make(map[string]*redisearch.RediSearchEngine)
	searchEnginesMu.Unlock()
	searchIndexMetaMu.Lock()
	searchIndexMeta = make(map[string]*indexMeta)
	searchIndexMetaMu.Unlock()

	readSrv := MustNewStandaloneServer()
	defer readSrv.Close()
	r := readSrv.Exec(conn, utils.ToCmdLine("FT.SEARCH", "rwidx", "rewritten", "NOCONTENT"))
	if !searchTotalIs(t, r, 1) {
		t.Fatalf("after AOF rewrite restart: expected 1 hit, got %s", r.ToBytes())
	}
}

// TestP8FTRDBPersistsIndexDef verifies RDB (and thus RDB-preamble) opaque
// encodes FT.CREATE args and LoadRDB rebuilds the index with document backfill.
func TestP8FTRDBPersistsIndexDef(t *testing.T) {
	skipHeavyTests(t)
	tmpDir, err := os.MkdirTemp("", "godis-ft-rdb")
	if err != nil {
		t.Fatal(err)
	}
	rdbFilename := filepath.Join(tmpDir, "dump.rdb")
	oldProps := config.Properties
	t.Cleanup(func() {
		config.Properties = oldProps
		_ = os.RemoveAll(tmpDir)
	})
	config.Properties = &config.ServerProperties{
		Databases:   16,
		AppendOnly:  false,
		RDBFilename: rdbFilename,
	}

	writeSrv := MustNewStandaloneServer()
	conn := connection.NewFakeConn()
	asserts.AssertStatusReply(t, writeSrv.Exec(conn, utils.ToCmdLine(
		"FT.CREATE", "rdbidx", "ON", "HASH", "PREFIX", "1", "rdb:", "SKIPINITIALSCAN", "SCHEMA", "t", "TEXT",
	)), "OK")
	// HSET auto-indexes after create; SKIPINITIALSCAN is stripped on RDB encode
	// so cold load backfills even if POST-create docs were only in the HASH.
	asserts.AssertIntReply(t, writeSrv.Exec(conn, utils.ToCmdLine("HSET", "rdb:1", "t", "rdbhit")), 1)

	if err := aof.WriteRDBFromDB(rdbFilename, writeSrv); err != nil {
		t.Fatalf("WriteRDBFromDB: %v", err)
	}
	writeSrv.Close()

	searchEnginesMu.Lock()
	searchEngines = make(map[string]*redisearch.RediSearchEngine)
	searchEnginesMu.Unlock()
	searchIndexMetaMu.Lock()
	searchIndexMeta = make(map[string]*indexMeta)
	searchIndexMetaMu.Unlock()

	readSrv := MustNewStandaloneServer()
	defer readSrv.Close()
	r := readSrv.Exec(conn, utils.ToCmdLine("FT.SEARCH", "rdbidx", "rdbhit", "NOCONTENT"))
	if !searchTotalIs(t, r, 1) {
		t.Fatalf("after RDB load: expected 1 hit, got %s", r.ToBytes())
	}
}

