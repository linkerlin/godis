package database

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/linkerlin/godis/aof"
	"github.com/linkerlin/godis/config"
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
