package database

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2aeParseSaveConfig(t *testing.T) {
	pts := parseSaveConfig("3600 1 300 100")
	if len(pts) != 2 || pts[0].seconds != 3600 || pts[0].changes != 1 {
		t.Fatalf("parse: %+v", pts)
	}
	if parseSaveConfig("") != nil || parseSaveConfig("1") != nil {
		t.Fatal("invalid should disable")
	}
}

func TestM2aeSaveScheduleDirty(t *testing.T) {
	server, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	c := connection.NewFakeConn()
	oldRDB := config.Properties.RDBFilename
	defer func() { config.Properties.RDBFilename = oldRDB }()
	// Do not mutate global Save — shared testServer cron would autosave the suite.
	config.Properties.RDBFilename = filepath.Join(t.TempDir(), "m2ae.rdb")

	server.dirty.Store(0)
	server.lastSaveUnix.Store(time.Now().Unix() - 2)
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "k", "v")), "OK")
	if server.DirtyChanges() < 1 {
		t.Fatalf("dirty should increase after write, got %d", server.DirtyChanges())
	}
	server.checkSavePointsWith("1 1")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if server.DirtyChanges() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected dirty reset after autosave, still %d", server.DirtyChanges())
}

func TestM2aeGeoHash52BitExact(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"GEOADD", "g", "13.361389", "38.115556", "Palermo",
	)), 1)
	got := db.Exec(nil, utils.ToCmdLine("GEOHASH", "g", "Palermo"))
	mr, ok := got.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 1 || len(mr.Args[0]) == 0 {
		t.Fatalf("GEOHASH: %s", got.ToBytes())
	}
	pos := db.Exec(nil, utils.ToCmdLine("GEOPOS", "g", "Palermo"))
	pm, ok := pos.(*protocol.MultiRawReply)
	if !ok || len(pm.Replies) != 1 {
		t.Fatalf("GEOPOS: %s", pos.ToBytes())
	}
}

func TestM2aeReplBacklogRing(t *testing.T) {
	bl := newReplBacklog(8)
	bl.appendBytes([]byte("abcdefgh"))
	if bl.histLen() != 8 || bl.beginOffset != 0 {
		t.Fatalf("full: hist=%d begin=%d", bl.histLen(), bl.beginOffset)
	}
	bl.appendBytes([]byte("XY"))
	if bl.beginOffset != 2 || bl.histLen() != 8 {
		t.Fatalf("ring drop: begin=%d hist=%d", bl.beginOffset, bl.histLen())
	}
	snap, _ := bl.getSnapshot()
	if string(snap) != "cdefghXY" {
		t.Fatalf("snapshot=%q", snap)
	}
	if bl.isValidOffset(0) || bl.isValidOffset(1) {
		t.Fatal("old offsets must be invalid")
	}
	part, _ := bl.getSnapshotAfter(4)
	if string(part) != "efghXY" {
		t.Fatalf("after=%q", part)
	}
}
