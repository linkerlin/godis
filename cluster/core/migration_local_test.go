package core

import (
	"sync"
	"testing"

	"github.com/linkerlin/godis/datastruct/set"
)

func TestStartExportingIdempotentAfterSetSlotMigrating(t *testing.T) {
	sm := &slotStatus{
		state:       slotStateExporting,
		migratePeer: "peer-admin",
		keys:        set.Make("a"),
		dirtyKeys:   set.Make(),
		mu:          &sync.RWMutex{},
	}
	if err := sm.startExporting("peer-real"); err != nil {
		t.Fatalf("idempotent startExporting: %v", err)
	}
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if sm.state != slotStateExporting {
		t.Fatalf("state=%d", sm.state)
	}
	if sm.migratePeer != "peer-real" {
		t.Fatalf("migratePeer=%q want peer-real", sm.migratePeer)
	}
	if sm.exportSnapshot == nil || !sm.exportSnapshot.Has("a") {
		t.Fatalf("exportSnapshot missing key a")
	}
}

func TestStartExportingRejectsImporting(t *testing.T) {
	sm := &slotStatus{
		state: slotStateImporting,
		keys:  set.Make(),
		mu:    &sync.RWMutex{},
	}
	if err := sm.startExporting("x"); err == nil {
		t.Fatal("expected error when slot is importing")
	}
}

func TestMarkImportingAndClearMigrate(t *testing.T) {
	cluster := &Cluster{slotsManager: newSlotsManager()}
	st := cluster.slotsManager.getSlot(42)
	st.markImporting("src-node")
	st.mu.RLock()
	state, peer := st.state, st.migratePeer
	st.mu.RUnlock()
	if state != slotStateImporting || peer != "src-node" {
		t.Fatalf("importing state=%d peer=%q", state, peer)
	}

	cluster.clearImportingTask()
	if cluster.slotsManager.importingTask != nil {
		t.Fatal("expected nil importingTask")
	}
	st.mu.Lock()
	st.clearMigrateWithinLock()
	st.mu.Unlock()
	if st.state != slotStateHosting || st.migratePeer != "" {
		t.Fatalf("after clear: state=%d peer=%q", st.state, st.migratePeer)
	}
}
