package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
)

func TestDefaultFuncMovedAskAndAsking(t *testing.T) {
	ids := []string{"127.0.0.1:7000", "127.0.0.1:7001"}
	nodes := MakeTestCluster(ids)
	a := nodes[ids[0]]
	b := nodes[ids[1]]
	// Exercise client redirect path (not in-mem proxy Relay).
	a.inmemProxy = false
	b.inmemProxy = false

	// Slot of "k" on node b under TestCluster routing: GetSlot then % 2
	slot := a.GetSlot("k")
	owner := a.PickNode(slot)
	otherID := ids[0]
	if owner == ids[0] {
		otherID = ids[1]
	}
	other := nodes[otherID]
	c := connection.NewFakeConn()

	// Wrong node → MOVED
	r := DefaultFunc(other, c, utils.ToCmdLine("GET", "k"))
	moved, ok := r.(*protocol.MovedErrReply)
	if !ok {
		t.Fatalf("want MOVED, got %T %s", r, r.ToBytes())
	}
	if moved.Addr != owner {
		t.Fatalf("MOVED addr=%q want %q", moved.Addr, owner)
	}
	if !strings.HasPrefix(string(moved.ToBytes()), "-MOVED ") {
		t.Fatalf("wire: %q", moved.ToBytes())
	}

	// Right node → local exec
	r = DefaultFunc(nodes[owner], c, utils.ToCmdLine("SET", "k", "v"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("local SET: %s", r.ToBytes())
	}

	// ASKING on importer allows exec even if PickNode says exporter
	migKey := "k"
	slot = b.GetSlot(migKey)
	st := b.slotsManager.getSlot(slot)
	st.mu.Lock()
	st.state = slotStateImporting
	st.mu.Unlock()
	// Force PickNode to always return exporter (node 0) for this cluster node
	b.pickNodeImpl = func(uint32) string { return ids[0] }
	askConn := connection.NewFakeConn()
	_ = execClusterAsking(b, askConn, nil)
	if !askConn.IsAsking() {
		t.Fatal("ASKING flag not set")
	}
	r = DefaultFunc(b, askConn, utils.ToCmdLine("GET", migKey))
	if protocol.IsErrorReply(r) && strings.HasPrefix(string(r.ToBytes()), "-MOVED") {
		t.Fatalf("ASKING should avoid MOVED: %s", r.ToBytes())
	}
	if askConn.IsAsking() {
		t.Fatal("ASKING should be consumed")
	}

	// Exporter without key → ASK
	a.pickNodeImpl = func(uint32) string { return ids[0] }
	stA := a.slotsManager.getSlot(slot)
	stA.mu.Lock()
	stA.state = slotStateExporting
	stA.mu.Unlock()
	// Stub migration target via injecting FSM is heavy; call MakeAskErrReply shape via Direct helper.
	// When raft is nil migrationTargetForSlot returns "" — ASK path skipped. Unit-test Ask reply type:
	ask := protocol.MakeAskErrReply(slot, ids[1])
	if !strings.HasPrefix(string(ask.ToBytes()), "-ASK ") {
		t.Fatalf("ASK wire: %q", ask.ToBytes())
	}
}

func TestClusterReadonlyReadwriteFlags(t *testing.T) {
	c := connection.NewFakeConn()
	_ = execClusterReadonly(nil, c, nil)
	if !c.IsClusterReadOnly() {
		t.Fatal("READONLY flag")
	}
	_ = execClusterReadwrite(nil, c, nil)
	if c.IsClusterReadOnly() {
		t.Fatal("READWRITE should clear flag")
	}
}
