package core

import (
	"strconv"
	"strings"
	"testing"

	"github.com/linkerlin/godis/cluster/raft"
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

func TestSetSlotMigratingFeedsAsk(t *testing.T) {
	ids := []string{"127.0.0.1:7100", "127.0.0.1:7101"}
	nodes := MakeTestCluster(ids)
	a := nodes[ids[0]]
	a.inmemProxy = false
	a.pickNodeImpl = func(uint32) string { return ids[0] }

	key := "ask-me"
	slot := a.GetSlot(key)
	slotStr := strconv.FormatUint(uint64(slot), 10)

	r := execCluster(a, nil, [][]byte{
		[]byte("CLUSTER"), []byte("SETSLOT"),
		[]byte(slotStr), []byte("MIGRATING"), []byte(ids[1]),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("SETSLOT MIGRATING: %T %s", r, r.ToBytes())
	}

	st := a.slotsManager.getSlot(slot)
	st.mu.RLock()
	if st.state != slotStateExporting || st.migratePeer != ids[1] {
		st.mu.RUnlock()
		t.Fatalf("slot state=%d peer=%q", st.state, st.migratePeer)
	}
	st.mu.RUnlock()

	if target := a.migrationTargetForSlot(slot); target != ids[1] {
		t.Fatalf("migrationTarget=%q want %q", target, ids[1])
	}

	c := connection.NewFakeConn()
	got := DefaultFunc(a, c, utils.ToCmdLine("GET", key))
	ask, ok := got.(*protocol.AskErrReply)
	if !ok {
		t.Fatalf("want ASK, got %T %s", got, got.ToBytes())
	}
	if ask.Addr != ids[1] {
		t.Fatalf("ASK addr=%q want %q", ask.Addr, ids[1])
	}

	r = execCluster(a, nil, [][]byte{
		[]byte("CLUSTER"), []byte("SETSLOT"),
		[]byte(slotStr), []byte("STABLE"),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("SETSLOT STABLE: %s", r.ToBytes())
	}
	got = DefaultFunc(a, c, utils.ToCmdLine("GET", key))
	if _, ok := got.(*protocol.AskErrReply); ok {
		t.Fatal("STABLE should clear ASK")
	}
}

func TestSetSlotNodeClearsAsk(t *testing.T) {
	ids := []string{"127.0.0.1:7100", "127.0.0.1:7101"}
	nodes := MakeTestCluster(ids)
	a := nodes[ids[0]]
	a.inmemProxy = false
	a.pickNodeImpl = func(uint32) string { return ids[0] }
	// FSM-only so NODE can ApplyLocal
	a.raftNode = &raft.Node{
		Cfg: &raft.RaftConfig{RedisAdvertiseAddr: ids[0]},
		FSM: &raft.FSM{
			Node2Slot:    map[string][]uint32{ids[0]: {}},
			Slot2Node:    map[uint32]string{},
			Migratings:   make(map[string]*raft.MigratingTask),
			MasterSlaves: map[string]*raft.MasterSlave{ids[0]: {MasterId: ids[0]}},
			SlaveMasters: make(map[string]string),
			Failovers:    make(map[string]*raft.FailoverTask),
		},
	}

	key := "ask-node"
	slot := a.GetSlot(key)
	slotStr := strconv.FormatUint(uint64(slot), 10)

	// Own the slot in FSM, then mark MIGRATING → ASK
	a.raftNode.ApplyLocal(&raft.LogEntry{
		Event: raft.EventAddSlots,
		SlotsTask: &raft.SlotsTask{NodeId: ids[0], Slots: []uint32{slot}},
	})
	r := execCluster(a, nil, [][]byte{
		[]byte("CLUSTER"), []byte("SETSLOT"),
		[]byte(slotStr), []byte("MIGRATING"), []byte(ids[1]),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("MIGRATING: %s", r.ToBytes())
	}
	c := connection.NewFakeConn()
	got := DefaultFunc(a, c, utils.ToCmdLine("GET", key))
	if _, ok := got.(*protocol.AskErrReply); !ok {
		t.Fatalf("want ASK before NODE, got %T %s", got, got.ToBytes())
	}

	r = execCluster(a, nil, [][]byte{
		[]byte("CLUSTER"), []byte("SETSLOT"),
		[]byte(slotStr), []byte("NODE"), []byte(ids[1]),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("NODE: %s", r.ToBytes())
	}
	if a.raftNode.FSM.Slot2Node[slot] != ids[1] {
		t.Fatalf("owner=%q want %q", a.raftNode.FSM.Slot2Node[slot], ids[1])
	}
	got = DefaultFunc(a, c, utils.ToCmdLine("GET", key))
	if _, ok := got.(*protocol.AskErrReply); ok {
		t.Fatal("NODE should clear ASK migrate state")
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

// Real doMigrate path writes FSM Migratings without SETSLOT; ASK must still fire
// for missing keys on the exporter even when local state is still hosting.
func TestFSMMigratingsAskWithoutLocalExporting(t *testing.T) {
	ids := []string{"127.0.0.1:7200", "127.0.0.1:7201"}
	nodes := MakeTestCluster(ids)
	a := nodes[ids[0]]
	a.inmemProxy = false
	a.pickNodeImpl = func(uint32) string { return ids[0] }
	a.raftNode = &raft.Node{
		Cfg: &raft.RaftConfig{RedisAdvertiseAddr: ids[0]},
		FSM: &raft.FSM{
			Node2Slot:    map[string][]uint32{ids[0]: {}},
			Slot2Node:    map[uint32]string{},
			Migratings:   make(map[string]*raft.MigratingTask),
			MasterSlaves: map[string]*raft.MasterSlave{ids[0]: {MasterId: ids[0]}},
			SlaveMasters: make(map[string]string),
			Failovers:    make(map[string]*raft.FailoverTask),
		},
	}

	key := "fsm-ask"
	slot := a.GetSlot(key)
	a.raftNode.ApplyLocal(&raft.LogEntry{
		Event:     raft.EventAddSlots,
		SlotsTask: &raft.SlotsTask{NodeId: ids[0], Slots: []uint32{slot}},
	})
	a.raftNode.FSM.Migratings["t1"] = &raft.MigratingTask{
		ID:         "t1",
		SrcNode:    ids[0],
		TargetNode: ids[1],
		Slots:      []uint32{slot},
	}

	st := a.slotsManager.getSlot(slot)
	st.mu.RLock()
	state := st.state
	st.mu.RUnlock()
	if state != slotStateHosting {
		t.Fatalf("precondition: local hosting, got %d", state)
	}
	if target := a.migrationTargetForSlot(slot); target != ids[1] {
		t.Fatalf("migrationTarget=%q want %q", target, ids[1])
	}

	c := connection.NewFakeConn()
	got := DefaultFunc(a, c, utils.ToCmdLine("GET", key))
	ask, ok := got.(*protocol.AskErrReply)
	if !ok {
		t.Fatalf("want ASK from FSM Migratings, got %T %s", got, got.ToBytes())
	}
	if ask.Addr != ids[1] {
		t.Fatalf("ASK addr=%q want %q", ask.Addr, ids[1])
	}
}

