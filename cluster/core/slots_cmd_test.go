package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/cluster/raft"
	"github.com/linkerlin/godis/redis/protocol"
)

func newFSMCluster(id string) *Cluster {
	fsm := &raft.FSM{
		Node2Slot:    make(map[string][]uint32),
		Slot2Node:    make(map[uint32]string),
		Migratings:   make(map[string]*raft.MigratingTask),
		MasterSlaves: make(map[string]*raft.MasterSlave),
		SlaveMasters: make(map[string]string),
		Failovers:    make(map[string]*raft.FailoverTask),
	}
	return &Cluster{
		id_: id,
		raftNode: &raft.Node{
			Cfg: &raft.RaftConfig{RedisAdvertiseAddr: id},
			FSM: fsm,
		},
	}
}

func TestAddDelSlotsWritesFSM(t *testing.T) {
	cl := newFSMCluster("127.0.0.1:7000")

	r := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("ADDSLOTS"), []byte("1"), []byte("2"), []byte("5"),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("ADDSLOTS: %T %s", r, r.ToBytes())
	}
	if cl.raftNode.FSM.Slot2Node[1] != cl.SelfID() || cl.raftNode.FSM.Slot2Node[5] != cl.SelfID() {
		t.Fatalf("Slot2Node=%v", cl.raftNode.FSM.Slot2Node)
	}

	// busy
	bad := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("ADDSLOTS"), []byte("1"),
	})
	if !protocol.IsErrorReply(bad) || !strings.Contains(string(bad.ToBytes()), "already busy") {
		t.Fatalf("want busy: %s", bad.ToBytes())
	}

	r = execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("DELSLOTS"), []byte("1"),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("DELSLOTS: %s", r.ToBytes())
	}
	if _, ok := cl.raftNode.FSM.Slot2Node[1]; ok {
		t.Fatal("slot 1 should be unassigned")
	}

	// unassigned
	bad = execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("DELSLOTS"), []byte("1"),
	})
	if !protocol.IsErrorReply(bad) || !strings.Contains(string(bad.ToBytes()), "already unassigned") {
		t.Fatalf("want unassigned: %s", bad.ToBytes())
	}

	// range + flush
	r = execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("ADDSLOTSRANGE"), []byte("10"), []byte("12"),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("ADDSLOTSRANGE: %s", r.ToBytes())
	}
	if len(cl.raftNode.FSM.Slot2Node) != 5 { // 2,5,10,11,12
		t.Fatalf("assigned=%d map=%v", len(cl.raftNode.FSM.Slot2Node), cl.raftNode.FSM.Slot2Node)
	}

	r = execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("FLUSHSLOTS")})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("FLUSHSLOTS: %s", r.ToBytes())
	}
	if len(cl.raftNode.FSM.Slot2Node) != 0 {
		t.Fatalf("after flush: %v", cl.raftNode.FSM.Slot2Node)
	}
}

func TestAddSlotsDuplicateReject(t *testing.T) {
	cl := newFSMCluster("n1")
	bad := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("ADDSLOTS"), []byte("3"), []byte("3"),
	})
	if !protocol.IsErrorReply(bad) || !strings.Contains(string(bad.ToBytes()), "multiple times") {
		t.Fatalf("want duplicate: %s", bad.ToBytes())
	}
}

func TestSetSlotNodeWritesFSMAndClearsMigrate(t *testing.T) {
	src := newFSMCluster("127.0.0.1:7000")
	_ = execCluster(src, nil, [][]byte{
		[]byte("CLUSTER"), []byte("ADDSLOTS"), []byte("7"),
	})
	if src.raftNode.FSM.Slot2Node[7] != src.SelfID() {
		t.Fatalf("precondition owner=%v", src.raftNode.FSM.Slot2Node[7])
	}

	r := execCluster(src, nil, [][]byte{
		[]byte("CLUSTER"), []byte("SETSLOT"), []byte("7"), []byte("MIGRATING"), []byte("127.0.0.1:7001"),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("MIGRATING: %s", r.ToBytes())
	}

	dst := "127.0.0.1:7001"
	r = execCluster(src, nil, [][]byte{
		[]byte("CLUSTER"), []byte("SETSLOT"), []byte("7"), []byte("NODE"), []byte(dst),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("NODE: %T %s", r, r.ToBytes())
	}
	if src.raftNode.FSM.Slot2Node[7] != dst {
		t.Fatalf("Slot2Node[7]=%q want %q", src.raftNode.FSM.Slot2Node[7], dst)
	}
	st := src.slotsManager.getSlot(7)
	st.mu.RLock()
	state, peer := st.state, st.migratePeer
	st.mu.RUnlock()
	if state != slotStateHosting || peer != "" {
		t.Fatalf("after NODE: state=%d peer=%q", state, peer)
	}

	// Idempotent re-assign to same owner.
	r = execCluster(src, nil, [][]byte{
		[]byte("CLUSTER"), []byte("SETSLOT"), []byte("7"), []byte("NODE"), []byte(dst),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("NODE idempotent: %s", r.ToBytes())
	}
}
