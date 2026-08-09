package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/cluster/raft"
	"github.com/linkerlin/godis/redis/protocol"
)

func TestClusterGetKeysInSlotReturnsLocalKeys(t *testing.T) {
	cl := &Cluster{id_: "node-gk", slotsManager: newSlotsManager()}
	cl.slotsManager.getSlot(3).keys.Add("alpha")
	cl.slotsManager.getSlot(3).keys.Add("beta")

	r := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("GETKEYSINSLOT"), []byte("3"), []byte("10"),
	})
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mb.Args) != 2 {
		t.Fatalf("GETKEYSINSLOT: %T %s", r, r.ToBytes())
	}
	seen := map[string]bool{}
	for _, a := range mb.Args {
		seen[string(a)] = true
	}
	if !seen["alpha"] || !seen["beta"] {
		t.Fatalf("keys=%v", seen)
	}

	lim := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("GETKEYSINSLOT"), []byte("3"), []byte("1"),
	})
	mb, ok = lim.(*protocol.MultiBulkReply)
	if !ok || len(mb.Args) != 1 {
		t.Fatalf("count=1: %T %s", lim, lim.ToBytes())
	}

	empty := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("GETKEYSINSLOT"), []byte("9"), []byte("5"),
	})
	if _, ok := empty.(*protocol.EmptyMultiBulkReply); !ok {
		if mb2, ok2 := empty.(*protocol.MultiBulkReply); !ok2 || len(mb2.Args) != 0 {
			t.Fatalf("empty slot: %T %s", empty, empty.ToBytes())
		}
	}
}

func TestClusterReplicasFromFSM(t *testing.T) {
	master := "127.0.0.1:7000"
	slave := "127.0.0.1:7001"
	fsm := &raft.FSM{
		Node2Slot: map[string][]uint32{
			master: {0, 1},
		},
		Slot2Node: map[uint32]string{0: master, 1: master},
		MasterSlaves: map[string]*raft.MasterSlave{
			master: {MasterId: master, Slaves: []string{slave}},
		},
		SlaveMasters: map[string]string{slave: master},
		Migratings:   make(map[string]*raft.MigratingTask),
		Failovers:    make(map[string]*raft.FailoverTask),
	}
	cl := &Cluster{
		id_: master,
		raftNode: &raft.Node{
			Cfg: &raft.RaftConfig{RedisAdvertiseAddr: master},
			FSM: fsm,
		},
	}

	r := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("REPLICAS"), []byte(master)})
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mb.Args) != 1 {
		t.Fatalf("REPLICAS: %T %s", r, r.ToBytes())
	}
	line := string(mb.Args[0])
	if !strings.Contains(line, slave) || !strings.Contains(line, "slave") {
		t.Fatalf("replica line: %s", line)
	}

	unk := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("REPLICAS"), []byte("no-such-node")})
	if !protocol.IsErrorReply(unk) || !strings.Contains(string(unk.ToBytes()), "Unknown node") {
		t.Fatalf("unknown: %s", unk.ToBytes())
	}
}
