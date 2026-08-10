package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/cluster/raft"
	"github.com/linkerlin/godis/redis/protocol"
)

func TestSlotRangePairs(t *testing.T) {
	got := slotRangePairs([]uint32{0, 1, 2, 5, 7, 8})
	want := [][2]uint32{{0, 2}, {5, 5}, {7, 8}}
	if len(got) != len(want) {
		t.Fatalf("len=%d want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("i=%d got=%v want=%v", i, got[i], want[i])
		}
	}
}

func TestClusterTopologyFromFSM(t *testing.T) {
	a := "127.0.0.1:7000"
	b := "127.0.0.1:7001"
	fsm := &raft.FSM{
		Node2Slot: map[string][]uint32{
			a: {0, 1, 2},
			b: {100, 101},
		},
		Slot2Node: map[uint32]string{
			0: a, 1: a, 2: a, 100: b, 101: b,
		},
		MasterSlaves: map[string]*raft.MasterSlave{
			a: {MasterId: a, Slaves: []string{b + "-replica"}},
		},
		SlaveMasters: map[string]string{
			b + "-replica": a,
		},
		Migratings: make(map[string]*raft.MigratingTask),
		Failovers:  make(map[string]*raft.FailoverTask),
	}
	cl := &Cluster{
		id_: a,
		raftNode: &raft.Node{
			Cfg: &raft.RaftConfig{RedisAdvertiseAddr: a},
			FSM: fsm,
		},
	}

	nodes := string(execClusterNodes(cl).(*protocol.BulkReply).Arg)
	if !strings.Contains(nodes, a) || !strings.Contains(nodes, "0-2") {
		t.Fatalf("NODES missing a/slots: %s", nodes)
	}
	if !strings.Contains(nodes, b) || !strings.Contains(nodes, "100-101") {
		t.Fatalf("NODES missing b/slots: %s", nodes)
	}
	if !strings.Contains(nodes, "myself,master") {
		t.Fatalf("NODES flags: %s", nodes)
	}
	if !strings.Contains(nodes, "slave") || !strings.Contains(nodes, a+" ") {
		// replica line should reference master id
		if !strings.Contains(nodes, b+"-replica") {
			t.Fatalf("NODES missing replica: %s", nodes)
		}
	}

	info := string(execClusterInfo(cl).(*protocol.BulkReply).Arg)
	if !strings.Contains(info, "cluster_slots_assigned:5\n") {
		t.Fatalf("INFO assigned: %s", info)
	}
	if !strings.Contains(info, "cluster_known_nodes:3\n") {
		t.Fatalf("INFO known: %s", info)
	}
	if !strings.Contains(info, "cluster_stats_messages_ping_sent:0\n") {
		t.Fatalf("INFO missing gossip ping_sent zero: %s", info)
	}

	slots := execClusterSlots(cl)
	multi, ok := slots.(*protocol.MultiRawReply)
	if !ok || len(multi.Replies) < 2 {
		t.Fatalf("SLOTS: %T %s", slots, slots.ToBytes())
	}

	shards := execClusterShards(cl)
	sm, ok := shards.(*protocol.MultiRawReply)
	if !ok || len(sm.Replies) != 2 {
		t.Fatalf("SHARDS want 2 shards, got %T %s", shards, shards.ToBytes())
	}
}

func TestClusterTopologyFallbackNoFSM(t *testing.T) {
	cl := &Cluster{id_: "127.0.0.1:6399"}
	nodes := string(execClusterNodes(cl).(*protocol.BulkReply).Arg)
	if !strings.Contains(nodes, "127.0.0.1:6399") || !strings.Contains(nodes, "0-16383") {
		t.Fatalf("fallback NODES: %s", nodes)
	}
	info := string(execClusterInfo(cl).(*protocol.BulkReply).Arg)
	if !strings.Contains(info, "cluster_slots_assigned:16384\n") {
		t.Fatalf("fallback INFO: %s", info)
	}
}
