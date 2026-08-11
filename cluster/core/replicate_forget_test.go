package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/cluster/raft"
	"github.com/linkerlin/godis/redis/protocol"
)

func TestClusterReplicateAndForgetFSM(t *testing.T) {
	master := "127.0.0.1:7000"
	replica := "127.0.0.1:7001"
	cl := newFSMCluster(replica)
	cl.raftNode.ApplyLocal(&raft.LogEntry{
		Event:    raft.EventJoin,
		JoinTask: &raft.JoinTask{NodeId: master},
	})
	cl.raftNode.ApplyLocal(&raft.LogEntry{
		Event:    raft.EventJoin,
		JoinTask: &raft.JoinTask{NodeId: replica},
	})

	r := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("REPLICATE"), []byte(master),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("REPLICATE: %T %s", r, r.ToBytes())
	}
	if cl.raftNode.FSM.SlaveMasters[replica] != master {
		t.Fatalf("SlaveMasters=%v", cl.raftNode.FSM.SlaveMasters)
	}
	ms := cl.raftNode.FSM.MasterSlaves[master]
	if ms == nil || len(ms.Slaves) != 1 || ms.Slaves[0] != replica {
		t.Fatalf("MasterSlaves=%v", cl.raftNode.FSM.MasterSlaves)
	}
	if _, stillMaster := cl.raftNode.FSM.MasterSlaves[replica]; stillMaster {
		t.Fatalf("replica should be demoted from MasterSlaves")
	}

	// idempotent
	r = execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("REPLICATE"), []byte(master),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("REPLICATE again: %s", r.ToBytes())
	}

	self := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("REPLICATE"), []byte(replica),
	})
	if !protocol.IsErrorReply(self) || !strings.Contains(string(self.ToBytes()), "myself") {
		t.Fatalf("self replicate: %s", self.ToBytes())
	}

	// FORGET self is forbidden
	r = execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("FORGET"), []byte(replica),
	})
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "forget myself") {
		t.Fatalf("FORGET self: %s", r.ToBytes())
	}

	// From master node: forget its replica (safe path)
	mcl := newFSMCluster(master)
	mcl.raftNode.FSM = cl.raftNode.FSM
	mcl.raftNode.Cfg = &raft.RaftConfig{RedisAdvertiseAddr: master}
	r = execCluster(mcl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("FORGET"), []byte(replica),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("FORGET replica: %T %s", r, r.ToBytes())
	}
	if _, ok := mcl.raftNode.FSM.SlaveMasters[replica]; ok {
		t.Fatalf("replica still in SlaveMasters")
	}

	// empty master forget
	extra := "127.0.0.1:7002"
	mcl.raftNode.ApplyLocal(&raft.LogEntry{
		Event:    raft.EventJoin,
		JoinTask: &raft.JoinTask{NodeId: extra},
	})
	r = execCluster(mcl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("FORGET"), []byte(extra),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("FORGET empty master: %T %s", r, r.ToBytes())
	}

	unknown := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("FORGET"), []byte("no-such-node"),
	})
	if !protocol.IsErrorReply(unknown) || !strings.Contains(string(unknown.ToBytes()), "Unknown node") {
		t.Fatalf("unknown: %s", unknown.ToBytes())
	}
}

func TestClusterReplicateRejectsSlots(t *testing.T) {
	master := "127.0.0.1:7100"
	self := "127.0.0.1:7101"
	cl := newFSMCluster(self)
	cl.raftNode.ApplyLocal(&raft.LogEntry{
		Event:    raft.EventJoin,
		JoinTask: &raft.JoinTask{NodeId: master},
	})
	cl.raftNode.ApplyLocal(&raft.LogEntry{
		Event:    raft.EventJoin,
		JoinTask: &raft.JoinTask{NodeId: self},
	})
	cl.raftNode.ApplyLocal(&raft.LogEntry{
		Event: raft.EventAddSlots,
		SlotsTask: &raft.SlotsTask{
			NodeId: self,
			Slots:  []uint32{1},
		},
	})
	r := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("REPLICATE"), []byte(master),
	})
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "slots") {
		t.Fatalf("want slots err: %s", r.ToBytes())
	}
}

func TestClusterBusStatsFromHeartbeatAndMeet(t *testing.T) {
	cl := newFSMCluster("127.0.0.1:7200")
	cl.raftNode.ApplyLocal(&raft.LogEntry{
		Event:    raft.EventJoin,
		JoinTask: &raft.JoinTask{NodeId: "127.0.0.1:7200"},
	})
	r := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("MEET"), []byte("127.0.0.1"), []byte("7201"),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("MEET: %s", r.ToBytes())
	}
	// fake heartbeat recv
	_ = execHeartbeat(cl, nil, [][]byte{[]byte("cluster.heartbeat"), []byte("peer")})
	info := string(execClusterInfo(cl).(*protocol.BulkReply).Arg)
	if !strings.Contains(info, "cluster_bus_port:0\n") {
		t.Fatalf("bus_port must stay 0: %s", info)
	}
	if !strings.Contains(info, "cluster_stats_messages_meet_sent:1\n") {
		t.Fatalf("meet_sent: %s", info)
	}
	// FSM-only CLUSTER MEET is initiator-side only; no peer cluster.join recv.
	if !strings.Contains(info, "cluster_stats_messages_meet_received:0\n") {
		t.Fatalf("meet_received want 0 on initiator FSM MEET: %s", info)
	}
	if !strings.Contains(info, "cluster_stats_messages_ping_received:1\n") {
		t.Fatalf("ping_received: %s", info)
	}
	if !strings.Contains(info, "cluster_stats_messages_pong_sent:1\n") {
		t.Fatalf("pong_sent: %s", info)
	}
}

// TestClusterBusStatsMeetReceived: peer RPC cluster.join locally applied bumps
// meet_received (CLUSTER INFO seam); still not a Redis gossip bus.
func TestClusterBusStatsMeetReceived(t *testing.T) {
	cl := newFSMCluster("127.0.0.1:7300")
	cl.raftNode.ApplyLocal(&raft.LogEntry{
		Event:    raft.EventJoin,
		JoinTask: &raft.JoinTask{NodeId: "127.0.0.1:7300"},
	})
	r := execJoin(cl, nil, [][]byte{
		[]byte("cluster.join"), []byte("127.0.0.1:7301"), []byte("127.0.0.1:17301"),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("cluster.join: %s", r.ToBytes())
	}
	if _, known := cl.raftNode.FSM.MasterSlaves["127.0.0.1:7301"]; !known {
		t.Fatalf("join not in FSM MasterSlaves: %+v", cl.raftNode.FSM.MasterSlaves)
	}
	info := string(execClusterInfo(cl).(*protocol.BulkReply).Arg)
	if !strings.Contains(info, "cluster_bus_port:0\n") {
		t.Fatalf("bus_port must stay 0: %s", info)
	}
	if !strings.Contains(info, "cluster_stats_messages_meet_received:1\n") {
		t.Fatalf("meet_received: %s", info)
	}
	if !strings.Contains(info, "cluster_stats_messages_meet_sent:0\n") {
		t.Fatalf("meet_sent must stay 0 on peer recv: %s", info)
	}
	if !strings.Contains(info, "cluster_stats_messages_received:1\n") {
		t.Fatalf("messages_received should include meet: %s", info)
	}
}

func TestClusterReplicateRequiresFSM(t *testing.T) {
	cl := &Cluster{id_: "node-no-fsm"}
	r := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("REPLICATE"), []byte("master-id"),
	})
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "requires Raft FSM") {
		t.Fatalf("want requires FSM: %s", r.ToBytes())
	}
}
