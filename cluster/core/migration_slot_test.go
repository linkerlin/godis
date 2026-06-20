package core

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/linkerlin/godis/cluster/raft"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
)

func TestDoMigrateSlot(t *testing.T) {
	skipHeavyTests(t)

	leaderDir := "test/migrate/0"
	os.RemoveAll(leaderDir)
	os.MkdirAll(leaderDir, 0777)
	defer os.RemoveAll(leaderDir)

	followerDir := "test/migrate/1"
	os.RemoveAll(followerDir)
	os.MkdirAll(followerDir, 0777)
	defer os.RemoveAll(followerDir)

	RegisterDefaultCmd("get")
	RegisterDefaultCmd("set")

	connections := NewInMemConnectionFactory()
	leaderCfg := &Config{
		RaftConfig: raft.RaftConfig{
			RedisAdvertiseAddr: "127.0.0.1:7399",
			RaftListenAddr:     "127.0.0.1:17666",
			RaftAdvertiseAddr:  "127.0.0.1:17666",
			Dir:                leaderDir,
		},
		StartAsSeed:    true,
		connectionStub: connections,
		noCron:         true,
	}
	leader, err := NewCluster(leaderCfg)
	if err != nil {
		t.Fatal(err)
	}
	connections.nodes[leaderCfg.RedisAdvertiseAddr] = leader

	followerCfg := &Config{
		RaftConfig: raft.RaftConfig{
			RedisAdvertiseAddr: "127.0.0.1:7499",
			RaftListenAddr:     "127.0.0.1:17667",
			RaftAdvertiseAddr:  "127.0.0.1:17667",
			Dir:                followerDir,
		},
		StartAsSeed:    false,
		JoinAddress:    leaderCfg.RedisAdvertiseAddr,
		connectionStub: connections,
		noCron:         true,
	}
	follower, err := NewCluster(followerCfg)
	if err != nil {
		t.Fatal(err)
	}
	connections.nodes[followerCfg.RedisAdvertiseAddr] = follower

	joined := false
	for i := 0; i < 10; i++ {
		nodes, err := leader.raftNode.GetNodes()
		if err == nil && len(nodes) == 2 {
			joined = true
			break
		}
		time.Sleep(time.Second)
	}
	if !joined {
		t.Fatal("cluster join failed")
	}

	var slotToMove uint32
	var foundSlot bool
	leader.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
		slots := fsm.Node2Slot[leaderCfg.RedisAdvertiseAddr]
		if len(slots) > 0 {
			slotToMove = slots[0]
			foundSlot = true
		}
	})
	if !foundSlot {
		t.Fatal("could not find a slot owned by leader")
	}

	key, value := keyValueForSlot(leader, slotToMove)
	c := connection.NewFakeConn()
	ret := leader.Exec(c, utils.ToCmdLine("set", key, value))
	if !protocol.IsOKReply(ret) {
		t.Fatalf("set failed: %s", string(ret.ToBytes()))
	}

	err = leader.doMigrateSlot(slotToMove, leaderCfg.RedisAdvertiseAddr, followerCfg.RedisAdvertiseAddr)
	if err != nil {
		t.Fatalf("doMigrateSlot failed: %v", err)
	}

	if owner := leader.PickNode(slotToMove); owner != followerCfg.RedisAdvertiseAddr {
		t.Fatalf("slot %d owner = %s, want %s", slotToMove, owner, followerCfg.RedisAdvertiseAddr)
	}

	ret = leader.Exec(c, utils.ToCmdLine("get", key))
	bulk, ok := ret.(*protocol.BulkReply)
	if !ok || string(bulk.Arg) != value {
		t.Fatalf("get after migrate = %v, want %q", ret, value)
	}
}

func TestDoMigrateSlotRejectsWrongOwner(t *testing.T) {
	skipHeavyTests(t)

	dir := "test/migrate/wrong-owner"
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0777)
	defer os.RemoveAll(dir)

	connections := NewInMemConnectionFactory()
	cfg := &Config{
		RaftConfig: raft.RaftConfig{
			RedisAdvertiseAddr: "127.0.0.1:7599",
			RaftListenAddr:     "127.0.0.1:18666",
			RaftAdvertiseAddr:  "127.0.0.1:18666",
			Dir:                dir,
		},
		StartAsSeed:    true,
		connectionStub: connections,
		noCron:         true,
	}
	leader, err := NewCluster(cfg)
	if err != nil {
		t.Fatal(err)
	}
	connections.nodes[cfg.RedisAdvertiseAddr] = leader

	err = leader.doMigrateSlot(0, "127.0.0.1:9999", "127.0.0.1:8888")
	if err == nil {
		t.Fatal("expected error for wrong source node")
	}
}

func keyValueForSlot(cluster *Cluster, slot uint32) (string, string) {
	value := utils.RandString(8)
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("migkey-%d", i)
		if cluster.GetSlot(key) == slot {
			return key, value
		}
	}
	return fmt.Sprintf("slot-%d", slot), value
}
