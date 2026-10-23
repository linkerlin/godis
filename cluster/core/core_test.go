package core

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/linkerlin/godis/cluster/raft"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
)

// testAddrs hands out free ports so cluster tests never collide on the fixed
// ports older revisions used (shuffle/count>1 runs dialed a previous test's
// still-listening cluster and failed to join).
func testAddrs(t *testing.T) (redisAddr, raftAddr string) {
	t.Helper()
	p1 := freeTestPort(t)
	p2 := freeTestPort(t)
	return fmt.Sprintf("127.0.0.1:%d", p1), fmt.Sprintf("127.0.0.1:%d", p2)
}

func freeTestPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return p
}

func TestClusterBootstrap(t *testing.T) {
	skipHeavyTests(t)
	// start leader
	leaderDir := t.TempDir()

	RegisterDefaultCmd("get")
	RegisterDefaultCmd("set")

	// connection stub
	connections := NewInMemConnectionFactory()
	leaderRedis, leaderRaft := testAddrs(t)
	leaderCfg := &Config{
		RaftConfig: raft.RaftConfig{
			RedisAdvertiseAddr: leaderRedis,
			RaftListenAddr:     leaderRaft,
			RaftAdvertiseAddr:  leaderRaft,
			Dir:                leaderDir,
		},
		StartAsSeed:    true,
		connectionStub: connections,
		noCron:         true,
	}
	leader, err := NewCluster(leaderCfg)
	if err != nil {
		t.Error(err)
		return
	}
	connections.nodes[leaderCfg.RedisAdvertiseAddr] = leader

	// set key-values for test
	testEntries := make(map[string]string)
	c := connection.NewFakeConn()
	for i := 0; i < 1000; i++ {
		key := utils.RandString(10)
		value := utils.RandString(10)
		testEntries[key] = value
		result := leader.Exec(c, utils.ToCmdLine("set", key, value))
		if !protocol.IsOKReply(result) {
			t.Errorf("command [set] failed: %s", string(result.ToBytes()))
			return
		}
	}

	// start follower
	followerDir := t.TempDir()
	followerRedis, followerRaft := testAddrs(t)
	followerCfg := &Config{
		RaftConfig: raft.RaftConfig{
			RedisAdvertiseAddr: followerRedis,
			RaftListenAddr:     followerRaft,
			RaftAdvertiseAddr:  followerRaft,
			Dir:                followerDir,
		},
		StartAsSeed:    false,
		JoinAddress:    leaderCfg.RedisAdvertiseAddr,
		connectionStub: connections,
		noCron:         true,
	}
	follower, err := NewCluster(followerCfg)
	if err != nil {
		t.Error(err)
		return
	}
	connections.nodes[followerCfg.RedisAdvertiseAddr] = follower

	_ = follower.SelfID()
	// check nodes
	joined := false
	for i := 0; i < 10; i++ {
		nodes, err := leader.raftNode.GetNodes()
		if err != nil {
			t.Log(err)
			continue
		}
		if len(nodes) == 2 {
			t.Log("join success")
			joined = true
			break
		}
		time.Sleep(time.Second)
	}
	if !joined {
		t.Error("join failed")
		return
	}

	// rebalance
	leader.doRebalance()
	time.Sleep(2 * time.Second)
	// Wait for BOTH FSM views to converge. Checking only the leader's FSM
	// races the final route-change propagation: for a moment the follower
	// still routes a just-migrated slot back to the leader, and the GET loop
	// below then receives a MOVED reply instead of a bulk (nightly -race hit
	// exactly that window).
	converged := false
	for i := 0; i < 1000 && !converged; i++ {
		converged = true
		for _, node := range []*Cluster{leader, follower} {
			nodeOK := false
			node.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
				leaderSlots := len(fsm.Node2Slot[leaderCfg.RedisAdvertiseAddr])
				followerSlots := len(fsm.Node2Slot[followerCfg.RedisAdvertiseAddr])
				if len(fsm.Migratings) == 0 && leaderSlots > 0 && followerSlots > 0 {
					nodeOK = true
				}
			})
			if !nodeOK {
				converged = false
			}
		}
		if !converged {
			time.Sleep(time.Second)
		}
	}
	if !converged {
		t.Error("rebalance did not converge on both nodes")
		return
	}
	t.Log("rebalance success")

	// set key-values for test
	for key, value := range testEntries {
		c := connection.NewFakeConn()
		result := leader.Exec(c, utils.ToCmdLine("get", key))
		result2, ok := result.(*protocol.BulkReply)
		if !ok {
			t.Errorf("command [get] failed: %s", string(result.ToBytes()))
			return
		}
		if string(result2.Arg) != value {
			t.Errorf("command [get] failed: %s", string(result.ToBytes()))
			return
		}
	}
}

func TestFailover(t *testing.T) {
	skipHeavyTests(t)
	// start leader
	leaderDir := t.TempDir()
	RegisterCmd("slaveof", func(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
		return protocol.MakeOkReply()
	})

	// connection stub
	connections := NewInMemConnectionFactory()
	leaderRedis, leaderRaft := testAddrs(t)
	leaderCfg := &Config{
		RaftConfig: raft.RaftConfig{
			RedisAdvertiseAddr: leaderRedis,
			RaftListenAddr:     leaderRaft,
			RaftAdvertiseAddr:  leaderRaft,
			Dir:                leaderDir,
		},
		StartAsSeed:    true,
		connectionStub: connections,
		noCron:         true,
	}
	leader, err := NewCluster(leaderCfg)
	if err != nil {
		t.Error(err)
		return
	}
	connections.nodes[leaderCfg.RedisAdvertiseAddr] = leader

	// start follower
	followerDir := t.TempDir()
	followerRedis, followerRaft := testAddrs(t)
	followerCfg := &Config{
		RaftConfig: raft.RaftConfig{
			RedisAdvertiseAddr: followerRedis,
			RaftListenAddr:     followerRaft,
			RaftAdvertiseAddr:  followerRaft,
			Dir:                followerDir,
		},
		StartAsSeed:    false,
		JoinAddress:    leaderCfg.RedisAdvertiseAddr,
		connectionStub: connections,
		noCron:         true,
		Master:         leader.SelfID(),
	}
	follower, err := NewCluster(followerCfg)
	if err != nil {
		t.Error(err)
		return
	}
	connections.nodes[followerCfg.RedisAdvertiseAddr] = follower

	_ = follower.SelfID()
	// check nodes
	joined := false
	for i := 0; i < 10; i++ {
		nodes, err := leader.raftNode.GetNodes()
		if err != nil {
			t.Log(err)
			continue
		}
		if len(nodes) == 2 {
			t.Log("join success")
			joined = true
			break
		}
		time.Sleep(time.Second)
	}
	if !joined {
		t.Error("join failed")
		return
	}

	// rebalance
	leader.replicaManager.masterHeartbeats[leader.SelfID()] = time.Now().Add(-time.Hour)
	leader.doFailoverCheck()
	time.Sleep(2 * time.Second)
	for i := 0; i < 1000; i++ {
		success := false
		leader.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
			ms := fsm.MasterSlaves[follower.SelfID()]
			if ms != nil && len(ms.Slaves) > 0 {
				success = true
			}
		})
		if success {
			t.Log("rebalance success")
			break
		} else {
			time.Sleep(time.Second)
		}
	}
}
