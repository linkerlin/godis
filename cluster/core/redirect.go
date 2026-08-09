package core

import (
	"github.com/linkerlin/godis/cluster/raft"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// clusterConn is implemented by *connection.Connection / FakeConn.
type clusterConn interface {
	SetClusterReadOnly(bool)
	IsClusterReadOnly() bool
	SetAsking(bool)
	IsAsking() bool
	ConsumeAsking() bool
}

func asClusterConn(c redis.Connection) clusterConn {
	if cc, ok := c.(clusterConn); ok {
		return cc
	}
	return nil
}

func execClusterAsking(_ *Cluster, c redis.Connection, _ CmdLine) redis.Reply {
	if cc := asClusterConn(c); cc != nil {
		cc.SetAsking(true)
	}
	return protocol.MakeOkReply()
}

func execClusterReadonly(_ *Cluster, c redis.Connection, _ CmdLine) redis.Reply {
	if cc := asClusterConn(c); cc != nil {
		cc.SetClusterReadOnly(true)
	}
	return protocol.MakeOkReply()
}

func execClusterReadwrite(_ *Cluster, c redis.Connection, _ CmdLine) redis.Reply {
	if cc := asClusterConn(c); cc != nil {
		cc.SetClusterReadOnly(false)
	}
	return protocol.MakeOkReply()
}

func init() {
	RegisterCmd("asking", execClusterAsking)
	RegisterCmd("readonly", execClusterReadonly)
	RegisterCmd("readwrite", execClusterReadwrite)
}

// migrationTargetForSlot returns the importing node address when this slot is mid-migration.
// Prefers Raft FSM Migratings; falls back to local SETSLOT MIGRATING migratePeer.
func (cluster *Cluster) migrationTargetForSlot(slot uint32) string {
	if cluster.raftNode != nil && cluster.raftNode.FSM != nil {
		var target string
		cluster.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
			for _, task := range fsm.Migratings {
				if task == nil {
					continue
				}
				for _, s := range task.Slots {
					if s == slot {
						target = task.TargetNode
						return
					}
				}
			}
		})
		if target != "" {
			return target
		}
	}
	// Prefer FSM Migratings; else local SETSLOT MIGRATING migratePeer (even if state
	// was cleared early — peer alone is enough when still mid-admin migrate).
	if cluster.slotsManager != nil {
		st := cluster.slotsManager.getSlot(slot)
		st.mu.RLock()
		peer, state := st.migratePeer, st.state
		st.mu.RUnlock()
		if peer != "" && (state == slotStateExporting || state == slotStateImporting) {
			return peer
		}
	}
	return ""
}

// keyExistsLocal reports whether key is present on the local DB engine.
func (cluster *Cluster) keyExistsLocal(key string) bool {
	_, ok, _ := cluster.db.GetEntity(0, key)
	return ok
}
