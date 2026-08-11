package core

import (
	"fmt"

	"github.com/linkerlin/godis/cluster/raft"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

const setReplicaCommand = "cluster.setreplica"

func init() {
	RegisterCmd(setReplicaCommand, execSetReplica)
}

// execClusterReplicate implements CLUSTER REPLICATE <master-node-id>.
// Godis path: EventJoin with Master → FSM MasterSlaves/SlaveMasters (not Redis gossip).
func execClusterReplicate(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if cluster == nil {
		return protocol.MakeErrReply("ERR This instance has cluster support disabled")
	}
	if cluster.raftNode == nil || cluster.raftNode.FSM == nil {
		return protocol.MakeErrReply("ERR CLUSTER REPLICATE requires Raft FSM")
	}
	masterID := string(cmdLine[2])
	self := cluster.SelfID()
	if masterID == self {
		return protocol.MakeErrReply("ERR Can't replicate myself")
	}

	var errMsg string
	var already bool
	cluster.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
		if _, ok := fsm.MasterSlaves[masterID]; !ok {
			errMsg = fmt.Sprintf("ERR Unknown node %s", masterID)
			return
		}
		if fsm.SlaveMasters[self] == masterID {
			already = true
			return
		}
		if len(fsm.Node2Slot[self]) > 0 {
			errMsg = "ERR To set a master the node must be empty and without assigned slots"
			return
		}
		if ms, ok := fsm.MasterSlaves[self]; ok && len(ms.Slaves) > 0 {
			errMsg = "ERR To set a master the node must be empty and without replicas"
			return
		}
	})
	if errMsg != "" {
		return protocol.MakeErrReply(errMsg)
	}
	if already {
		return protocol.MakeOkReply()
	}

	return execSetReplica(cluster, c, utils.ToCmdLine(setReplicaCommand, self, masterID))
}

// execSetReplica applies EventJoin{NodeId, Master} on the Raft leader / FSM-only node.
// format: cluster.setreplica replicaId masterId
func execSetReplica(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 3 {
		return protocol.MakeArgNumErrReply(setReplicaCommand)
	}
	if cluster == nil || cluster.raftNode == nil {
		return protocol.MakeErrReply("ERR CLUSTER REPLICATE requires Raft FSM")
	}
	replicaID := string(cmdLine[1])
	masterID := string(cmdLine[2])

	entry := &raft.LogEntry{
		Event: raft.EventJoin,
		JoinTask: &raft.JoinTask{
			NodeId: replicaID,
			Master: masterID,
		},
	}
	reply := cluster.proposeTopologyEntry(c, cmdLine, entry)
	if protocol.IsErrorReply(reply) {
		return reply
	}
	if cluster.db != nil && replicaID == cluster.SelfID() {
		if err := cluster.SlaveOf(masterID); err != nil {
			return protocol.MakeErrReply("ERR " + err.Error())
		}
	}
	return protocol.MakeOkReply()
}

// execClusterForget implements CLUSTER FORGET <node-id> as FSM topology cleanup.
// Not Redis gossip: no bus, no 60s re-meet ban. Safe path only (no slots / no replicas).
func execClusterForget(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if cluster == nil {
		return protocol.MakeErrReply("ERR This instance has cluster support disabled")
	}
	if cluster.raftNode == nil || cluster.raftNode.FSM == nil {
		return protocol.MakeErrReply("ERR CLUSTER FORGET requires Raft FSM")
	}
	nodeID := string(cmdLine[2])
	self := cluster.SelfID()
	if nodeID == self {
		return protocol.MakeErrReply("ERR I tried hard but I can't forget myself...")
	}

	var errMsg string
	cluster.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
		_, asMaster := fsm.MasterSlaves[nodeID]
		_, asSlave := fsm.SlaveMasters[nodeID]
		if !asMaster && !asSlave {
			errMsg = fmt.Sprintf("ERR Unknown node %s", nodeID)
			return
		}
		if len(fsm.Node2Slot[nodeID]) > 0 {
			errMsg = "ERR Can't forget a node with assigned slots"
			return
		}
		if ms, ok := fsm.MasterSlaves[nodeID]; ok && len(ms.Slaves) > 0 {
			errMsg = "ERR Can't forget a master with replicas"
			return
		}
	})
	if errMsg != "" {
		return protocol.MakeErrReply(errMsg)
	}

	entry := &raft.LogEntry{
		Event:      raft.EventForget,
		ForgetTask: &raft.ForgetTask{NodeId: nodeID},
	}
	reply := cluster.proposeTopologyEntry(c, cmdLine, entry)
	if protocol.IsErrorReply(reply) {
		return reply
	}
	if cluster.raftNode.RaftReady() && cluster.raftNode.State() == raft.Leader {
		_ = cluster.raftNode.HandleEvict(nodeID)
	}
	return protocol.MakeOkReply()
}

// proposeTopologyEntry Propose on Raft leader, or ApplyLocal for FSM-only stubs.
func (cluster *Cluster) proposeTopologyEntry(c redis.Connection, cmdLine CmdLine, entry *raft.LogEntry) redis.Reply {
	if cluster.raftNode.RaftReady() {
		if cluster.raftNode.State() != raft.Leader {
			leaderConn, err := cluster.BorrowLeaderClient()
			if err != nil {
				return protocol.MakeErrReply(err.Error())
			}
			defer cluster.connections.ReturnPeerClient(leaderConn)
			return leaderConn.Send(cmdLine)
		}
		if _, err := cluster.raftNode.Propose(entry); err != nil {
			return protocol.MakeErrReply("ERR " + err.Error())
		}
		return protocol.MakeOkReply()
	}
	if cluster.raftNode.FSM == nil {
		return protocol.MakeErrReply("ERR Raft FSM unavailable")
	}
	cluster.raftNode.ApplyLocal(entry)
	if entry.Event == raft.EventForget && entry.ForgetTask != nil {
		id := entry.ForgetTask.NodeId
		var stillKnown bool
		cluster.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
			_, m := fsm.MasterSlaves[id]
			_, s := fsm.SlaveMasters[id]
			stillKnown = m || s
		})
		if stillKnown {
			return protocol.MakeErrReply(fmt.Sprintf("ERR Failed to forget node %s", id))
		}
	}
	if entry.Event == raft.EventJoin && entry.JoinTask != nil && entry.JoinTask.Master != "" {
		id, master := entry.JoinTask.NodeId, entry.JoinTask.Master
		var ok bool
		cluster.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
			ok = fsm.SlaveMasters[id] == master
		})
		if !ok {
			return protocol.MakeErrReply("ERR Failed to set replica topology")
		}
	}
	return protocol.MakeOkReply()
}
