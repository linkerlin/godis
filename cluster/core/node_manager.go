package core

import (
	"fmt"
	"math"
	"net"
	"strconv"
	"time"

	"github.com/linkerlin/godis/cluster/raft"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

/*

**Rebalance Procedure**
1. Invoke `triggerMigrationTask` on cluster Leader to start a migration task
2. Leader propose EventStartMigrate to raft group then send startMigrationCommand to TargetNode. (at `triggerMigrationTask`)

3. Target Node runs `doImports` after receiving startMigrationCommand
4. Target Node send exportCommand to Source Node.

6. Source Node get migrating task from raft (at `execExport`)
7. SourceNode set task into slotManager to start recording dirty keys during migration. (at `injectInsertCallback`)
8. Source Node dump old data to Target Node

9. Target node send migrationDoneCommand to Source Node. (at `doImports`)
10. Source Node runs `execFinishExport`, lock slots to stop writing
11. Source Node send dirty keys to Target Node

12. Source Node send migrationChangeRouteCommand to Leader
13. Leader proposes EventFinishMigrate to raft and waits Source Node and Target Node receives this entry(at `execMigrationChangeRoute`)
14. Source Node finish exporting, unlock slots, clean data
15. Target Node finish importing, unlock slots, start serve
*/

const joinClusterCommand = "cluster.join"
const migrationChangeRouteCommand = "cluster.migration.changeroute"

func init() {
	RegisterCmd(joinClusterCommand, execJoin)
	RegisterCmd(migrationChangeRouteCommand, execMigrationChangeRoute)
}

// doClusterJoin applies a topology join (AddToRaft+EventJoin, or FSM-only ApplyLocal).
// locallyApplied is true when this node performed the join; false when forwarded to leader.
// Callers own CLUSTER INFO bus counters (meet_sent vs meet_received).
func doClusterJoin(cluster *Cluster, redisAddr, raftAddr, master string) (reply redis.Reply, locallyApplied bool) {
	if cluster == nil || cluster.raftNode == nil {
		return protocol.MakeErrReply("ERR This instance has cluster support disabled"), false
	}

	// FSM-only stubs (unit tests): topology join without Hashicorp Raft.
	if !cluster.raftNode.RaftReady() {
		if cluster.raftNode.FSM == nil {
			return protocol.MakeErrReply("ERR cluster.join requires Raft FSM"), false
		}
		cluster.raftNode.ApplyLocal(&raft.LogEntry{
			Event: raft.EventJoin,
			JoinTask: &raft.JoinTask{
				NodeId: redisAddr,
				Master: master,
			},
		})
		return protocol.MakeOkReply(), true
	}

	state := cluster.raftNode.State()
	if state != raft.Leader {
		leaderConn, err := cluster.BorrowLeaderClient()
		if err != nil {
			return protocol.MakeErrReply(err.Error()), false
		}
		defer cluster.connections.ReturnPeerClient(leaderConn)
		cmd := utils.ToCmdLine(joinClusterCommand, redisAddr, raftAddr)
		if master != "" {
			cmd = append(cmd, []byte(master))
		}
		return leaderConn.Send(cmd), false
	}

	err := cluster.raftNode.AddToRaft(redisAddr, raftAddr)
	if err != nil {
		return protocol.MakeErrReply(err.Error()), false
	}
	_, err = cluster.raftNode.Propose(&raft.LogEntry{
		Event: raft.EventJoin,
		JoinTask: &raft.JoinTask{
			NodeId: redisAddr,
			Master: master,
		},
	})
	if err != nil {
		// todo: remove the node from raft
		return protocol.MakeErrReply(err.Error()), false
	}
	return protocol.MakeOkReply(), true
}

// execJoin handles peer RPC cluster.join (Raft path used by CLUSTER MEET).
// format: cluster.join redisAddress(advertised) raftAddress [masterId]
// Local success maps to CLUSTER INFO meet_received (not gossip bus frames).
func execJoin(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeArgNumErrReply(joinClusterCommand)
	}
	redisAddr := string(cmdLine[1])
	raftAddr := string(cmdLine[2])
	master := ""
	if len(cmdLine) >= 4 {
		master = string(cmdLine[3])
	}
	reply, local := doClusterJoin(cluster, redisAddr, raftAddr, master)
	if local && !protocol.IsErrorReply(reply) {
		cluster.bus.incrMeetReceived()
	}
	return reply
}

// execClusterMeet is the user-facing CLUSTER MEET ip port [raft-port].
//
// Godis is Raft/advertise based — this is NOT Redis Cluster gossip handshake.
// When Hashicorp Raft is ready, optional raft-port is required and the request
// follows the same AddToRaft + EventJoin / leader-forward path as cluster.join.
// With FSM-only stubs (unit tests), EventJoin is applied locally without AddToRaft.
func execClusterMeet(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 4 && len(cmdLine) != 5 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|meet' command")
	}
	if cluster == nil {
		return protocol.MakeErrReply("ERR This instance has cluster support disabled")
	}
	if cluster.raftNode == nil {
		return protocol.MakeErrReply("ERR CLUSTER MEET requires Raft")
	}

	ip := string(cmdLine[2])
	if ip == "" {
		return protocol.MakeErrReply("ERR Invalid node address specified")
	}
	port, err := strconv.ParseInt(string(cmdLine[3]), 10, 64)
	if err != nil || port <= 0 || port > 65535 {
		return protocol.MakeErrReply("ERR Invalid TCP base port specified: " + string(cmdLine[3]))
	}
	redisAddr := net.JoinHostPort(ip, strconv.FormatInt(port, 10))

	raftAddr := ""
	if len(cmdLine) == 5 {
		raftPort, err := strconv.ParseInt(string(cmdLine[4]), 10, 64)
		if err != nil || raftPort <= 0 || raftPort > 65535 {
			return protocol.MakeErrReply("ERR Invalid raft port specified: " + string(cmdLine[4]))
		}
		raftAddr = net.JoinHostPort(ip, strconv.FormatInt(raftPort, 10))
	}

	// Already in FSM topology → idempotent OK (no gossip ping).
	if cluster.raftNode.FSM != nil {
		var known bool
		cluster.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
			_, known = fsm.MasterSlaves[redisAddr]
			if !known {
				_, known = fsm.SlaveMasters[redisAddr]
			}
		})
		if known {
			return protocol.MakeOkReply()
		}
	}

	if cluster.raftNode.RaftReady() {
		if raftAddr == "" {
			return protocol.MakeErrReply("ERR CLUSTER MEET requires raft-port (Godis uses Raft advertise, not gossip). Usage: CLUSTER MEET ip port raft-port")
		}
		// Initiator: meet_sent only. Peer accepting cluster.join bumps meet_received.
		reply, _ := doClusterJoin(cluster, redisAddr, raftAddr, "")
		if !protocol.IsErrorReply(reply) {
			cluster.bus.incrMeetSent()
		}
		return reply
	}

	// FSM-only (no Hashicorp Raft): topology join without AddToRaft.
	if cluster.raftNode.FSM == nil {
		return protocol.MakeErrReply("ERR CLUSTER MEET requires Raft FSM")
	}
	cluster.raftNode.ApplyLocal(&raft.LogEntry{
		Event: raft.EventJoin,
		JoinTask: &raft.JoinTask{
			NodeId: redisAddr,
		},
	})
	cluster.bus.incrMeetSent()
	return protocol.MakeOkReply()
}

func (cluster *Cluster) doRebalance() {
	cluster.rebalanceManager.mu.Lock()
	defer cluster.rebalanceManager.mu.Unlock()
	pendingTasks, err := cluster.makeRebalancePlan()
	if err != nil {
		logger.Errorf("makeRebalancePlan err: %v", err)
		return
	}
	if len(pendingTasks) == 0 {
		return
	}
	logger.Infof("rebalance plan generated, contains %d tasks", len(pendingTasks))

	for _, task := range pendingTasks {
		err := cluster.triggerMigrationTask(task)
		if err != nil {
			logger.Errorf("triggerMigrationTask err: %v", err)
		} else {
			logger.Infof("triggerMigrationTask %s success", task.ID)
		}
	}

}

// triggerRebalanceTask start a rebalance task
// only leader can do
func (cluster *Cluster) triggerMigrationTask(task *raft.MigratingTask) error {
	// propose migration
	_, err := cluster.raftNode.Propose(&raft.LogEntry{
		Event:         raft.EventStartMigrate,
		MigratingTask: task,
	})
	if err != nil {
		return fmt.Errorf("propose EventStartMigrate  %s failed: %v", task.ID, err)
	}
	logger.Infof("propose EventStartMigrate %s success", task.ID)

	cmdLine := utils.ToCmdLine(startMigrationCommand, task.ID)
	targetNodeConn, err := cluster.connections.BorrowPeerClient(task.TargetNode)
	if err != nil {
		return err
	}
	defer cluster.connections.ReturnPeerClient(targetNodeConn)
	reply := targetNodeConn.Send(cmdLine)
	if protocol.IsOKReply(reply) {
		return nil
	}
	if errReply, ok := reply.(protocol.ErrorReply); ok {
		return fmt.Errorf("start migration rejected: %s", errReply.Error())
	}
	return fmt.Errorf("start migration unexpected reply: %s", string(reply.ToBytes()))
}

func (cluster *Cluster) makeRebalancePlan() ([]*raft.MigratingTask, error) {

	var migratings []*raft.MigratingTask
	cluster.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
		avgSlot := int(math.Ceil(float64(SlotCount) / float64(len(fsm.MasterSlaves))))
		var exportingNodes []string
		var importingNodes []string
		for _, ms := range fsm.MasterSlaves {
			nodeId := ms.MasterId
			nodeSlots := fsm.Node2Slot[nodeId]
			if len(nodeSlots) > avgSlot+1 {
				exportingNodes = append(exportingNodes, nodeId)
			}
			if len(nodeSlots) < avgSlot-1 {
				importingNodes = append(importingNodes, nodeId)
			}
		}

		importIndex := 0
		exportIndex := 0
		var exportSlots []uint32
		for importIndex < len(importingNodes) && exportIndex < len(exportingNodes) {
			exportNode := exportingNodes[exportIndex]
			if len(exportSlots) == 0 {
				exportNodeSlots := fsm.Node2Slot[exportNode]
				exportCount := len(exportNodeSlots) - avgSlot
				exportSlots = exportNodeSlots[0:exportCount]
			}
			importNode := importingNodes[importIndex]
			importNodeCurrentIndex := fsm.Node2Slot[importNode]
			requirements := avgSlot - len(importNodeCurrentIndex)
			task := &raft.MigratingTask{
				ID:         utils.RandString(20),
				SrcNode:    exportNode,
				TargetNode: importNode,
			}
			if requirements <= len(exportSlots) {
				// exportSlots 可以提供足够的 slots, importingNode 处理完毕
				task.Slots = exportSlots[0:requirements]
				exportSlots = exportSlots[requirements:]
				importIndex++
			} else {
				// exportSlots 无法提供足够的 slots, exportingNode 处理完毕
				task.Slots = exportSlots
				exportSlots = nil
				exportIndex++
			}
			migratings = append(migratings, task)
		}
	})
	return migratings, nil
}

func (cluster *Cluster) waitCommitted(peer string, logIndex uint64) error {
	srcNodeConn, err := cluster.connections.BorrowPeerClient(peer)
	if err != nil {
		return err
	}
	defer cluster.connections.ReturnPeerClient(srcNodeConn)
	var peerIndex uint64
	for i := 0; i < 50; i++ {
		reply := srcNodeConn.Send(utils.ToCmdLine(getCommittedIndexCommand))
		switch reply := reply.(type) {
		case *protocol.IntReply:
			peerIndex = uint64(reply.Code)
			if peerIndex >= logIndex {
				return nil
			}
		case *protocol.StandardErrReply:
			logger.Infof("get committed index failed: %v", reply.Error())
		default:
			logger.Infof("get committed index unknown responseL %+v", reply.ToBytes())
		}
		time.Sleep(time.Millisecond * 100)
	}
	return fmt.Errorf("wait committed timeout")
}

// doMigrateSlot migrates a single slot from one node to another using the
// existing Raft-backed migration pipeline (triggerMigrationTask → export/import → route change).
func (cluster *Cluster) doMigrateSlot(slot uint32, from, to string) error {
	if cluster.raftNode == nil {
		return fmt.Errorf("raft node not initialized")
	}
	if cluster.raftNode.State() != raft.Leader {
		return fmt.Errorf("not cluster leader")
	}
	if from == to {
		return nil
	}

	currentOwner := cluster.PickNode(slot)
	if currentOwner == "" {
		return fmt.Errorf("slot %d has no owner", slot)
	}
	if currentOwner != from {
		return fmt.Errorf("slot %d owned by %s, expected %s", slot, currentOwner, from)
	}

	task := &raft.MigratingTask{
		ID:         utils.RandString(20),
		SrcNode:    from,
		TargetNode: to,
		Slots:      []uint32{slot},
	}

	if err := cluster.triggerMigrationTask(task); err != nil {
		return err
	}
	return cluster.waitMigrationDone(task.ID, slot, to)
}

// waitMigrationDone blocks until the migration task completes and the slot routes to targetNode.
func (cluster *Cluster) waitMigrationDone(taskID string, slot uint32, targetNode string) error {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		done := false
		cluster.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
			if fsm.GetMigratingTask(taskID) != nil {
				return
			}
			if fsm.PickNode(slot) == targetNode {
				done = true
			}
		})
		if done {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for slot %d migration to %s", slot, targetNode)
}

// execMigrationChangeRoute should be executed at leader
// it proposes EventFinishMigrate through raft, to change the route to the new node
// it returns until the proposal has been accepted by the majority  and two related nodes
// format: cluster.migration.changeroute taskid
func execMigrationChangeRoute(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeArgNumErrReply(migrationChangeRouteCommand)
	}
	state := cluster.raftNode.State()
	if state != raft.Leader {
		// I am not leader, forward request to leader
		leaderConn, err := cluster.BorrowLeaderClient()
		if err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		defer cluster.connections.ReturnPeerClient(leaderConn)
		return leaderConn.Send(cmdLine)
	}
	taskId := string(cmdLine[1])
	logger.Infof("change route for migration %s", taskId)
	task := cluster.raftNode.FSM.GetMigratingTask(taskId)
	if task == nil {
		return protocol.MakeErrReply("ERR task not found")
	}
	logger.Infof("change route for migration %s, got task info", taskId)
	// propose
	logIndex, err := cluster.raftNode.Propose(&raft.LogEntry{
		Event:         raft.EventFinishMigrate,
		MigratingTask: task,
	})
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	logger.Infof("change route for migration %s, raft proposed", taskId)

	// confirm the 2 related node committed this log
	err = cluster.waitCommitted(task.SrcNode, logIndex)
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	logger.Infof("change route for migration %s, confirm source node finished", taskId)

	err = cluster.waitCommitted(task.TargetNode, logIndex)
	if err != nil {
		return protocol.MakeErrReply("ERR " + err.Error())
	}
	logger.Infof("change route for migration %s, confirm target node finished", taskId)

	return protocol.MakeOkReply()
}
