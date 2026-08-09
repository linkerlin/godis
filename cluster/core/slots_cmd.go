package core

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/cluster/raft"
	"github.com/linkerlin/godis/datastruct/set"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/hashslot"
	"github.com/linkerlin/godis/redis/protocol"
)

func parseSlotArgs(args [][]byte) ([]uint32, redis.Reply) {
	slots := make([]uint32, 0, len(args))
	seen := make(map[uint32]struct{}, len(args))
	for _, a := range args {
		n, err := strconv.ParseInt(string(a), 10, 64)
		if err != nil || n < 0 || n >= int64(hashslot.Count) {
			return nil, protocol.MakeErrReply("ERR Invalid or out of range slot")
		}
		s := uint32(n)
		if _, ok := seen[s]; ok {
			return nil, protocol.MakeErrReply(fmt.Sprintf("ERR Slot %d specified multiple times", s))
		}
		seen[s] = struct{}{}
		slots = append(slots, s)
	}
	return slots, nil
}

func parseSlotRanges(args [][]byte) ([]uint32, redis.Reply) {
	if len(args)%2 != 0 {
		return nil, protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|addslotsrange' command")
	}
	var slots []uint32
	seen := make(map[uint32]struct{})
	for i := 0; i < len(args); i += 2 {
		start, err1 := strconv.ParseInt(string(args[i]), 10, 64)
		end, err2 := strconv.ParseInt(string(args[i+1]), 10, 64)
		if err1 != nil || err2 != nil || start < 0 || end < 0 ||
			start >= int64(hashslot.Count) || end >= int64(hashslot.Count) || start > end {
			return nil, protocol.MakeErrReply("ERR Invalid or out of range slot")
		}
		for n := start; n <= end; n++ {
			s := uint32(n)
			if _, ok := seen[s]; ok {
				return nil, protocol.MakeErrReply(fmt.Sprintf("ERR Slot %d specified multiple times", s))
			}
			seen[s] = struct{}{}
			slots = append(slots, s)
		}
	}
	return slots, nil
}

func (cluster *Cluster) isClusterReplica() bool {
	if cluster == nil || cluster.raftNode == nil || cluster.raftNode.FSM == nil {
		return false
	}
	var yes bool
	self := cluster.SelfID()
	cluster.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
		_, yes = fsm.SlaveMasters[self]
	})
	return yes
}

// proposeSlotsChange validates slot ownership then Propose/ApplyLocal / leader-forward.
func (cluster *Cluster) proposeSlotsChange(c redis.Connection, cmdLine CmdLine, event int, slots []uint32) redis.Reply {
	if cluster == nil {
		return protocol.MakeErrReply("ERR This instance has cluster support disabled")
	}
	if len(slots) == 0 {
		return protocol.MakeOkReply()
	}
	// No raft node at all (arity-only unit clusters): keep historically OK.
	if cluster.raftNode == nil {
		return protocol.MakeOkReply()
	}
	if cluster.isClusterReplica() {
		return protocol.MakeErrReply("ERR Please use SETSLOT only with masters.")
	}

	self := cluster.SelfID()
	if errReply := cluster.validateSlotsChange(event, self, slots); errReply != nil {
		return errReply
	}

	entry := &raft.LogEntry{
		Event: event,
		SlotsTask: &raft.SlotsTask{
			NodeId: self,
			Slots:  slots,
		},
	}

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
			return protocol.MakeErrReply(err.Error())
		}
		return protocol.MakeOkReply()
	}

	// FSM injected without Hashicorp Raft (unit tests).
	if cluster.raftNode.FSM == nil {
		return protocol.MakeOkReply()
	}
	cluster.raftNode.ApplyLocal(entry)
	return protocol.MakeOkReply()
}

func (cluster *Cluster) validateSlotsChange(event int, self string, slots []uint32) redis.Reply {
	if cluster.raftNode == nil || cluster.raftNode.FSM == nil {
		return nil
	}
	var errMsg string
	cluster.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
		for _, s := range slots {
			owner, assigned := fsm.Slot2Node[s]
			switch event {
			case raft.EventAddSlots:
				if assigned {
					errMsg = fmt.Sprintf("ERR Slot %d is already busy", s)
					return
				}
			case raft.EventRemoveSlots:
				if !assigned {
					errMsg = fmt.Sprintf("ERR Slot %d is already unassigned", s)
					return
				}
				if owner != self {
					errMsg = fmt.Sprintf("ERR Slot %d is not served by this node", s)
					return
				}
			}
		}
	})
	if errMsg != "" {
		return protocol.MakeErrReply(errMsg)
	}
	return nil
}

func execClusterAddSlots(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|addslots' command")
	}
	slots, errReply := parseSlotArgs(cmdLine[2:])
	if errReply != nil {
		return errReply
	}
	return cluster.proposeSlotsChange(c, cmdLine, raft.EventAddSlots, slots)
}

func execClusterDelSlots(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 3 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|delslots' command")
	}
	slots, errReply := parseSlotArgs(cmdLine[2:])
	if errReply != nil {
		return errReply
	}
	return cluster.proposeSlotsChange(c, cmdLine, raft.EventRemoveSlots, slots)
}

func execClusterAddSlotsRange(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 4 || (len(cmdLine)-2)%2 != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|addslotsrange' command")
	}
	slots, errReply := parseSlotRanges(cmdLine[2:])
	if errReply != nil {
		return errReply
	}
	return cluster.proposeSlotsChange(c, cmdLine, raft.EventAddSlots, slots)
}

func execClusterDelSlotsRange(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 4 || (len(cmdLine)-2)%2 != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|delslotsrange' command")
	}
	slots, errReply := parseSlotRanges(cmdLine[2:])
	if errReply != nil {
		return errReply
	}
	return cluster.proposeSlotsChange(c, cmdLine, raft.EventRemoveSlots, slots)
}

func execClusterFlushSlots(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|flushslots' command")
	}
	if cluster == nil {
		return protocol.MakeErrReply("ERR This instance has cluster support disabled")
	}
	if cluster.raftNode == nil || cluster.raftNode.FSM == nil {
		return protocol.MakeOkReply()
	}
	self := cluster.SelfID()
	var slots []uint32
	cluster.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
		slots = append([]uint32(nil), fsm.Node2Slot[self]...)
	})
	return cluster.proposeSlotsChange(c, cmdLine, raft.EventRemoveSlots, slots)
}

// execClusterSetSlot handles CLUSTER SETSLOT.
// MIGRATING / IMPORTING / STABLE update local slotsManager so ASK/ASKING align with admin state.
// NODE assigns slot ownership in the Raft FSM and clears local MIGRATING/IMPORTING.
func execClusterSetSlot(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 4 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|setslot' command")
	}
	if cluster == nil {
		return protocol.MakeErrReply("ERR This instance has cluster support disabled")
	}
	slot, err := strconv.ParseInt(string(cmdLine[2]), 10, 64)
	if err != nil || slot < 0 || slot >= int64(hashslot.Count) {
		return protocol.MakeErrReply("ERR Invalid or out of range slot")
	}
	sub := strings.ToUpper(string(cmdLine[3]))
	switch sub {
	case "MIGRATING", "IMPORTING":
		if len(cmdLine) != 5 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|setslot' command")
		}
	case "STABLE":
		if len(cmdLine) != 4 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|setslot' command")
		}
	case "NODE":
		if len(cmdLine) != 5 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|setslot' command")
		}
		return cluster.execSetSlotNode(c, cmdLine, uint32(slot), string(cmdLine[4]))
	default:
		return protocol.MakeErrReply("ERR Invalid CLUSTER SETSLOT action")
	}

	if cluster.isClusterReplica() {
		return protocol.MakeErrReply("ERR Please use SETSLOT only with masters.")
	}
	if cluster.slotsManager == nil {
		cluster.slotsManager = newSlotsManager()
	}

	st := cluster.slotsManager.getSlot(uint32(slot))
	st.mu.Lock()
	defer st.mu.Unlock()

	switch sub {
	case "MIGRATING":
		st.state = slotStateExporting
		st.migratePeer = string(cmdLine[4])
		if st.dirtyKeys == nil {
			st.dirtyKeys = set.Make()
		}
	case "IMPORTING":
		st.state = slotStateImporting
		st.migratePeer = string(cmdLine[4])
	case "STABLE":
		st.state = slotStateHosting
		st.migratePeer = ""
		st.dirtyKeys = nil
		st.exportSnapshot = nil
	}
	return protocol.MakeOkReply()
}

func (cluster *Cluster) clearLocalSlotMigrate(slot uint32) {
	if cluster.slotsManager == nil {
		return
	}
	st := cluster.slotsManager.getSlot(slot)
	st.mu.Lock()
	st.state = slotStateHosting
	st.migratePeer = ""
	st.dirtyKeys = nil
	st.exportSnapshot = nil
	st.mu.Unlock()
}

// execSetSlotNode writes slot ownership to the FSM (EventAssignSlots) and clears local migrate state.
func (cluster *Cluster) execSetSlotNode(c redis.Connection, cmdLine CmdLine, slot uint32, nodeID string) redis.Reply {
	if cluster.isClusterReplica() {
		return protocol.MakeErrReply("ERR Please use SETSLOT only with masters.")
	}
	if nodeID == "" {
		return protocol.MakeErrReply("ERR Invalid node id")
	}

	// No raft node: keep local-admin semantics (same as ADDSLOTS with nil raft).
	if cluster.raftNode == nil {
		if cluster.slotsManager == nil {
			cluster.slotsManager = newSlotsManager()
		}
		cluster.clearLocalSlotMigrate(slot)
		return protocol.MakeOkReply()
	}

	entry := &raft.LogEntry{
		Event: raft.EventAssignSlots,
		SlotsTask: &raft.SlotsTask{
			NodeId: nodeID,
			Slots:  []uint32{slot},
		},
	}

	if cluster.raftNode.RaftReady() {
		if cluster.raftNode.State() != raft.Leader {
			leaderConn, err := cluster.BorrowLeaderClient()
			if err != nil {
				return protocol.MakeErrReply(err.Error())
			}
			defer cluster.connections.ReturnPeerClient(leaderConn)
			reply := leaderConn.Send(cmdLine)
			// Clear migrate state on the node that received SETSLOT, not only the leader.
			if !protocol.IsErrorReply(reply) {
				cluster.clearLocalSlotMigrate(slot)
			}
			return reply
		}
		if _, err := cluster.raftNode.Propose(entry); err != nil {
			return protocol.MakeErrReply(err.Error())
		}
		cluster.clearLocalSlotMigrate(slot)
		return protocol.MakeOkReply()
	}

	// FSM injected without Hashicorp Raft (unit tests).
	if cluster.raftNode.FSM == nil {
		cluster.clearLocalSlotMigrate(slot)
		return protocol.MakeOkReply()
	}
	cluster.raftNode.ApplyLocal(entry)
	cluster.clearLocalSlotMigrate(slot)
	return protocol.MakeOkReply()
}
