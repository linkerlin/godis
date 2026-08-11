package raft

import (
	"errors"
	"sort"
)

// PickNode returns node hosting slot, ignore migrating
func (fsm *FSM) PickNode(slot uint32) string {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	return fsm.Slot2Node[slot]
}

// WithReadLock allow invoker do something complicated with read lock
func (fsm *FSM) WithReadLock(fn func(fsm *FSM)) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	fn(fsm)
}

func (fsm *FSM) GetMigratingTask(taskId string) *MigratingTask {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()
	return fsm.Migratings[taskId]
}

func (fsm *FSM) addSlots(nodeID string, slots []uint32) {
	for _, slotId := range slots {
		/// update node2Slot
		index := sort.Search(len(fsm.Node2Slot[nodeID]), func(i int) bool {
			return fsm.Node2Slot[nodeID][i] >= slotId
		})
		if !(index < len(fsm.Node2Slot[nodeID]) && fsm.Node2Slot[nodeID][index] == slotId) {
			// not found in node's slots, insert
			fsm.Node2Slot[nodeID] = append(fsm.Node2Slot[nodeID][:index],
				append([]uint32{slotId}, fsm.Node2Slot[nodeID][index:]...)...)
		}
		/// update slot2Node
		fsm.Slot2Node[slotId] = nodeID
	}
}

func (fsm *FSM) removeSlots(nodeID string, slots []uint32) {
	for _, slotId := range slots {
		/// update node2slot
		index := sort.Search(len(fsm.Node2Slot[nodeID]), func(i int) bool { return fsm.Node2Slot[nodeID][i] >= slotId })
		// found slot remove
		for index < len(fsm.Node2Slot[nodeID]) && fsm.Node2Slot[nodeID][index] == slotId {
			fsm.Node2Slot[nodeID] = append(fsm.Node2Slot[nodeID][:index], fsm.Node2Slot[nodeID][index+1:]...)
		}
		// update slot2node
		if fsm.Slot2Node[slotId] == nodeID {
			delete(fsm.Slot2Node, slotId)
		}
	}
}

// assignSlots sets slot ownership to nodeID, removing each slot from its previous owner if any.
func (fsm *FSM) assignSlots(nodeID string, slots []uint32) {
	for _, slotId := range slots {
		if old, ok := fsm.Slot2Node[slotId]; ok && old != nodeID {
			fsm.removeSlots(old, []uint32{slotId})
		}
	}
	fsm.addSlots(nodeID, slots)
}

func (fsm *FSM) failover(oldMasterId, newMasterId string) {
	oldSlaves := fsm.MasterSlaves[oldMasterId].Slaves
	newSlaves := make([]string, 0, len(oldSlaves))
	// change other slaves
	for _, slave := range oldSlaves {
		if slave != newMasterId {
			fsm.SlaveMasters[slave] = newMasterId
			newSlaves = append(newSlaves, slave)
		}
	}
	// change old master
	delete(fsm.MasterSlaves, oldMasterId)
	fsm.SlaveMasters[oldMasterId] = newMasterId
	newSlaves = append(newSlaves, oldMasterId)

	// change new master
	delete(fsm.SlaveMasters, newMasterId)
	fsm.MasterSlaves[newMasterId] = &MasterSlave{
		MasterId: newMasterId,
		Slaves:   newSlaves,
	}
}

// GetMaster returns the master's redis service address
// Returns empty string ("") if id points to a master node
func (fsm *FSM) GetMaster(id string) string {
	master := ""
	fsm.WithReadLock(func(fsm *FSM) {
		master = fsm.SlaveMasters[id]
	})
	return master
}

func (fsm *FSM) addNode(id, masterId string) error {
	if masterId == "" {
		fsm.MasterSlaves[id] = &MasterSlave{
			MasterId: id,
		}
		return nil
	}
	return fsm.setReplica(id, masterId)
}

// setReplica attaches id as a replica of masterId (CLUSTER REPLICATE / EventJoin with Master).
// Caller must hold fsm.mu (or be inside Apply). Rejects when id owns slots or has replicas.
func (fsm *FSM) setReplica(id, masterId string) error {
	if id == "" || masterId == "" {
		return errors.New("id and master required")
	}
	if id == masterId {
		return errors.New("can't replicate myself")
	}
	master := fsm.MasterSlaves[masterId]
	if master == nil {
		return errors.New("master not found")
	}
	if fsm.SlaveMasters[id] == masterId {
		return nil // idempotent
	}
	if len(fsm.Node2Slot[id]) > 0 {
		return errors.New("node has assigned slots")
	}
	if ms, ok := fsm.MasterSlaves[id]; ok {
		if len(ms.Slaves) > 0 {
			return errors.New("node has replicas")
		}
		delete(fsm.MasterSlaves, id)
	}
	if old, ok := fsm.SlaveMasters[id]; ok && old != masterId {
		if oms := fsm.MasterSlaves[old]; oms != nil {
			oms.Slaves = filterNodeIDs(oms.Slaves, id)
		}
	}
	exists := false
	for _, slave := range master.Slaves {
		if slave == id {
			exists = true
			break
		}
	}
	if !exists {
		master.Slaves = append(master.Slaves, id)
	}
	fsm.SlaveMasters[id] = masterId
	return nil
}

// forgetNode drops id from topology. Safe path: unknown→err; has slots→err; master with replicas→err.
func (fsm *FSM) forgetNode(id string) error {
	if id == "" {
		return errors.New("empty node id")
	}
	if len(fsm.Node2Slot[id]) > 0 {
		return errors.New("Can't forget a node with assigned slots")
	}
	if ms, ok := fsm.MasterSlaves[id]; ok && len(ms.Slaves) > 0 {
		return errors.New("Can't forget a master with replicas")
	}
	known := false
	if _, ok := fsm.MasterSlaves[id]; ok {
		known = true
		delete(fsm.MasterSlaves, id)
	}
	if masterId, ok := fsm.SlaveMasters[id]; ok {
		known = true
		if ms := fsm.MasterSlaves[masterId]; ms != nil {
			ms.Slaves = filterNodeIDs(ms.Slaves, id)
		}
		delete(fsm.SlaveMasters, id)
	}
	for _, ms := range fsm.MasterSlaves {
		if ms == nil {
			continue
		}
		ms.Slaves = filterNodeIDs(ms.Slaves, id)
	}
	delete(fsm.Node2Slot, id)
	if !known {
		return errors.New("Unknown node id")
	}
	return nil
}

func filterNodeIDs(ids []string, drop string) []string {
	if len(ids) == 0 {
		return ids
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id != drop {
			out = append(out, id)
		}
	}
	return out
}
