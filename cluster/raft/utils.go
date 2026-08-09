package raft

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/hashicorp/raft"
)

func (node *Node) Self() string {
	return node.Cfg.ID()
}

// RaftReady reports whether the Hashicorp Raft instance is started (Propose safe).
func (node *Node) RaftReady() bool {
	return node != nil && node.inner != nil
}

// ApplyLocal applies a log entry to the FSM without Raft consensus (tests / FSM-only stubs).
func (node *Node) ApplyLocal(entry *LogEntry) {
	if node == nil || node.FSM == nil || entry == nil {
		return
	}
	bin, err := json.Marshal(entry)
	if err != nil {
		return
	}
	node.FSM.Apply(&raft.Log{Data: bin})
}

func (node *Node) State() raft.RaftState {
	return node.inner.State()
}

func (node *Node) CommittedIndex() (uint64, error) {
	stats := node.inner.Stats()
	committedIndex0 := stats["commit_index"]
	return strconv.ParseUint(committedIndex0, 10, 64)
}

func (node *Node) GetLeaderRedisAddress() string {
	// redis advertise address used as leader id
	_, id := node.inner.LeaderWithID()
	return string(id)
}

func (node *Node) GetNodes() ([]raft.Server, error) {
	configFuture := node.inner.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return nil, fmt.Errorf("failed to get raft configuration: %v", err)
	}
	return configFuture.Configuration().Servers, nil
}

func (node *Node) GetSlaves(id string) *MasterSlave {
	node.FSM.mu.RLock()
	defer node.FSM.mu.RUnlock()
	return node.FSM.MasterSlaves[id]
}
