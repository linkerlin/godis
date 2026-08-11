package raft

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"
)

func TestFSMSetReplicaAndForget(t *testing.T) {
	fsm := &FSM{
		Node2Slot:    make(map[string][]uint32),
		Slot2Node:    make(map[uint32]string),
		Migratings:   make(map[string]*MigratingTask),
		MasterSlaves: make(map[string]*MasterSlave),
		SlaveMasters: make(map[string]string),
		Failovers:    make(map[string]*FailoverTask),
	}
	apply := func(e *LogEntry) {
		bin, _ := json.Marshal(e)
		fsm.Apply(&raft.Log{Data: bin})
	}
	apply(&LogEntry{Event: EventJoin, JoinTask: &JoinTask{NodeId: "m1"}})
	apply(&LogEntry{Event: EventJoin, JoinTask: &JoinTask{NodeId: "s1"}})
	apply(&LogEntry{Event: EventJoin, JoinTask: &JoinTask{NodeId: "s1", Master: "m1"}})
	if fsm.SlaveMasters["s1"] != "m1" {
		t.Fatalf("slave map: %v", fsm.SlaveMasters)
	}
	if _, ok := fsm.MasterSlaves["s1"]; ok {
		t.Fatal("s1 should not remain master")
	}
	apply(&LogEntry{Event: EventForget, ForgetTask: &ForgetTask{NodeId: "s1"}})
	if _, ok := fsm.SlaveMasters["s1"]; ok {
		t.Fatal("s1 should be forgotten")
	}
	if len(fsm.MasterSlaves["m1"].Slaves) != 0 {
		t.Fatalf("m1 slaves=%v", fsm.MasterSlaves["m1"].Slaves)
	}
}
