package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/cluster/raft"
	"github.com/linkerlin/godis/redis/protocol"
)

func TestClusterMeetArityAndNoRaft(t *testing.T) {
	cl := &Cluster{id_: "127.0.0.1:7000"}

	badArity := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("MEET"), []byte("127.0.0.1"),
	})
	if !protocol.IsErrorReply(badArity) || !strings.Contains(string(badArity.ToBytes()), "wrong number") {
		t.Fatalf("want arity err: %s", badArity.ToBytes())
	}

	noRaft := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("MEET"), []byte("127.0.0.1"), []byte("7001"),
	})
	if !protocol.IsErrorReply(noRaft) || !strings.Contains(string(noRaft.ToBytes()), "requires Raft") {
		t.Fatalf("want requires Raft: %s", noRaft.ToBytes())
	}

	badPort := execCluster(newFSMCluster("127.0.0.1:7000"), nil, [][]byte{
		[]byte("CLUSTER"), []byte("MEET"), []byte("127.0.0.1"), []byte("notaport"),
	})
	if !protocol.IsErrorReply(badPort) || !strings.Contains(string(badPort.ToBytes()), "Invalid TCP base port") {
		t.Fatalf("want invalid port: %s", badPort.ToBytes())
	}
}

func TestClusterMeetFSMOnlyJoin(t *testing.T) {
	cl := newFSMCluster("127.0.0.1:7000")
	cl.raftNode.ApplyLocal(&raft.LogEntry{
		Event:    raft.EventJoin,
		JoinTask: &raft.JoinTask{NodeId: "127.0.0.1:7000"},
	})

	r := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("MEET"), []byte("127.0.0.1"), []byte("7001"),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("MEET: %T %s", r, r.ToBytes())
	}
	if _, ok := cl.raftNode.FSM.MasterSlaves["127.0.0.1:7001"]; !ok {
		t.Fatalf("peer not in MasterSlaves: %v", cl.raftNode.FSM.MasterSlaves)
	}

	r = execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("MEET"), []byte("127.0.0.1"), []byte("7001"),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("MEET again: %s", r.ToBytes())
	}
}

func TestClusterHelpMentionsMeet(t *testing.T) {
	help := string(execClusterHelp().ToBytes())
	for _, want := range []string{"MEET", "raft-port", "SETSLOT", "MIGRATING"} {
		if !strings.Contains(help, want) {
			t.Fatalf("HELP missing %s", want)
		}
	}
}
