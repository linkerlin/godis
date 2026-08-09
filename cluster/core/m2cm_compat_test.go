package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/redis/protocol"
)

func TestM2cmClusterAddDelSlotsSetSlot(t *testing.T) {
	cl := &Cluster{id_: "node-cm"}

	r := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("ADDSLOTS"), []byte("1"), []byte("2"),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("ADDSLOTS: %T %s", r, r.ToBytes())
	}

	r = execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("DELSLOTS"), []byte("1"),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("DELSLOTS: %T %s", r, r.ToBytes())
	}

	r = execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("SETSLOT"), []byte("10"), []byte("STABLE"),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("SETSLOT STABLE want OK: %T %s", r, r.ToBytes())
	}

	// No raft: NODE clears local migrate state and returns OK (like ADDSLOTS).
	r = execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("SETSLOT"), []byte("10"), []byte("NODE"), []byte("node-cm"),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("SETSLOT NODE want OK: %T %s", r, r.ToBytes())
	}

	bad := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("SETSLOT"), []byte("10"), []byte("WEIRD"),
	})
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("SETSLOT WEIRD want error: %s", bad.ToBytes())
	}

	help := string(execClusterHelp().ToBytes())
	for _, want := range []string{"ADDSLOTS", "DELSLOTS", "SETSLOT", "MEET"} {
		if !strings.Contains(help, want) {
			t.Fatalf("HELP missing %s: %s", want, help)
		}
	}
	if !strings.Contains(help, "Raft FSM") && !strings.Contains(help, "assigns slot ownership") {
		t.Fatalf("HELP should mention NODE FSM ownership: %s", help)
	}
}
