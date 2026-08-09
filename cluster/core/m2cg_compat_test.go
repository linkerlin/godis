package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/redis/protocol"
)

func TestM2cgClusterGetKeysInSlotForgetResetSaveConfig(t *testing.T) {
	cl := &Cluster{id_: "node-cg", slotsManager: newSlotsManager()}

	r := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("GETKEYSINSLOT"), []byte("0"), []byte("10"),
	})
	if _, ok := r.(*protocol.EmptyMultiBulkReply); !ok {
		if mb, ok2 := r.(*protocol.MultiBulkReply); !ok2 || len(mb.Args) != 0 {
			t.Fatalf("GETKEYSINSLOT want empty: %T %s", r, r.ToBytes())
		}
	}

	cl.slotsManager.getSlot(0).keys.Add("k-in-slot")
	r = execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("GETKEYSINSLOT"), []byte("0"), []byte("10"),
	})
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mb.Args) != 1 || string(mb.Args[0]) != "k-in-slot" {
		t.Fatalf("GETKEYSINSLOT want local key: %T %s", r, r.ToBytes())
	}

	for _, sub := range [][][]byte{
		{[]byte("CLUSTER"), []byte("FORGET"), []byte("deadnode")},
		{[]byte("CLUSTER"), []byte("RESET")},
		{[]byte("CLUSTER"), []byte("RESET"), []byte("SOFT")},
		{[]byte("CLUSTER"), []byte("RESET"), []byte("HARD")},
		{[]byte("CLUSTER"), []byte("SAVECONFIG")},
	} {
		rr := execCluster(cl, nil, sub)
		if !protocol.IsErrorReply(rr) || !strings.Contains(string(rr.ToBytes()), "not supported") {
			t.Fatalf("%s want not supported: %T %s", string(sub[1]), rr, rr.ToBytes())
		}
	}

	bad := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("RESET"), []byte("WEIRD")})
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("RESET WEIRD want error: %s", bad.ToBytes())
	}

	help := string(execClusterHelp().ToBytes())
	for _, want := range []string{"GETKEYSINSLOT", "FORGET", "RESET", "SAVECONFIG", "Not supported"} {
		if !strings.Contains(help, want) {
			t.Fatalf("HELP missing %s: %s", want, help)
		}
	}
}
