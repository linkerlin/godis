package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/redis/protocol"
)

func TestM2ccClusterCountKeysInSlotAndShards(t *testing.T) {
	cl := &Cluster{id_: "node-x", slotsManager: newSlotsManager()}
	cl.slotsManager.getSlot(42).keys.Add("k1")
	cl.slotsManager.getSlot(42).keys.Add("k2")

	r := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("COUNTKEYSINSLOT"), []byte("42")})
	ir, ok := r.(*protocol.IntReply)
	if !ok || ir.Code != 2 {
		t.Fatalf("COUNTKEYSINSLOT: %T %s", r, r.ToBytes())
	}

	bad := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("COUNTKEYSINSLOT"), []byte("99999")})
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want slot range error: %s", bad.ToBytes())
	}

	shards := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("SHARDS")})
	mr, ok := shards.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) < 1 {
		t.Fatalf("SHARDS: %T %s", shards, shards.ToBytes())
	}
	if !strings.Contains(string(shards.ToBytes()), "node-x") {
		t.Fatalf("SHARDS missing node id: %s", shards.ToBytes())
	}

	help := execClusterHelp()
	joined := string(help.ToBytes())
	if !strings.Contains(joined, "COUNTKEYSINSLOT") || !strings.Contains(joined, "SHARDS") {
		t.Fatalf("HELP missing entries: %s", joined)
	}
}
