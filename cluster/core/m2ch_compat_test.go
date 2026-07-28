package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/redis/protocol"
)

func TestM2chClusterFlushSlotsMyShardID(t *testing.T) {
	cl := &Cluster{id_: "node-ch-shard"}

	r := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("FLUSHSLOTS")})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("FLUSHSLOTS: %T %s", r, r.ToBytes())
	}

	r = execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("MYSHARDID")})
	bulk, ok := r.(*protocol.BulkReply)
	if !ok || string(bulk.Arg) != "node-ch-shard" {
		t.Fatalf("MYSHARDID: %T %s", r, r.ToBytes())
	}

	help := string(execClusterHelp().ToBytes())
	if !strings.Contains(help, "FLUSHSLOTS") || !strings.Contains(help, "MYSHARDID") {
		t.Fatalf("HELP missing entries: %s", help)
	}
}
