package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/redis/protocol"
)

func TestM2cbClusterMyID(t *testing.T) {
	cl := &Cluster{id_: "node-abc123"}
	r := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("MYID")})
	bulk, ok := r.(*protocol.BulkReply)
	if !ok || string(bulk.Arg) != "node-abc123" {
		t.Fatalf("CLUSTER MYID: %T %s", r, r.ToBytes())
	}

	help := execClusterHelp()
	mr, ok := help.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("CLUSTER HELP: %T", help)
	}
	joined := ""
	for _, item := range mr.Replies {
		if b, ok := item.(*protocol.BulkReply); ok {
			joined += string(b.Arg) + "\n"
		}
	}
	if !strings.Contains(joined, "CLUSTER MYID") {
		t.Fatalf("HELP missing MYID: %s", joined)
	}
}
