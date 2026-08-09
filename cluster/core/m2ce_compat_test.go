package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/redis/protocol"
)

func TestM2ceClusterReplicasSlaves(t *testing.T) {
	cl := &Cluster{id_: "node-z"}

	r := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("REPLICAS"), []byte("node-z")})
	if _, ok := r.(*protocol.EmptyMultiBulkReply); !ok {
		// also accept empty MultiBulkReply
		if mb, ok2 := r.(*protocol.MultiBulkReply); !ok2 || len(mb.Args) != 0 {
			t.Fatalf("REPLICAS want empty: %T %s", r, r.ToBytes())
		}
	}

	r = execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("SLAVES"), []byte("node-z")})
	if _, ok := r.(*protocol.EmptyMultiBulkReply); !ok {
		if mb, ok2 := r.(*protocol.MultiBulkReply); !ok2 || len(mb.Args) != 0 {
			t.Fatalf("SLAVES want empty: %T %s", r, r.ToBytes())
		}
	}

	unk := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("REPLICAS"), []byte("other")})
	if !protocol.IsErrorReply(unk) || !strings.Contains(string(unk.ToBytes()), "Unknown node") {
		t.Fatalf("unknown node: %s", unk.ToBytes())
	}

	help := string(execClusterHelp().ToBytes())
	if !strings.Contains(help, "REPLICAS") || !strings.Contains(help, "SLAVES") {
		t.Fatalf("HELP missing REPLICAS/SLAVES: %s", help)
	}
}
