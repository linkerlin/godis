package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/redis/protocol"
)

func TestM2cfClusterLinksSetConfigEpoch(t *testing.T) {
	cl := &Cluster{id_: "node-cf"}

	r := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("LINKS")})
	if _, ok := r.(*protocol.EmptyMultiBulkReply); !ok {
		if mb, ok2 := r.(*protocol.MultiBulkReply); !ok2 || len(mb.Args) != 0 {
			t.Fatalf("LINKS want empty: %T %s", r, r.ToBytes())
		}
	}

	r = execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("SET-CONFIG-EPOCH"), []byte("1")})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("SET-CONFIG-EPOCH: %T %s", r, r.ToBytes())
	}

	bad := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("SET-CONFIG-EPOCH"), []byte("-1")})
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want invalid epoch error: %s", bad.ToBytes())
	}

	help := string(execClusterHelp().ToBytes())
	if !strings.Contains(help, "LINKS") || !strings.Contains(help, "SET-CONFIG-EPOCH") {
		t.Fatalf("HELP missing entries: %s", help)
	}
}
