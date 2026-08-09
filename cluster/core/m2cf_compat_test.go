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
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "not supported") {
		t.Fatalf("SET-CONFIG-EPOCH want not supported: %T %s", r, r.ToBytes())
	}

	bad := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("SET-CONFIG-EPOCH"), []byte("-1")})
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want invalid epoch error: %s", bad.ToBytes())
	}

	help := string(execClusterHelp().ToBytes())
	if !strings.Contains(help, "LINKS") || !strings.Contains(help, "SET-CONFIG-EPOCH") || !strings.Contains(help, "Not supported") {
		t.Fatalf("HELP missing entries: %s", help)
	}
}
