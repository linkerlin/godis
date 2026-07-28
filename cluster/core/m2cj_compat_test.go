package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/redis/protocol"
)

func TestM2cjClusterReplicateFailover(t *testing.T) {
	cl := &Cluster{id_: "node-cj"}

	r := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("REPLICATE"), []byte("master-id"),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("REPLICATE: %T %s", r, r.ToBytes())
	}

	r = execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("FAILOVER")})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("FAILOVER: %T %s", r, r.ToBytes())
	}

	r = execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("FAILOVER"), []byte("FORCE")})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("FAILOVER FORCE: %T %s", r, r.ToBytes())
	}

	bad := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("FAILOVER"), []byte("WEIRD")})
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("FAILOVER WEIRD want error: %s", bad.ToBytes())
	}

	help := string(execClusterHelp().ToBytes())
	if !strings.Contains(help, "REPLICATE") || !strings.Contains(help, "FAILOVER") {
		t.Fatalf("HELP missing entries: %s", help)
	}
}
