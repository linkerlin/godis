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
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "requires Raft FSM") {
		t.Fatalf("REPLICATE want requires Raft FSM: %T %s", r, r.ToBytes())
	}

	r = execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("FAILOVER")})
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "not supported") {
		t.Fatalf("FAILOVER want not supported: %T %s", r, r.ToBytes())
	}

	r = execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("FAILOVER"), []byte("FORCE")})
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "not supported") {
		t.Fatalf("FAILOVER FORCE want not supported: %T %s", r, r.ToBytes())
	}

	bad := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("FAILOVER"), []byte("WEIRD")})
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("FAILOVER WEIRD want error: %s", bad.ToBytes())
	}

	help := string(execClusterHelp().ToBytes())
	if !strings.Contains(help, "REPLICATE") || !strings.Contains(help, "FAILOVER") || !strings.Contains(help, "Not supported") {
		t.Fatalf("HELP missing entries: %s", help)
	}
	if !strings.Contains(help, "FSM EventJoin") && !strings.Contains(help, "MasterSlaves") {
		t.Fatalf("HELP REPLICATE should mention FSM path: %s", help)
	}
}
