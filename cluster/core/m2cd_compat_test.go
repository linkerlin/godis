package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/redis/protocol"
)

func TestM2cdClusterCountFailureReportsBumpEpoch(t *testing.T) {
	cl := &Cluster{id_: "node-y"}

	r := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("COUNT-FAILURE-REPORTS"), []byte("node-y")})
	ir, ok := r.(*protocol.IntReply)
	if !ok || ir.Code != 0 {
		t.Fatalf("COUNT-FAILURE-REPORTS: %T %s", r, r.ToBytes())
	}

	r = execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("BUMPEPOCH")})
	st, ok := r.(*protocol.StatusReply)
	if !ok || !strings.HasPrefix(st.Status, "BUMPED ") {
		t.Fatalf("BUMPEPOCH: %T %s", r, r.ToBytes())
	}

	help := string(execClusterHelp().ToBytes())
	if !strings.Contains(help, "COUNT-FAILURE-REPORTS") || !strings.Contains(help, "BUMPEPOCH") {
		t.Fatalf("HELP missing entries: %s", help)
	}
}
