package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/redis/protocol"
)

func TestM2ckClusterGetSetName(t *testing.T) {
	cl := &Cluster{id_: "node-ck"}

	r := execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("GETNAME")})
	if _, ok := r.(*protocol.NullBulkReply); !ok {
		t.Fatalf("GETNAME empty want null bulk: %T %s", r, r.ToBytes())
	}

	r = execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("SETNAME"), []byte("shard-a")})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("SETNAME: %T %s", r, r.ToBytes())
	}

	r = execCluster(cl, nil, [][]byte{[]byte("CLUSTER"), []byte("GETNAME")})
	bulk, ok := r.(*protocol.BulkReply)
	if !ok || string(bulk.Arg) != "shard-a" {
		t.Fatalf("GETNAME: %T %s", r, r.ToBytes())
	}

	help := string(execClusterHelp().ToBytes())
	if !strings.Contains(help, "SETNAME") || !strings.Contains(help, "GETNAME") {
		t.Fatalf("HELP missing entries: %s", help)
	}
}
