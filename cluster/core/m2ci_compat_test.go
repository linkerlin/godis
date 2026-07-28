package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/redis/protocol"
)

func TestM2ciClusterAddDelSlotsRange(t *testing.T) {
	cl := &Cluster{id_: "node-ci"}

	r := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("ADDSLOTSRANGE"), []byte("0"), []byte("100"),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("ADDSLOTSRANGE: %T %s", r, r.ToBytes())
	}

	r = execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("DELSLOTSRANGE"), []byte("0"), []byte("100"), []byte("200"), []byte("300"),
	})
	if _, ok := r.(*protocol.OkReply); !ok {
		t.Fatalf("DELSLOTSRANGE: %T %s", r, r.ToBytes())
	}

	bad := execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("ADDSLOTSRANGE"), []byte("0"),
	})
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("odd arity want error: %s", bad.ToBytes())
	}

	bad = execCluster(cl, nil, [][]byte{
		[]byte("CLUSTER"), []byte("ADDSLOTSRANGE"), []byte("-1"), []byte("1"),
	})
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("invalid slot want error: %s", bad.ToBytes())
	}

	help := string(execClusterHelp().ToBytes())
	if !strings.Contains(help, "ADDSLOTSRANGE") || !strings.Contains(help, "DELSLOTSRANGE") {
		t.Fatalf("HELP missing entries: %s", help)
	}
}
