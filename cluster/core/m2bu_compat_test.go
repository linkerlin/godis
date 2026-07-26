package core

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/redis/protocol"
)

func TestM2buClusterUnknownSubcommandHelpHint(t *testing.T) {
	r := execCluster(nil, nil, [][]byte{[]byte("CLUSTER"), []byte("FOO")})
	err, ok := r.(*protocol.StandardErrReply)
	if !ok {
		t.Fatalf("want error: %T", r)
	}
	if !strings.Contains(err.Error(), "FOO") || !strings.Contains(err.Error(), "CLUSTER HELP") {
		t.Fatalf("want Try CLUSTER HELP: %s", err.Error())
	}
}
