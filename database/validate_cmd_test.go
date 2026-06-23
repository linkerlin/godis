package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/consts"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

func oversizedKey() string {
	return strings.Repeat("k", consts.MaxKeySize+1)
}

func TestOversizedKeyRejected(t *testing.T) {
	db := makeTestDB()
	key := oversizedKey()

	cases := []struct {
		name string
		cmd  [][]byte
	}{
		{"set", utils.ToCmdLine("SET", key, "v")},
		{"bf.add", utils.ToCmdLine("BF.ADD", key, "item")},
		{"ft.add", utils.ToCmdLine("FT.CREATE", key, "SCHEMA", "title", "TEXT")},
		{"ts.add", utils.ToCmdLine("TS.CREATE", key)},
		{"vsadd", utils.ToCmdLine("VSADD", key, "id1", "[1,2,3]")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reply := db.Exec(nil, tc.cmd)
			if !protocol.IsErrorReply(reply) {
				t.Fatalf("expected error for oversized key, got %s", reply.ToBytes())
			}
			if !strings.Contains(string(reply.ToBytes()), "key too large") {
				t.Fatalf("expected key too large, got %s", reply.ToBytes())
			}
		})
	}
}

func TestEmptyKeyRejected(t *testing.T) {
	db := makeTestDB()
	reply := db.Exec(nil, utils.ToCmdLine("SET", "", "v"))
	if !protocol.IsErrorReply(reply) {
		t.Fatalf("expected error for empty key, got %s", reply.ToBytes())
	}
}
