package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2cjConfigListpackIntsetStreamHLL(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:              16,
		ListMaxListpackSize:    -2,
		SetMaxIntsetEntries:    512,
		ZSetMaxListpackEntries: 128,
		ZSetMaxListpackValue:   64,
		StreamNodeMaxBytes:     4096,
		HLLSparseMaxBytes:      3000,
	}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "list-max-listpack-size")),
		[]string{"list-max-listpack-size", "-2"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "list-max-listpack-size", "-1")), "OK")

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "set-max-intset-entries")),
		[]string{"set-max-intset-entries", "512"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "set-max-intset-entries", "256")), "OK")

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "zset-max-listpack-entries")),
		[]string{"zset-max-listpack-entries", "128"})
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "zset-max-listpack-value")),
		[]string{"zset-max-listpack-value", "64"})
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "stream-node-max-bytes")),
		[]string{"stream-node-max-bytes", "4096"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "hll-sparse-max-bytes", "4000")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "hll-sparse-max-bytes")),
		[]string{"hll-sparse-max-bytes", "4000"})
}

func TestM2cjInfoMemClientsAndDefrag(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	r := server.Exec(c, utils.ToCmdLine("INFO", "memory"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO memory: %T", r)
	}
	s := string(bulk.Arg)
	for _, want := range []string{
		"mem_clients_slaves:0",
		"mem_clients_normal:0",
		"mem_cluster_links:0",
		"active_defrag_running:0",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %s in: %s", want, s)
		}
	}
}

func TestM2cjClientListTotCmds(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	RegisterClient(c)
	defer UnregisterClient(c)

	r := server.Exec(c, utils.ToCmdLine("CLIENT", "LIST"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("CLIENT LIST: %T %s", r, r.ToBytes())
	}
	if !strings.Contains(string(bulk.Arg), "tot-cmds=") {
		t.Fatalf("missing tot-cmds=: %s", bulk.Arg)
	}
}

func TestM2cjACLCatCMSTopKTdigest(t *testing.T) {
	db := makeTestDB()
	cats := db.Exec(nil, utils.ToCmdLine("ACL", "CAT"))
	mb, ok := cats.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("ACL CAT: %T", cats)
	}
	joined := ""
	for _, a := range mb.Args {
		joined += string(a) + " "
	}
	for _, want := range []string{"@cms", "@topk", "@tdigest"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("ACL CAT missing %s: %s", want, joined)
		}
	}

	r := db.Exec(nil, utils.ToCmdLine("ACL", "CAT", "@cms"))
	cms, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(cms.Args) == 0 {
		t.Fatalf("ACL CAT @cms: %T %s", r, r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("ACL", "CAT", "@topk"))
	topk, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(topk.Args) == 0 {
		t.Fatalf("ACL CAT @topk: %T %s", r, r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("ACL", "CAT", "@tdigest"))
	td, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(td.Args) == 0 {
		t.Fatalf("ACL CAT @tdigest: %T %s", r, r.ToBytes())
	}
	found := false
	for _, a := range td.Args {
		if string(a) == "tdigest.create" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("ACL CAT @tdigest missing tdigest.create: %s", r.ToBytes())
	}
}
