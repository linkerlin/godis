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

func TestM2ciConfigReplAofHashStubs(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:                    16,
		ReplBacklogTTL:               3600,
		ReplicaIgnoreMaxmemory:       true,
		AofRewriteIncrementalFsync:   true,
		ClusterAllowReplicaMigration: true,
		ClusterReplicaValidityFactor: 10,
		HashMaxListpackEntries:       512,
	}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "repl-backlog-ttl")),
		[]string{"repl-backlog-ttl", "3600"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-backlog-ttl", "7200")), "OK")

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "replica-ignore-maxmemory")),
		[]string{"replica-ignore-maxmemory", "yes"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replica-ignore-maxmemory", "no")), "OK")

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "aof-rewrite-incremental-fsync")),
		[]string{"aof-rewrite-incremental-fsync", "yes"})
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-allow-replica-migration")),
		[]string{"cluster-allow-replica-migration", "yes"})
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-replica-validity-factor")),
		[]string{"cluster-replica-validity-factor", "10"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "hash-max-listpack-entries", "256")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "hash-max-listpack-entries")),
		[]string{"hash-max-listpack-entries", "256"})
}

func TestM2ciDebugObject(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	missing := server.Exec(c, utils.ToCmdLine("DEBUG", "OBJECT", "nope"))
	if !protocol.IsErrorReply(missing) {
		t.Fatalf("missing key want error: %s", missing.ToBytes())
	}

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "k", "v")), "OK")
	r := server.Exec(c, utils.ToCmdLine("DEBUG", "OBJECT", "k"))
	st, ok := r.(*protocol.StatusReply)
	if !ok {
		t.Fatalf("DEBUG OBJECT: %T %s", r, r.ToBytes())
	}
	if !strings.Contains(st.Status, "encoding:embstr") || !strings.Contains(st.Status, "serializedlength:1") {
		t.Fatalf("unexpected object info: %s", st.Status)
	}

	help := string(server.Exec(c, utils.ToCmdLine("DEBUG", "HELP")).ToBytes())
	if !strings.Contains(help, "OBJECT") {
		t.Fatalf("HELP missing OBJECT: %s", help)
	}
}

func TestM2ciInfoTotalBlockingKeys(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	r := server.Exec(c, utils.ToCmdLine("INFO", "stats"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO stats: %T", r)
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "total_blocking_keys:0") || !strings.Contains(s, "total_blocking_keys_on_keys:0") {
		t.Fatalf("missing blocking keys fields: %s", s)
	}
}

func TestM2ciACLCatBloomCuckooTimeseries(t *testing.T) {
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
	for _, want := range []string{"bloom", "cuckoo", "timeseries"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("ACL CAT missing %s: %s", want, joined)
		}
	}

	r := db.Exec(nil, utils.ToCmdLine("ACL", "CAT", "bloom"))
	bloom, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(bloom.Args) == 0 {
		t.Fatalf("ACL CAT bloom: %T %s", r, r.ToBytes())
	}
	found := false
	for _, a := range bloom.Args {
		if string(a) == "bf.add" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("ACL CAT bloom missing bf.add: %s", r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("ACL", "CAT", "cuckoo"))
	if cf, ok := r.(*protocol.MultiBulkReply); !ok || len(cf.Args) == 0 {
		t.Fatalf("ACL CAT cuckoo: %T %s", r, r.ToBytes())
	}

	r = db.Exec(nil, utils.ToCmdLine("ACL", "CAT", "timeseries"))
	ts, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(ts.Args) == 0 {
		t.Fatalf("ACL CAT timeseries: %T %s", r, r.ToBytes())
	}
	foundTS := false
	for _, a := range ts.Args {
		if string(a) == "ts.create" {
			foundTS = true
			break
		}
	}
	if !foundTS {
		t.Fatalf("ACL CAT timeseries missing ts.create: %s", r.ToBytes())
	}
}
