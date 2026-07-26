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

func TestM2bqScriptHelp(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("SCRIPT", "HELP"))
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("SCRIPT HELP: %T %s", r, r.ToBytes())
	}
	joined := string(bytesJoin(mb.Args))
	if !strings.Contains(joined, "LOAD") || !strings.Contains(joined, "FLUSH") {
		t.Fatalf("help missing LOAD/FLUSH: %s", joined)
	}
}

func TestM2bqFTConfigMinPrefixMaxExpansions(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MINPREFIX", "3")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MAXEXPANSIONS", "50")), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "GET", "MINPREFIX")),
		[]string{"MINPREFIX", "3"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "GET", "MAXEXPANSIONS")),
		[]string{"MAXEXPANSIONS", "50"})
	bad := db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MINPREFIX", "x"))
	if !protocol.IsErrorReply(bad) {
		t.Fatalf("want reject non-int MINPREFIX")
	}
	// restore defaults for other tests
	_ = db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MINPREFIX", "2"))
	_ = db.Exec(nil, utils.ToCmdLine("FT.CONFIG", "SET", "MAXEXPANSIONS", "200"))
}

func TestM2bqConfigClusterRaftGetAndHelp(t *testing.T) {
	oldProps := config.Properties
	config.Properties = &config.ServerProperties{
		ClusterEnable:     true,
		ClusterAsSeed:     true,
		ClusterSeed:       "127.0.0.1:6399",
		RaftListenAddr:    "0.0.0.0:16666",
		RaftAdvertiseAddr: "127.0.0.1:16666",
		MasterInCluster:   "",
		Databases:         16,
	}
	defer func() { config.Properties = oldProps }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-seed")),
		[]string{"cluster-seed", "127.0.0.1:6399"})
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "raft-listen-address")),
		[]string{"raft-listen-address", "0.0.0.0:16666"})
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-as-seed")),
		[]string{"cluster-as-seed", "yes"})

	help := server.Exec(c, utils.ToCmdLine("CONFIG", "HELP"))
	mb, ok := help.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("CONFIG HELP: %T %s", help, help.ToBytes())
	}
	joined := string(bytesJoin(mb.Args))
	if !strings.Contains(joined, "GET") || !strings.Contains(joined, "REWRITE") {
		t.Fatalf("CONFIG HELP incomplete: %s", joined)
	}
}

func TestM2bqObjectSetListpack(t *testing.T) {
	db := makeTestDB()
	_ = db.Exec(nil, utils.ToCmdLine("SADD", "ss", "alpha", "beta"))
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "ss")), "listpack")

	_ = db.Exec(nil, utils.ToCmdLine("SADD", "si", "1", "2", "3"))
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "si")), "intset")

	for i := 0; i < 130; i++ {
		_ = db.Exec(nil, utils.ToCmdLine("SADD", "sbig", "m"+utils.RandString(8)))
	}
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "sbig")), "hashtable")
}
