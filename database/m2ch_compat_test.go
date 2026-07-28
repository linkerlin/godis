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

func TestM2chConfigIOReplSamplesTracking(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:            16,
		IOThreads:            1,
		ReplDisklessSyncDelay: 5,
		MaxmemorySamples:     5,
		TrackingTableMaxKeys: 1000000,
	}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "io-threads")),
		[]string{"io-threads", "1"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "io-threads", "4")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "io-threads")),
		[]string{"io-threads", "4"})

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "io-threads-do-reads")),
		[]string{"io-threads-do-reads", "no"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "io-threads-do-reads", "yes")), "OK")

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "repl-diskless-sync")),
		[]string{"repl-diskless-sync", "no"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-diskless-sync", "yes")), "OK")

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "repl-diskless-sync-delay")),
		[]string{"repl-diskless-sync-delay", "5"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-diskless-sync-delay", "10")), "OK")

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "maxmemory-samples")),
		[]string{"maxmemory-samples", "5"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-samples", "10")), "OK")

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "tracking-table-max-keys")),
		[]string{"tracking-table-max-keys", "1000000"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tracking-table-max-keys", "100")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "tracking-table-max-keys")),
		[]string{"tracking-table-max-keys", "100"})
}

func TestM2chInfoClusterSection(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{Databases: 16, ClusterEnable: false}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	r := server.Exec(c, utils.ToCmdLine("INFO", "cluster"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO cluster: %T", r)
	}
	if !strings.Contains(string(bulk.Arg), "cluster_enabled:0") {
		t.Fatalf("standalone want cluster_enabled:0: %s", bulk.Arg)
	}

	config.Properties.ClusterEnable = true
	r = server.Exec(c, utils.ToCmdLine("INFO", "cluster"))
	bulk, ok = r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO cluster (enabled): %T", r)
	}
	s := string(bulk.Arg)
	for _, want := range []string{
		"cluster_enabled:1",
		"cluster_state:ok",
		"cluster_slots_assigned:16384",
		"cluster_known_nodes:1",
		"cluster_current_epoch:0",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %s in: %s", want, s)
		}
	}
}

func TestM2chClientListPeerID(t *testing.T) {
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
	line := string(bulk.Arg)
	if !strings.Contains(line, "peerid=") {
		t.Fatalf("missing peerid=: %s", line)
	}
	// peerid value is 40 hex chars
	idx := strings.Index(line, "peerid=")
	rest := line[idx+len("peerid="):]
	end := strings.IndexByte(rest, ' ')
	if end < 0 {
		end = len(rest)
		if i := strings.IndexByte(rest, '\n'); i >= 0 {
			end = i
		}
	}
	peer := rest[:end]
	if len(peer) != 40 {
		t.Fatalf("peerid len=%d want 40: %q", len(peer), peer)
	}
	for _, ch := range peer {
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f')) {
			t.Fatalf("peerid not hex: %q", peer)
		}
	}
}

func TestM2chLuaZRankWithScoreSetrespNumber(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1.5", "m"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('ZRANK', KEYS[1], 'm', 'WITHSCORE')
return type(t[1]) .. ':' .. type(t[2])
`, "1", "z"))
	asserts.AssertBulkReply(t, r, "number:number")

	r = db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('ZREVRANK', KEYS[1], 'm', 'WITHSCORE')
return type(t[2])
`, "1", "z"))
	asserts.AssertBulkReply(t, r, "number")
}
