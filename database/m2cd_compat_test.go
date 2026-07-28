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

func TestM2cdConfigClientBufferMinReplicasClusterCoverage(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:                  16,
		ClientQueryBufferLimit:     1073741824,
		ClientOutputBufferLimit:    "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60",
		MinReplicasMaxLag:          10,
		ClusterRequireFullCoverage: true,
	}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "client-query-buffer-limit")),
		[]string{"client-query-buffer-limit", "1073741824"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "client-query-buffer-limit", "2048")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "client-query-buffer-limit")),
		[]string{"client-query-buffer-limit", "2048"})

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "min-replicas-to-write")),
		[]string{"min-replicas-to-write", "0"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "min-replicas-to-write", "2")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "min-replicas-to-write")),
		[]string{"min-replicas-to-write", "2"})

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "min-replicas-max-lag")),
		[]string{"min-replicas-max-lag", "10"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "min-replicas-max-lag", "5")), "OK")

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-require-full-coverage")),
		[]string{"cluster-require-full-coverage", "yes"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-require-full-coverage", "no")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-require-full-coverage")),
		[]string{"cluster-require-full-coverage", "no"})

	out := server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "client-output-buffer-limit"))
	mb, ok := out.(*protocol.MultiBulkReply)
	if !ok || len(mb.Args) != 2 || string(mb.Args[0]) != "client-output-buffer-limit" {
		t.Fatalf("client-output-buffer-limit: %s", out.ToBytes())
	}
}

func TestM2cdInfoTotalForksAndCurrentCow(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	r := server.Exec(c, utils.ToCmdLine("INFO", "stats"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO stats: %T", r)
	}
	s := string(bulk.Arg)
	for _, want := range []string{
		"total_forks:0",
		"current_cow_size:0",
		"current_cow_size_age:0",
		"current_fork_perc:0.00",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %q: %s", want, s)
		}
	}
}

func TestM2cdLuaHScanZScanSetrespMap(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("HSET", "h", "f1", "v1", "f2", "v2"))
	_ = db.Exec(nil, utils.ToCmdLine("ZADD", "z", "1.5", "a", "2", "b"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('HSCAN', KEYS[1], '0', 'COUNT', '10')
local m = t[2]
return tostring(m['f1']) .. ':' .. tostring(m['f2'])
`, "1", "h"))
	asserts.AssertBulkReply(t, r, "v1:v2")

	r = db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('ZSCAN', KEYS[1], '0', 'COUNT', '10')
local m = t[2]
return tostring(m['a'] ~= nil) .. ':' .. tostring(m['b'] ~= nil)
`, "1", "z"))
	asserts.AssertBulkReply(t, r, "true:true")
}

func TestM2cdCommandGetKeysZUnionStore(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("COMMAND", "GETKEYS", "ZUNIONSTORE", "dest", "2", "z1", "z2"))
	asserts.AssertMultiBulkReply(t, r, []string{"dest", "z1", "z2"})

	r = db.Exec(nil, utils.ToCmdLine("COMMAND", "GETKEYS", "ZUNION", "2", "z1", "z2", "WEIGHTS", "1", "2"))
	asserts.AssertMultiBulkReply(t, r, []string{"z1", "z2"})
}
