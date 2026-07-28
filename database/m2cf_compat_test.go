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

func TestM2cfConfigClusterNodeTimeoutMigrationBarrierAllowReads(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:               16,
		ClusterNodeTimeout:      15000,
		ClusterMigrationBarrier: 1,
	}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-node-timeout")),
		[]string{"cluster-node-timeout", "15000"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-node-timeout", "20000")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-node-timeout")),
		[]string{"cluster-node-timeout", "20000"})

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-migration-barrier")),
		[]string{"cluster-migration-barrier", "1"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-migration-barrier", "2")), "OK")

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-allow-reads-when-down")),
		[]string{"cluster-allow-reads-when-down", "no"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-allow-reads-when-down", "yes")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-allow-reads-when-down")),
		[]string{"cluster-allow-reads-when-down", "yes"})
}

func TestM2cfInfoTotalNetReplBytes(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	r := server.Exec(c, utils.ToCmdLine("INFO", "stats"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO stats: %T", r)
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "total_net_repl_input_bytes:0") ||
		!strings.Contains(s, "total_net_repl_output_bytes:0") {
		t.Fatalf("missing total_net_repl_* : %s", s)
	}
}

func TestM2cfInfoTrackingTotalItems(t *testing.T) {
	c := connection.NewFakeConn()
	id := EnableTracking(c, "ON", []string{"user:", "app:"}, "", false)
	TrackKey(id, "k1")
	TrackKey(id, "k2")

	if GetTotalTrackedItems() < 2 {
		t.Fatalf("items=%d want >=2", GetTotalTrackedItems())
	}
	if GetTotalTrackedPrefixes() < 2 {
		t.Fatalf("prefixes=%d want >=2", GetTotalTrackedPrefixes())
	}

	server := MustNewStandaloneServer()
	defer server.Close()
	fc := connection.NewFakeConn()
	r := server.Exec(fc, utils.ToCmdLine("INFO", "clients"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO clients: %T", r)
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "tracking_total_items:") || !strings.Contains(s, "tracking_total_prefixes:") {
		t.Fatalf("missing tracking fields: %s", s)
	}
}

func TestM2cfLuaGeoDistGeoposSetrespNumber(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("GEOADD", "g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local d = redis.call('GEODIST', KEYS[1], 'Palermo', 'Catania')
return type(d)
`, "1", "g"))
	asserts.AssertBulkReply(t, r, "number")

	r = db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local p = redis.call('GEOPOS', KEYS[1], 'Palermo')
return type(p[1][1]) .. ':' .. type(p[1][2])
`, "1", "g"))
	asserts.AssertBulkReply(t, r, "number:number")
}
