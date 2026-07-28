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

func TestM2ckConfigAnnounceStreamHashSetOOM(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:             16,
		StreamNodeMaxEntries:  100,
		HashMaxListpackValue:  64,
		SetMaxListpackEntries: 128,
	}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-announce-ip")),
		[]string{"cluster-announce-ip", ""})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-announce-ip", "10.0.0.1")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-announce-ip")),
		[]string{"cluster-announce-ip", "10.0.0.1"})

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-announce-port", "6399")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-announce-port")),
		[]string{"cluster-announce-port", "6399"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-announce-bus-port", "16399")), "OK")

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "stream-node-max-entries")),
		[]string{"stream-node-max-entries", "100"})
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "hash-max-listpack-value")),
		[]string{"hash-max-listpack-value", "64"})
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "set-max-listpack-entries")),
		[]string{"set-max-listpack-entries", "128"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "oom-score-adj", "1")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "oom-score-adj")),
		[]string{"oom-score-adj", "1"})
}

func TestM2ckInfoEventloopFields(t *testing.T) {
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
		"eventloop_cycles:0",
		"eventloop_duration_sum:0",
		"eventloop_duration_max:0",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %s in: %s", want, s)
		}
	}
}

func TestM2ckCommandDocsSyscmdFlag(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	r := server.Exec(c, utils.ToCmdLine("COMMAND", "DOCS", "config"))
	raw := string(r.ToBytes())
	if !strings.Contains(raw, "doc_flags") {
		t.Fatalf("missing doc_flags: %s", raw)
	}
	if !strings.Contains(raw, "syscmd") {
		t.Fatalf("CONFIG docs want syscmd flag: %s", raw)
	}

	r = server.Exec(c, utils.ToCmdLine("COMMAND", "DOCS", "get"))
	raw = string(r.ToBytes())
	if strings.Contains(raw, "syscmd") {
		t.Fatalf("GET should not have syscmd: %s", raw)
	}
}

func TestM2ckLuaGeoSearchWithDistSetrespNumber(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("GEOADD", "g", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('GEOSEARCH', KEYS[1], 'FROMMEMBER', 'Palermo', 'BYRADIUS', '200', 'km', 'WITHDIST')
return type(t[1][2])
`, "1", "g"))
	asserts.AssertBulkReply(t, r, "number")
}
