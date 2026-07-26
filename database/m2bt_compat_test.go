package database

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2btCommandGetKeysAndFlags(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("COMMAND", "GETKEYSANDFLAGS", "SET", "k", "v"))
	raw, ok := r.(*protocol.MultiRawReply)
	if !ok || len(raw.Replies) != 1 {
		t.Fatalf("GETKEYSANDFLAGS SET: %T %s", r, r.ToBytes())
	}
	entry := raw.Replies[0].(*protocol.MultiRawReply)
	if string(entry.Replies[0].(*protocol.BulkReply).Arg) != "k" {
		t.Fatalf("key: %s", entry.Replies[0].ToBytes())
	}
	flags := entry.Replies[1].(*protocol.MultiBulkReply)
	if len(flags.Args) != 1 || string(flags.Args[0]) != "W" {
		t.Fatalf("want W: %v", flags.Args)
	}

	r = db.Exec(nil, utils.ToCmdLine("COMMAND", "GETKEYSANDFLAGS", "GET", "k"))
	raw = r.(*protocol.MultiRawReply)
	entry = raw.Replies[0].(*protocol.MultiRawReply)
	flags = entry.Replies[1].(*protocol.MultiBulkReply)
	if string(flags.Args[0]) != "R" {
		t.Fatalf("want R: %v", flags.Args)
	}

	help := db.Exec(nil, utils.ToCmdLine("COMMAND", "HELP"))
	if !strings.Contains(string(help.ToBytes()), "GETKEYSANDFLAGS") {
		t.Fatalf("HELP missing GETKEYSANDFLAGS")
	}
}

func TestM2btInfoEverythingModulesLatency(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	for _, sec := range []string{"modules", "latency", "everything"} {
		r := server.Exec(c, utils.ToCmdLine("INFO", sec))
		bulk, ok := r.(*protocol.BulkReply)
		if !ok {
			t.Fatalf("INFO %s: %T %s", sec, r, r.ToBytes())
		}
		s := string(bulk.Arg)
		switch sec {
		case "modules":
			if !strings.Contains(s, "# Modules") {
				t.Fatalf("modules: %s", s)
			}
		case "latency":
			if !strings.Contains(s, "# Latency") {
				t.Fatalf("latency: %s", s)
			}
		case "everything":
			if !strings.Contains(s, "# Server") || !strings.Contains(s, "# Modules") {
				t.Fatalf("everything incomplete: %s", s)
			}
		}
	}
}

func TestM2btConfigReplicaAnnounceAndActiveDefrag(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{Databases: 16}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replica-announce-ip", "10.0.0.1")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "slave-announce-ip")),
		[]string{"slave-announce-ip", "10.0.0.1"})
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "replica-announce-ip")),
		[]string{"replica-announce-ip", "10.0.0.1"})

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "activedefrag", "yes")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "activedefrag")),
		[]string{"activedefrag", "yes"})
}

func TestM2btLuaXRangeSetrespMap(t *testing.T) {
	db := makeTestDB()
	InitScriptingEngine(db)
	_ = db.Exec(nil, utils.ToCmdLine("XADD", "s", "1-0", "f", "v"))

	r := db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(3)
local t = redis.call('XRANGE', KEYS[1], '-', '+')
return tostring(t[1][2]['f'])
`, "1", "s"))
	asserts.AssertBulkReply(t, r, "v")

	r = db.Exec(nil, utils.ToCmdLine("EVAL", `
redis.setresp(2)
local t = redis.call('XRANGE', KEYS[1], '-', '+')
return type(t[1][2]) .. ':' .. tostring(t[1][2][1])
`, "1", "s"))
	asserts.AssertBulkReply(t, r, "table:f")
}

func TestM2btInfoErrorstatsCounts(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	resetServerStats()
	defer resetServerStats()

	_ = server.Exec(c, utils.ToCmdLine("GET")) // arity error → ERR
	if atomic.LoadUint64(&serverStats.TotalErrorReplies) == 0 {
		t.Fatal("expected TotalErrorReplies > 0")
	}
	r := server.Exec(c, utils.ToCmdLine("INFO", "errorstats"))
	bulk := r.(*protocol.BulkReply)
	if !strings.Contains(string(bulk.Arg), "errorstat_ERR:count=") {
		t.Fatalf("want errorstat_ERR: %s", bulk.Arg)
	}
	_ = server.Exec(c, utils.ToCmdLine("CONFIG", "RESETSTAT"))
	r = server.Exec(c, utils.ToCmdLine("INFO", "errorstats"))
	if strings.Contains(string(r.(*protocol.BulkReply).Arg), "errorstat_") {
		t.Fatalf("after RESETSTAT want empty counts: %s", r.ToBytes())
	}
}
