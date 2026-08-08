package database

import (
	"testing"
	"time"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestMiscServerCommands(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "mem-k", "value")), "OK")

	memStats := server.Exec(c, utils.ToCmdLine("MEMORY", "STATS"))
	if _, ok := memStats.(*protocol.MapReply); !ok {
		t.Fatalf("MEMORY STATS: got %T", memStats)
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("MEMORY", "PURGE")), "OK")
	usage := server.Exec(c, utils.ToCmdLine("MEMORY", "USAGE", "mem-k"))
	ir, ok := usage.(*protocol.IntReply)
	if !ok || ir.Code <= 0 {
		t.Fatalf("MEMORY USAGE expected >0, got %T %s", usage, usage.ToBytes())
	}
	asserts.AssertNullBulk(t, server.Exec(c, utils.ToCmdLine("MEMORY", "USAGE", "no-such-key")))

	timeReply := server.Exec(c, utils.ToCmdLine("TIME"))
	if _, ok := timeReply.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("TIME: got %s", timeReply.ToBytes())
	}

	RecordLatency("command", 2*time.Millisecond)
	lat := server.Exec(c, utils.ToCmdLine("LATENCY", "LATEST"))
	if _, ok := lat.(*protocol.MultiRawReply); !ok {
		t.Fatalf("LATENCY LATEST: got %T", lat)
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("LATENCY", "RESET")), "OK")

	for _, cmd := range []struct {
		name string
		line [][]byte
	}{
		{"PUBSUB CHANNELS", utils.ToCmdLine("PUBSUB", "CHANNELS")},
		{"PUBSUB NUMPAT", utils.ToCmdLine("PUBSUB", "NUMPAT")},
		{"MODULE LIST", utils.ToCmdLine("MODULE", "LIST")},
	} {
		reply := server.Exec(c, cmd.line)
		if protocol.IsErrorReply(reply) {
			t.Fatalf("%s: %s", cmd.name, reply.ToBytes())
		}
	}
}

func TestBloomFilterBasic(t *testing.T) {
	db := makeTestDB()

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"BF.RESERVE", "bf:1", "0.01", "100",
	)), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BF.ADD", "bf:1", "item")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BF.EXISTS", "bf:1", "item")), 1)

	info := db.Exec(nil, utils.ToCmdLine("BF.INFO", "bf:1"))
	if _, ok := info.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("BF.INFO: got %s", info.ToBytes())
	}
}

func TestCFAndCMSBasic(t *testing.T) {
	db := makeTestDB()

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"CF.RESERVE", "cf:1", "100",
	)), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("CF.ADD", "cf:1", "x")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("CF.EXISTS", "cf:1", "x")), 1)

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"CMS.INITBYDIM", "cms:1", "1000", "5",
	)), "OK")
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("CMS.INCRBY", "cms:1", "a", "3")), []string{"3"})
	asserts.AssertMultiBulkReply(t, db.Exec(nil, utils.ToCmdLine("CMS.QUERY", "cms:1", "a")), []string{"3"})
}

func TestTopKAndJSONBasic(t *testing.T) {
	db := makeTestDB()

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TOPK.RESERVE", "tk:1", "3")), "OK")
	add := db.Exec(nil, utils.ToCmdLine("TOPK.ADD", "tk:1", "a", "b", "c"))
	if _, ok := add.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("TOPK.ADD: got %s", add.ToBytes())
	}
	query := db.Exec(nil, utils.ToCmdLine("TOPK.QUERY", "tk:1", "a"))
	if protocol.IsErrorReply(query) {
		t.Fatalf("TOPK.QUERY: %s", query.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"JSON.SET", "j:1", "$", `{"n":1,"arr":[1,2]}`,
	)), "OK")
	get := db.Exec(nil, utils.ToCmdLine("JSON.GET", "j:1", "$.n"))
	if protocol.IsErrorReply(get) {
		t.Fatalf("JSON.GET: %s", get.ToBytes())
	}
	arrLen := db.Exec(nil, utils.ToCmdLine("JSON.ARRLEN", "j:1", "$.arr"))
	if protocol.IsErrorReply(arrLen) {
		t.Fatalf("JSON.ARRLEN: %s", arrLen.ToBytes())
	}

	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TS.CREATE", "ts:1")), "OK")
	tsAdd := db.Exec(nil, utils.ToCmdLine("TS.ADD", "ts:1", "*", "1.5"))
	if _, ok := tsAdd.(*protocol.IntReply); !ok {
		t.Fatalf("TS.ADD: got %s", tsAdd.ToBytes())
	}
}
