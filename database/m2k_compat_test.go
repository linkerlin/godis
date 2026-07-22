package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2kFlushDBAsync(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "a", "1")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("FLUSHDB", "ASYNC")), "OK")
	asserts.AssertNullBulk(t, server.Exec(c, utils.ToCmdLine("GET", "a")))
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "b", "1")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("FLUSHDB", "SYNC")), "OK")
}

func TestM2kBGRewriteAOFWithoutAOF(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	old := server.persister
	server.persister = nil
	defer func() { server.persister = old }()
	r := server.Exec(c, utils.ToCmdLine("BGREWRITEAOF"))
	asserts.AssertStatusReply(t, r, "Background append only file rewriting started")
}

func TestM2kInfoRedisVersion(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("INFO", "server"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO: %T", r)
	}
	if !strings.Contains(string(bulk.Arg), "redis_version:") {
		t.Fatalf("INFO missing redis_version: %s", bulk.Arg)
	}
}

func TestM2kCFCompact(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("CF.ADD", "cf", "x"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("CF.COMPACT", "cf")), "OK")
}

func TestM2kJSONMergeResp(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("JSON.SET", "j", ".", `{"a":1,"b":{"c":2}}`))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"JSON.MERGE", "j", ".", `{"b":{"d":3},"a":null}`)), "OK")
	got := db.Exec(nil, utils.ToCmdLine("JSON.GET", "j"))
	s := string(got.(*protocol.BulkReply).Arg)
	if strings.Contains(s, `"a"`) || !strings.Contains(s, `"d"`) {
		t.Fatalf("JSON.MERGE result: %s", s)
	}
	r := db.Exec(nil, utils.ToCmdLine("JSON.RESP", "j", ".b"))
	if _, ok := r.(*protocol.MultiRawReply); !ok {
		t.Fatalf("JSON.RESP: %T %s", r, r.ToBytes())
	}
}

func TestM2kTDigestMinMax(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("TDIGEST.CREATE", "td"))
	db.Exec(nil, utils.ToCmdLine("TDIGEST.ADD", "td", "1", "5", "3"))
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("TDIGEST.MIN", "td")), "1")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("TDIGEST.MAX", "td")), "5")
}

func TestM2kReplicaOfAlias(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	// NO ONE should succeed like SLAVEOF NO ONE
	r := server.Exec(c, utils.ToCmdLine("REPLICAOF", "NO", "ONE"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("REPLICAOF NO ONE: %s", r.ToBytes())
	}
}
