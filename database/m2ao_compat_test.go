package database

import (
	"strconv"
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/pubsub"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2aoObjectEncodingHLLVector(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("PFADD", "h", "a"))
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "h")), "hyperloglog")

	db.Exec(nil, utils.ToCmdLine("VADD", "v", "VALUES", "2", "1", "0", "ELE", "e1"))
	enc := db.Exec(nil, utils.ToCmdLine("OBJECT", "ENCODING", "v"))
	asserts.AssertBulkReply(t, enc, "vectorset")
}

func TestM2aoXInfoFullCount(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("XADD", "s", "*", "f", "1"))
	db.Exec(nil, utils.ToCmdLine("XADD", "s", "*", "f", "2"))
	db.Exec(nil, utils.ToCmdLine("XADD", "s", "*", "f", "3"))
	r := db.Exec(nil, utils.ToCmdLine("XINFO", "STREAM", "s", "FULL", "COUNT", "2"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok {
		t.Fatalf("FULL reply type: %T %s", r, r.ToBytes())
	}
	found := false
	for i := 0; i+1 < len(mr.Replies); i += 2 {
		k, ok1 := mr.Replies[i].(*protocol.BulkReply)
		if !ok1 || string(k.Arg) != "entries" {
			continue
		}
		ents, ok2 := mr.Replies[i+1].(*protocol.MultiRawReply)
		if !ok2 {
			t.Fatalf("entries not nested: %T", mr.Replies[i+1])
		}
		if len(ents.Replies) != 2 {
			t.Fatalf("COUNT 2 want 2 entries, got %d", len(ents.Replies))
		}
		found = true
	}
	if !found {
		t.Fatalf("entries missing: %s", r.ToBytes())
	}
}

func TestM2aoFTSearchUnknownOption(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx", "SCHEMA", "t", "TEXT",
	)), "OK")
	r := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "idx", "hello", "NOTANOPTION"))
	asserts.AssertErrReply(t, r, "ERR syntax error")
	r2 := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "idx", "*", "VERBATIM", "NOSTOPWORDS"))
	if protocol.IsErrorReply(r2) {
		t.Fatalf("VERBATIM/NOSTOPWORDS should be accepted: %s", r2.ToBytes())
	}
}

func TestM2aoPubSubRESP3Null(t *testing.T) {
	c := connection.NewFakeConn()
	c.SetProtocolVersion(3)
	hub := pubsub.MakeHub()
	_ = pubsub.UnSubscribe(hub, c, nil)
	out := string(c.Bytes())
	if !strings.Contains(out, "_\r\n") || strings.Contains(out, "$-1\r\n") {
		t.Fatalf("RESP3 unsubscribe null want _, got %q", out)
	}
}

func TestM2apMemoryHelp(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("MEMORY", "HELP"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) < 3 {
		t.Fatalf("MEMORY HELP: %T %s", r, r.ToBytes())
	}
}

func TestM2apClientUnknownSubcommand(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("CLIENT", "NOTREAL"))
	asserts.AssertErrReply(t, r, "ERR Unknown subcommand or wrong number of arguments for 'NOTREAL'")
}

func TestM2apCFExpansion(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"CF.RESERVE", "cf", "2", "BUCKETSIZE", "1", "EXPANSION", "2",
	)), "OK")
	for i := 0; i < 8; i++ {
		r := db.Exec(nil, utils.ToCmdLine("CF.ADD", "cf", "item"+strconv.Itoa(i)))
		if protocol.IsErrorReply(r) {
			t.Fatalf("CF.ADD %d: %s", i, r.ToBytes())
		}
	}
}

func TestM2aqTDigestWeights(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TDIGEST.CREATE", "td")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"TDIGEST.ADD", "td", "VALUES", "10", "20", "WEIGHTS", "1", "3",
	)), "OK")
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("TDIGEST.MIN", "td")), "10")
}
