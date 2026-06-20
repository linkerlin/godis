package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

func TestFTCreateMultiFieldSchema(t *testing.T) {
	db := makeTestDB()
	reply := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "articles", "SCHEMA",
		"title", "TEXT", "body", "TEXT",
	))
	if _, ok := reply.(*protocol.OkReply); !ok {
		t.Fatalf("expected OK, got %s", reply.ToBytes())
	}

	searchEnginesMu.RLock()
	engine, ok := searchEngines["articles"]
	searchEnginesMu.RUnlock()
	if !ok || engine == nil {
		t.Fatal("index should be registered")
	}
}

func TestFTCreateMultiFieldAddSearch(t *testing.T) {
	db := makeTestDB()
	if reply := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idx", "SCHEMA", "title", "TEXT", "body", "TEXT",
	)); protocol.IsErrorReply(reply) {
		t.Fatalf("create: %s", reply.ToBytes())
	}
	if reply := db.Exec(nil, utils.ToCmdLine(
		"FT.ADD", "idx", "doc1", "FIELDS", "title", "hello", "body", "world",
	)); protocol.IsErrorReply(reply) {
		t.Fatalf("add: %s", reply.ToBytes())
	}

	reply := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "idx", "hello"))
	multi, ok := reply.(*protocol.MultiBulkReply)
	if !ok || len(multi.Args) < 2 {
		t.Fatalf("search: %s", reply.ToBytes())
	}
	if string(multi.Args[0]) != "1" {
		t.Fatalf("expected 1 hit, got %s", multi.Args[0])
	}
}
