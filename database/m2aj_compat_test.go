package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2ajDumpRestoreStreamOpaque(t *testing.T) {
	db := makeTestDB()
	id := db.Exec(nil, utils.ToCmdLine("XADD", "s", "*", "a", "1"))
	if _, ok := id.(*protocol.BulkReply); !ok {
		t.Fatalf("XADD: %T %s", id, id.ToBytes())
	}
	dump := db.Exec(nil, utils.ToCmdLine("DUMP", "s"))
	bulk, ok := dump.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) == 0 {
		t.Fatalf("DUMP stream: %T %s", dump, dump.ToBytes())
	}
	db.Exec(nil, utils.ToCmdLine("DEL", "s"))
	restoreArgs := [][]byte{[]byte("RESTORE"), []byte("s2"), []byte("0"), bulk.Arg}
	asserts.AssertStatusReply(t, db.Exec(nil, restoreArgs), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "s2")), 1)
}

func TestM2ajDumpRestoreJSONOpaque(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"JSON.SET", "j", ".", `{"x":1,"y":"hi"}`)), "OK")
	dump := db.Exec(nil, utils.ToCmdLine("DUMP", "j"))
	bulk, ok := dump.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) == 0 {
		t.Fatalf("DUMP JSON: %T %s", dump, dump.ToBytes())
	}
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("DEL", "j")), 1)
	restoreArgs := [][]byte{[]byte("RESTORE"), []byte("j2"), []byte("0"), bulk.Arg}
	asserts.AssertStatusReply(t, db.Exec(nil, restoreArgs), "OK")
	got := db.Exec(nil, utils.ToCmdLine("JSON.GET", "j2", "."))
	b, ok := got.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(b.Arg), `"x"`) {
		t.Fatalf("RESTORE JSON: %T %s", got, got.ToBytes())
	}
}

func TestM2ajFTCreateOnType(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idxh", "ON", "HASH", "PREFIX", "1", "h:", "SCHEMA", "t", "TEXT")), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "idxj", "ON", "JSON", "PREFIX", "1", "j:", "SCHEMA", "t", "TEXT")), "OK")
	r := db.Exec(nil, utils.ToCmdLine(
		"FT.CREATE", "bad", "ON", "SET", "SCHEMA", "t", "TEXT"))
	asserts.AssertErrReply(t, r, "ERR Wrong type specified for ON. Expected HASH or JSON.")

	db.Exec(nil, utils.ToCmdLine("HSET", "h:1", "t", "hello"))
	db.Exec(nil, utils.ToCmdLine("HSET", "j:1", "t", "should-not-index-json-idx"))
	// HASH index should find h:1; JSON index must not pick up hash writes
	searchH := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "idxh", "hello"))
	if !strings.Contains(string(searchH.ToBytes()), "h:1") {
		t.Fatalf("HASH index miss: %s", searchH.ToBytes())
	}
	searchJ := db.Exec(nil, utils.ToCmdLine("FT.SEARCH", "idxj", "should-not-index-json-idx"))
	if strings.Contains(string(searchJ.ToBytes()), "j:1") {
		t.Fatalf("JSON index should not auto-index HSET: %s", searchJ.ToBytes())
	}
}

func TestM2ajClientListTypeFilter(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	RegisterClient(c)
	defer UnregisterClient(c)
	c.SetClientName("norm")

	pub := connection.NewFakeConn()
	RegisterClient(pub)
	defer UnregisterClient(pub)
	pub.Subscribe("ch")

	all := server.Exec(c, utils.ToCmdLine("CLIENT", "LIST"))
	ab, ok := all.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(ab.Arg), "norm") {
		t.Fatalf("CLIENT LIST: %s", all.ToBytes())
	}
	if !strings.Contains(string(ab.Arg), "psub=") {
		t.Fatalf("CLIENT LIST missing psub: %s", ab.Arg)
	}

	normal := server.Exec(c, utils.ToCmdLine("CLIENT", "LIST", "TYPE", "normal"))
	nb, _ := normal.(*protocol.BulkReply)
	if !strings.Contains(string(nb.Arg), "norm") || strings.Contains(string(nb.Arg), "flags=P") {
		t.Fatalf("TYPE normal: %q", string(nb.Arg))
	}

	pubsub := server.Exec(c, utils.ToCmdLine("CLIENT", "LIST", "TYPE", "pubsub"))
	pb, _ := pubsub.(*protocol.BulkReply)
	if !strings.Contains(string(pb.Arg), "flags=P") {
		t.Fatalf("TYPE pubsub: %q", string(pb.Arg))
	}
	if strings.Contains(string(pb.Arg), "name=norm") {
		t.Fatalf("TYPE pubsub should exclude normal: %q", string(pb.Arg))
	}
}
