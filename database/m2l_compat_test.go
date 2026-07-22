package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2lReset(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	c.SelectDB(2)
	c.SetClientName("tmp")
	c.SetMultiState(true)
	c.EnqueueCmd(utils.ToCmdLine("SET", "x", "1"))
	r := db.Exec(c, utils.ToCmdLine("RESET"))
	asserts.AssertStatusReply(t, r, "RESET")
	if c.GetDBIndex() != 0 || c.GetClientName() != "" || c.InMultiState() {
		t.Fatalf("RESET did not clear state: db=%d name=%q multi=%v",
			c.GetDBIndex(), c.GetClientName(), c.InMultiState())
	}
}

func TestM2lClientInfoSetInfo(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, db.Exec(c, utils.ToCmdLine("CLIENT", "SETINFO", "LIB-NAME", "go-redis")), "OK")
	asserts.AssertStatusReply(t, db.Exec(c, utils.ToCmdLine("CLIENT", "SETINFO", "LIB-VER", "9.0")), "OK")
	info := db.Exec(c, utils.ToCmdLine("CLIENT", "INFO"))
	bulk, ok := info.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("CLIENT INFO: %T", info)
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "lib-name=go-redis") || !strings.Contains(s, "lib-ver=9.0") {
		t.Fatalf("CLIENT INFO missing lib fields: %s", s)
	}
	if !strings.Contains(s, "id=") {
		t.Fatalf("CLIENT INFO missing id: %s", s)
	}
}

func TestM2lACLWhoami(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()
	c.SetACLUser("alice")
	r := db.Exec(c, utils.ToCmdLine("ACL", "WHOAMI"))
	asserts.AssertBulkReply(t, r, "alice")
}

func TestM2lJSONDebug(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("JSON.SET", "j", ".", `{"a":1,"b":[2,3]}`))
	mem := db.Exec(nil, utils.ToCmdLine("JSON.DEBUG", "MEMORY", "j"))
	ir, ok := mem.(*protocol.IntReply)
	if !ok || ir.Code <= 0 {
		t.Fatalf("JSON.DEBUG MEMORY: %T %s", mem, mem.ToBytes())
	}
	fields := db.Exec(nil, utils.ToCmdLine("JSON.DEBUG", "FIELDS", "j"))
	asserts.AssertIntReplyGreaterThan(t, fields, 0)
}

func TestM2lBFInsert(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine(
		"BF.INSERT", "bf", "CAPACITY", "100", "ERROR", "0.01", "ITEMS", "a", "b"))
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 2 {
		t.Fatalf("BF.INSERT: %T %s", r, r.ToBytes())
	}
}

func TestM2lTDigestResetRank(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("TDIGEST.CREATE", "td"))
	db.Exec(nil, utils.ToCmdLine("TDIGEST.ADD", "td", "1", "2", "3"))
	r := db.Exec(nil, utils.ToCmdLine("TDIGEST.RANK", "td", "2"))
	if _, ok := r.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("TDIGEST.RANK: %T %s", r, r.ToBytes())
	}
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("TDIGEST.RESET", "td")), "OK")
}

func TestM2lXDelex(t *testing.T) {
	db := makeTestDB()
	idReply := db.Exec(nil, utils.ToCmdLine("XADD", "s", "*", "f", "v"))
	id := string(idReply.(*protocol.BulkReply).Arg)
	db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "s", "g", "0-0"))
	db.Exec(nil, utils.ToCmdLine("XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">"))
	// ACKED should not delete while pending
	r := db.Exec(nil, utils.ToCmdLine("XDELEX", "s", "ACKED", "IDS", "1", id))
	asserts.AssertIntReply(t, r, 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "s")), 1)
	// DELREF deletes and clears PEL
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XDELEX", "s", "DELREF", "IDS", "1", id)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("XLEN", "s")), 0)
}
