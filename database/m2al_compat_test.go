package database

import (
	"strconv"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2alHelloClientID(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("HELLO", "2"))
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) < 8 {
		t.Fatalf("HELLO: %T %s", r, r.ToBytes())
	}
	want := strconv.FormatInt(c.GetClientID(), 10)
	found := false
	for i := 0; i+1 < len(mr.Args); i += 2 {
		if string(mr.Args[i]) == "id" && string(mr.Args[i+1]) == want {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("HELLO id want %s got %s", want, r.ToBytes())
	}
}

func TestM2alRoleMaster(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("ROLE"))
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) < 3 {
		t.Fatalf("ROLE: %T %s", r, r.ToBytes())
	}
	bulk, ok := mr.Replies[0].(*protocol.BulkReply)
	if !ok || string(bulk.Arg) != "master" {
		t.Fatalf("ROLE role: %s", r.ToBytes())
	}
}

func TestM2alClientKillTypeUser(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	RegisterClient(c)
	defer UnregisterClient(c)

	victim := connection.NewFakeConn()
	RegisterClient(victim)
	defer UnregisterClient(victim)
	victim.SetACLUser("bob")
	victim.Subscribe("ch")

	r := server.Exec(c, utils.ToCmdLine("CLIENT", "KILL", "TYPE", "pubsub", "SKIPME", "YES"))
	asserts.AssertIntReply(t, r, 1)
	if FindClientByID(victim.GetClientID()) != nil {
		t.Fatal("pubsub victim should be killed")
	}

	u1 := connection.NewFakeConn()
	RegisterClient(u1)
	defer UnregisterClient(u1)
	u1.SetACLUser("alice")
	u2 := connection.NewFakeConn()
	RegisterClient(u2)
	defer UnregisterClient(u2)
	u2.SetACLUser("bob")

	r = server.Exec(c, utils.ToCmdLine("CLIENT", "KILL", "USER", "alice", "SKIPME", "YES"))
	asserts.AssertIntReply(t, r, 1)
	if FindClientByID(u1.GetClientID()) != nil {
		t.Fatal("alice should be killed")
	}
	if FindClientByID(u2.GetClientID()) == nil {
		t.Fatal("bob should remain")
	}
}

func TestM2alDumpRestoreBloomAndHLL(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("BF.RESERVE", "bf", "0.01", "100")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BF.ADD", "bf", "x")), 1)
	dump := db.Exec(nil, utils.ToCmdLine("DUMP", "bf"))
	bulk, ok := dump.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) == 0 {
		t.Fatalf("DUMP bloom: %T %s", dump, dump.ToBytes())
	}
	db.Exec(nil, utils.ToCmdLine("DEL", "bf"))
	asserts.AssertStatusReply(t, db.Exec(nil, [][]byte{[]byte("RESTORE"), []byte("bf2"), []byte("0"), bulk.Arg}), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("BF.EXISTS", "bf2", "x")), 1)

	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "h", "a", "b", "c")), 1)
	hdump := db.Exec(nil, utils.ToCmdLine("DUMP", "h"))
	hb, ok := hdump.(*protocol.BulkReply)
	if !ok || len(hb.Arg) == 0 {
		t.Fatalf("DUMP hll: %T %s", hdump, hdump.ToBytes())
	}
	db.Exec(nil, utils.ToCmdLine("DEL", "h"))
	asserts.AssertStatusReply(t, db.Exec(nil, [][]byte{[]byte("RESTORE"), []byte("h2"), []byte("0"), hb.Arg}), "OK")
	cnt := db.Exec(nil, utils.ToCmdLine("PFCOUNT", "h2"))
	ir, ok := cnt.(*protocol.IntReply)
	if !ok || ir.Code < 1 {
		t.Fatalf("PFCOUNT after RESTORE: %T %s", cnt, cnt.ToBytes())
	}
}
