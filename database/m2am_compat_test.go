package database

import (
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2amClientKillLAddrMaxAge(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	RegisterClient(c)
	defer UnregisterClient(c)

	v1 := connection.NewFakeConn()
	RegisterClient(v1)
	defer UnregisterClient(v1)
	v1.SetLocalAddr("10.0.0.1:6399")

	v2 := connection.NewFakeConn()
	RegisterClient(v2)
	defer UnregisterClient(v2)
	v2.SetLocalAddr("10.0.0.2:6399")

	r := server.Exec(c, utils.ToCmdLine("CLIENT", "KILL", "LADDR", "10.0.0.1:6399", "SKIPME", "YES"))
	asserts.AssertIntReply(t, r, 1)
	if FindClientByID(v1.GetClientID()) != nil {
		t.Fatal("LADDR victim should be killed")
	}
	if FindClientByID(v2.GetClientID()) == nil {
		t.Fatal("other laddr should remain")
	}

	old := connection.NewFakeConn()
	RegisterClient(old)
	defer UnregisterClient(old)
	old.SetClientTimesForTest(time.Now().Add(-30*time.Second), time.Now())

	young := connection.NewFakeConn()
	RegisterClient(young)
	defer UnregisterClient(young)

	r = server.Exec(c, utils.ToCmdLine("CLIENT", "KILL", "MAXAGE", "10", "SKIPME", "YES"))
	asserts.AssertIntReply(t, r, 1)
	if FindClientByID(old.GetClientID()) != nil {
		t.Fatal("MAXAGE victim should be killed")
	}
	if FindClientByID(young.GetClientID()) == nil {
		t.Fatal("young client should remain")
	}

	list := server.Exec(c, utils.ToCmdLine("CLIENT", "LIST"))
	bulk, ok := list.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), "laddr=") {
		t.Fatalf("CLIENT LIST missing laddr: %s", list.ToBytes())
	}
}

func TestM2amHelloRoleSlave(t *testing.T) {
	c := connection.NewFakeConn()
	r := HelloWithRole(c, utils.ToCmdLine("2"), "slave")
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("HELLO: %T", r)
	}
	found := false
	for i := 0; i+1 < len(mr.Args); i += 2 {
		if string(mr.Args[i]) == "role" && string(mr.Args[i+1]) == "slave" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("HELLO role slave missing: %s", r.ToBytes())
	}
	_ = strconv.FormatInt(c.GetClientID(), 10)
}

func TestM2amDumpRestoreCuckoo(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("CF.RESERVE", "cf", "100")), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("CF.ADD", "cf", "item")), 1)
	dump := db.Exec(nil, utils.ToCmdLine("DUMP", "cf"))
	bulk, ok := dump.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) == 0 {
		t.Fatalf("DUMP cuckoo: %T %s", dump, dump.ToBytes())
	}
	db.Exec(nil, utils.ToCmdLine("DEL", "cf"))
	asserts.AssertStatusReply(t, db.Exec(nil, [][]byte{[]byte("RESTORE"), []byte("cf2"), []byte("0"), bulk.Arg}), "OK")
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("CF.EXISTS", "cf2", "item")), 1)
}
