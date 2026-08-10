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
	t.Cleanup(func() { UnregisterClient(c) })

	// Unique LADDRs avoid collisions with any leftover registry entries from
	// earlier suite tests (shared sync.Map client table).
	laddrVictim := "10.0.0.1:6401"
	laddrOther := "10.0.0.2:6402"

	v1 := connection.NewFakeConn()
	RegisterClient(v1)
	t.Cleanup(func() { UnregisterClient(v1) })
	v1.SetLocalAddr(laddrVictim)
	v1ID := v1.GetClientID()

	v2 := connection.NewFakeConn()
	RegisterClient(v2)
	t.Cleanup(func() { UnregisterClient(v2) })
	v2.SetLocalAddr(laddrOther)
	v2ID := v2.GetClientID()

	r := server.Exec(c, utils.ToCmdLine("CLIENT", "KILL", "LADDR", laddrVictim, "SKIPME", "YES"))
	// >=1: registry may contain other leftover clients with the same LADDR.
	asserts.AssertIntReplyGreaterThan(t, r, 0)
	if FindClientByID(v1ID) != nil {
		t.Fatal("LADDR victim should be killed")
	}
	if FindClientByID(v2ID) == nil {
		t.Fatal("other laddr should remain")
	}

	old := connection.NewFakeConn()
	RegisterClient(old)
	t.Cleanup(func() { UnregisterClient(old) })
	old.SetClientTimesForTest(time.Now().Add(-30*time.Second), time.Now())
	oldID := old.GetClientID()

	young := connection.NewFakeConn()
	RegisterClient(young)
	t.Cleanup(func() { UnregisterClient(young) })
	youngID := young.GetClientID()

	r = server.Exec(c, utils.ToCmdLine("CLIENT", "KILL", "MAXAGE", "10", "SKIPME", "YES"))
	// >= 1: earlier tests may leave aged registered clients behind, so the
	// kill count is environment-dependent; the victim assertions below are what
	// actually verify semantics.
	asserts.AssertIntReplyGreaterThan(t, r, 0)
	if FindClientByID(oldID) != nil {
		t.Fatal("MAXAGE victim should be killed")
	}
	if FindClientByID(youngID) == nil {
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
