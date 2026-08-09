package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestACLBasicCommands(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	asserts.AssertBulkReply(t, server.Exec(c, utils.ToCmdLine("ACL", "WHOAMI")), "default")

	list := server.Exec(c, utils.ToCmdLine("ACL", "LIST"))
	if _, ok := list.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("ACL LIST: got %T", list)
	}

	help := server.Exec(c, utils.ToCmdLine("ACL", "HELP"))
	if !strings.Contains(string(help.ToBytes()), "SETUSER") {
		t.Fatalf("ACL HELP: %s", help.ToBytes())
	}

	pass := server.Exec(c, utils.ToCmdLine("ACL", "GENPASS"))
	bulk, ok := pass.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) < 8 {
		t.Fatalf("ACL GENPASS: got %v", pass)
	}
}

func TestHelloCommand(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	reply := server.Exec(c, utils.ToCmdLine("HELLO", "3"))
	m, ok := reply.(*protocol.MapReply)
	if !ok {
		t.Fatalf("HELLO 3: got %T", reply)
	}
	if _, exists := m.Data["proto"]; !exists {
		t.Fatalf("HELLO missing proto: %v", m.Data)
	}
	if reply.ToBytes()[0] != '*' {
		t.Fatalf("HELLO 3 RESP2 wire should be array: %q", reply.ToBytes())
	}
	if protocol.ReplyToRESP3(reply)[0] != '%' {
		t.Fatalf("HELLO 3 RESP3 wire should be map: %q", protocol.ReplyToRESP3(reply))
	}

	c2 := connection.NewFakeConn()
	r2 := server.Exec(c2, utils.ToCmdLine("HELLO", "2"))
	if _, ok := r2.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("HELLO 2: got %T", r2)
	}
	if r2.ToBytes()[0] != '*' {
		t.Fatalf("HELLO 2 wire: %q", r2.ToBytes())
	}
}

func TestACLUsersCatDelUserDryRun(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	users := server.Exec(c, utils.ToCmdLine("ACL", "USERS"))
	if _, ok := users.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("ACL USERS: got %T", users)
	}

	cat := server.Exec(c, utils.ToCmdLine("ACL", "CAT"))
	catMulti, ok := cat.(*protocol.MultiBulkReply)
	if !ok || len(catMulti.Args) == 0 {
		t.Fatalf("ACL CAT: got %s", cat.ToBytes())
	}

	readCat := server.Exec(c, utils.ToCmdLine("ACL", "CAT", "@read"))
	if _, ok := readCat.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("ACL CAT @read: got %s", readCat.ToBytes())
	}

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"ACL", "SETUSER", "testuser", "on", ">pass", "+get",
	)), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("ACL", "DRYRUN", "testuser", "GET")), "OK")
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("ACL", "DELUSER", "testuser")), 1)

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("ACL", "LOG", "RESET")), "OK")
	log := server.Exec(c, utils.ToCmdLine("ACL", "LOG"))
	mr, ok := log.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) != 0 {
		t.Fatalf("ACL LOG after RESET: got %T %s", log, log.ToBytes())
	}
}

func TestServerGetAvgTTL(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "ttl-k1", "v")), "OK")
	asserts.AssertIntReply(t, server.Exec(c, utils.ToCmdLine("EXPIRE", "ttl-k1", "3600")), 1)

	avg, err := server.GetAvgTTL(0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if avg <= 0 {
		t.Fatalf("expected positive avg TTL, got %d", avg)
	}
}

func TestStreamXInfoGroups(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("XADD", "s:info", "*", "f", "v"))
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "s:info", "g", "$")), "OK")

	// Legacy concatenated name
	reply := db.Exec(nil, utils.ToCmdLine("XINFOGROUPS", "s:info"))
	if _, ok := reply.(*protocol.MultiRawReply); !ok {
		t.Fatalf("XINFOGROUPS: got %T %s", reply, reply.ToBytes())
	}

	// Standard spaced form
	spaced := db.Exec(nil, utils.ToCmdLine("XINFO", "GROUPS", "s:info"))
	if _, ok := spaced.(*protocol.MultiRawReply); !ok {
		t.Fatalf("XINFO GROUPS: got %T %s", spaced, spaced.ToBytes())
	}
}
