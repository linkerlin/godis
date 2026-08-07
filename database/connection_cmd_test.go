package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestConnectionCommandsViaServer(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("PING")), "PONG")
	asserts.AssertBulkReply(t, server.Exec(c, utils.ToCmdLine("PING", "hello")), "hello")
	asserts.AssertBulkReply(t, server.Exec(c, utils.ToCmdLine("ECHO", "world")), "world")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("QUIT")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("READONLY")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("READWRITE")), "OK")
}

func TestConnectionCommandsViaDB(t *testing.T) {
	db := makeTestDB()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, db.Exec(c, utils.ToCmdLine("PING")), "PONG")
	asserts.AssertBulkReply(t, db.Exec(c, utils.ToCmdLine("PING", "db")), "db")
	asserts.AssertBulkReply(t, db.Exec(c, utils.ToCmdLine("ECHO", "echoed")), "echoed")
	asserts.AssertStatusReply(t, db.Exec(c, utils.ToCmdLine("QUIT")), "OK")
	asserts.AssertStatusReply(t, db.Exec(c, utils.ToCmdLine("READONLY")), "OK")
	asserts.AssertStatusReply(t, db.Exec(c, utils.ToCmdLine("READWRITE")), "OK")

	list := db.Exec(c, utils.ToCmdLine("CLIENT", "LIST"))
	if _, ok := list.(*protocol.BulkReply); !ok {
		t.Fatalf("CLIENT LIST: got %s", list.ToBytes())
	}
	c.SetProtocolVersion(3)
	asserts.AssertStatusReply(t, db.Exec(c, utils.ToCmdLine("CLIENT", "REPLY", "ON")), "OK")
	asserts.AssertStatusReply(t, db.Exec(c, utils.ToCmdLine("CLIENT", "TRACKING", "ON")), "OK")
	asserts.AssertStatusReply(t, db.Exec(c, utils.ToCmdLine("CLIENT", "CACHING", "YES")), "OK")
	trackingInfo := db.Exec(c, utils.ToCmdLine("CLIENT", "TRACKINGINFO"))
	if _, ok := trackingInfo.(*protocol.MultiRawReply); !ok {
		t.Fatalf("CLIENT TRACKINGINFO via DB: got %s", trackingInfo.ToBytes())
	}
}

func TestClientSubcommands(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	c.SetProtocolVersion(3)

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CLIENT", "SETNAME", "my-client")), "OK")
	asserts.AssertBulkReply(t, server.Exec(c, utils.ToCmdLine("CLIENT", "GETNAME")), "my-client")
	clientID := server.Exec(c, utils.ToCmdLine("CLIENT", "ID"))
	if _, ok := clientID.(*protocol.IntReply); !ok {
		t.Fatalf("CLIENT ID: got %s", clientID.ToBytes())
	}

	info := server.Exec(c, utils.ToCmdLine("CLIENT", "INFO"))
	if _, ok := info.(*protocol.BulkReply); !ok {
		t.Fatalf("CLIENT INFO: got %s", info.ToBytes())
	}

	list := server.Exec(c, utils.ToCmdLine("CLIENT", "LIST"))
	if _, ok := list.(*protocol.BulkReply); !ok {
		t.Fatalf("CLIENT LIST: got %s", list.ToBytes())
	}

	help := server.Exec(c, utils.ToCmdLine("CLIENT", "HELP"))
	if !strings.Contains(string(help.ToBytes()), "SETNAME") {
		t.Fatalf("CLIENT HELP: %s", help.ToBytes())
	}

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CLIENT", "REPLY", "ON")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CLIENT", "TRACKING", "ON")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CLIENT", "CACHING", "YES")), "OK")
	redir := server.Exec(c, utils.ToCmdLine("CLIENT", "GETREDIR"))
	if _, ok := redir.(*protocol.IntReply); !ok {
		t.Fatalf("CLIENT GETREDIR: got %s", redir.ToBytes())
	}

	trackingInfo := server.Exec(c, utils.ToCmdLine("CLIENT", "TRACKINGINFO"))
	if _, ok := trackingInfo.(*protocol.MultiRawReply); !ok {
		t.Fatalf("CLIENT TRACKINGINFO: got %s", trackingInfo.ToBytes())
	}
}

func TestCommandDocs(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	all := server.Exec(c, utils.ToCmdLine("COMMAND", "DOCS"))
	multi, ok := all.(*protocol.MultiBulkReply)
	if !ok || len(multi.Args) < 2 {
		t.Fatalf("COMMAND DOCS: got %s", all.ToBytes())
	}

	one := server.Exec(c, utils.ToCmdLine("COMMAND", "DOCS", "set"))
	if _, ok := one.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("COMMAND DOCS set: got %s", one.ToBytes())
	}
}
