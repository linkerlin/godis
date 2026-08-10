package database

import (
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2buModuleLoadEx(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	r := server.Exec(c, utils.ToCmdLine("MODULE", "LOADEX", "/tmp/x.so"))
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "does not support") {
		t.Fatalf("LOADEX: %s", r.ToBytes())
	}
	help := server.Exec(c, utils.ToCmdLine("MODULE", "HELP"))
	if !strings.Contains(string(help.ToBytes()), "LOADEX") {
		t.Fatalf("HELP missing LOADEX: %s", help.ToBytes())
	}
}

func TestM2buLatencyHistogram(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	// Seed a sample so RESET emptiness is meaningful even on a cold process.
	RecordCommandLatency("set", 2*time.Millisecond)
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("LATENCY", "RESET")), "OK")

	r := server.Exec(c, utils.ToCmdLine("LATENCY", "HISTOGRAM"))
	m, ok := r.(*protocol.MapReply)
	if !ok || len(m.Data) != 0 {
		t.Fatalf("HISTOGRAM want empty map after RESET: %T %s", r, r.ToBytes())
	}
	help := server.Exec(c, utils.ToCmdLine("LATENCY", "HELP"))
	if !strings.Contains(string(help.ToBytes()), "HISTOGRAM") {
		t.Fatalf("HELP missing HISTOGRAM")
	}
}

func TestM2buReplBacklogSizeConfig(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{Databases: 16}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()

	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "repl-backlog-size")),
		[]string{"repl-backlog-size", "10485760"})
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-backlog-size", "2097152")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "repl-backlog-size")),
		[]string{"repl-backlog-size", "2097152"})
}

func TestM2buCommandListFilterByAclCat(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("COMMAND", "LIST", "FILTERBY", "ACLCAT", "@read"))
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("ACLCAT @read: %T %s", r, r.ToBytes())
	}
	joined := string(bytesJoin(mb.Args))
	if !strings.Contains(joined, "get") {
		t.Fatalf("want get in @read: %s", joined)
	}
	if strings.Contains(" "+joined+" ", " set ") {
		t.Fatalf("set should not be in @read: %s", joined)
	}

	empty := db.Exec(nil, utils.ToCmdLine("COMMAND", "LIST", "FILTERBY", "ACLCAT", "@nosuch"))
	if _, ok := empty.(*protocol.EmptyMultiBulkReply); !ok {
		if mb2, ok2 := empty.(*protocol.MultiBulkReply); !ok2 || len(mb2.Args) != 0 {
			t.Fatalf("unknown cat want empty: %T %s", empty, empty.ToBytes())
		}
	}
}
