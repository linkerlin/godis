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

func TestM2wVSetGetAttrAndRange(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "vw", "VALUES", "2", "1", "0", "ELE", "apple",
	)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VADD", "vw", "VALUES", "2", "0", "1", "ELE", "banana",
	)), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine(
		"VSETATTR", "vw", "apple", `{"color":"red"}`,
	)), 1)
	asserts.AssertBulkReply(t, db.Exec(nil, utils.ToCmdLine("VGETATTR", "vw", "apple")), `{"color":"red"}`)
	asserts.AssertNullBulk(t, db.Exec(nil, utils.ToCmdLine("VGETATTR", "vw", "banana")))
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("VSETATTR", "vw", "missing", `{}`)), 0)

	rng := db.Exec(nil, utils.ToCmdLine("VRANGE", "vw", "-", "+", "10"))
	mr, ok := rng.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 2 {
		t.Fatalf("VRANGE: %s", rng.ToBytes())
	}
	links := db.Exec(nil, utils.ToCmdLine("VLINKS", "vw", "apple"))
	if protocol.IsErrorReply(links) {
		t.Fatalf("VLINKS: %s", links.ToBytes())
	}
}

func TestM2wClientNoTouchAndReply(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "nt", "1")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CLIENT", "NO-TOUCH", "ON")), "OK")
	if !c.GetNoTouch() {
		t.Fatal("NO-TOUCH ON failed")
	}
	_ = server.Exec(c, utils.ToCmdLine("GET", "nt"))
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CLIENT", "NO-TOUCH", "OFF")), "OK")

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CLIENT", "REPLY", "SKIP")), "OK")
	if !c.ShouldSuppressReply() {
		t.Fatal("REPLY SKIP should suppress next")
	}
	if c.ShouldSuppressReply() {
		t.Fatal("SKIP should only suppress once")
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CLIENT", "REPLY", "OFF")), "OK")
	if !c.ShouldSuppressReply() {
		t.Fatal("REPLY OFF should suppress")
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CLIENT", "REPLY", "ON")), "OK")
	if c.ShouldSuppressReply() {
		t.Fatal("REPLY ON should not suppress")
	}
}

func TestM2wReplConfGetAck(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	c.SetMaster()
	r := server.Exec(c, utils.ToCmdLine("REPLCONF", "GETACK", "*"))
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) < 3 || !strings.EqualFold(string(mr.Args[0]), "REPLCONF") {
		t.Fatalf("GETACK reply: %s", r.ToBytes())
	}
	if !strings.EqualFold(string(mr.Args[1]), "ACK") {
		t.Fatalf("expected ACK, got %s", r.ToBytes())
	}
}

func TestM2wTimeoutConfigStillRoundTrips(t *testing.T) {
	config.Properties = &config.ServerProperties{Databases: 1, Timeout: 0}
	server, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "timeout", "1")), "OK")
	if config.Properties.Timeout != 1 {
		t.Fatalf("timeout=%d", config.Properties.Timeout)
	}
	// Idle deadline is applied in std.Handler; here we only verify config wiring.
	_ = time.Second
}
