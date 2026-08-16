package database

import (
	"testing"
	"time"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps49LatencyResetAndCommandDocsUnknown(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertIntReply(t, srv.Exec(c, utils.ToCmdLine("LATENCY", "RESET")), 0)
	asserts.AssertIntReply(t, srv.Exec(c, utils.ToCmdLine("LATENCY", "RESET", "nosuch")), 0)

	RecordLatency("command", 2*time.Millisecond)
	asserts.AssertIntReply(t, srv.Exec(c, utils.ToCmdLine("LATENCY", "RESET")), 1)
	RecordLatency("command", time.Millisecond)
	RecordLatency("fork", time.Millisecond)
	asserts.AssertIntReply(t, srv.Exec(c, utils.ToCmdLine("LATENCY", "RESET", "command")), 1)
	asserts.AssertIntReply(t, srv.Exec(c, utils.ToCmdLine("LATENCY", "RESET", "fork")), 1)

	docs := srv.Exec(c, utils.ToCmdLine("COMMAND", "DOCS", "nosuchcmd"))
	m, ok := docs.(*protocol.MapReply)
	if !ok || len(m.Data) != 0 {
		t.Fatalf("COMMAND DOCS unknown want empty map, got %T %s", docs, docs.ToBytes())
	}
	docs = srv.Exec(c, utils.ToCmdLine("COMMAND", "DOCS", "get", "nosuchcmd"))
	m, ok = docs.(*protocol.MapReply)
	if !ok || len(m.Data) != 1 {
		t.Fatalf("COMMAND DOCS get+unknown want 1 entry, got %T len=%d %s", docs, len(m.Data), docs.ToBytes())
	}
	if _, ok := m.Data["get"]; !ok {
		t.Fatalf("COMMAND DOCS missing get: %v", m.Data)
	}
}
