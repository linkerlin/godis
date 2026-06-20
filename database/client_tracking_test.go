package database

import (
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

func TestClientTrackingCommandIntegration(t *testing.T) {
	resetClientCacheForTest(t)
	server := getTestServer()
	conn := &trackingTestConn{name: "integration", clientID: 42}

	reply := server.Exec(conn, utils.ToCmdLine("CLIENT", "TRACKING", "ON"))
	assertStatusOK(t, reply)

	if conn.GetTrackingID() == "" {
		t.Fatal("expected tracking id on connection")
	}
	if !IsTrackingEnabled(conn.GetTrackingID()) {
		t.Fatal("tracking should be enabled")
	}

	reply = server.Exec(conn, utils.ToCmdLine("SET", "track-key", "v1"))
	assertStatusOK(t, reply)

	reply = server.Exec(conn, utils.ToCmdLine("GET", "track-key"))
	assertBulkReply(t, reply, "v1")

	reply = server.Exec(conn, utils.ToCmdLine("SET", "track-key", "v2"))
	assertStatusOK(t, reply)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(conn.lastWrite(), "invalidate") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(conn.lastWrite(), "invalidate") {
		t.Fatalf("expected invalidation push after write, got %q", conn.lastWrite())
	}

	reply = server.Exec(conn, utils.ToCmdLine("CLIENT", "TRACKING", "OFF"))
	assertStatusOK(t, reply)
	if conn.GetTrackingID() != "" {
		t.Fatal("tracking id should be cleared")
	}

	server.AfterClientClose(conn)
	if GetTrackingClientsCount() != 0 {
		t.Fatalf("expected 0 tracking clients after close, got %d", GetTrackingClientsCount())
	}
}

func TestClientSetNameAndID(t *testing.T) {
	resetClientCacheForTest(t)
	server := getTestServer()
	conn := &trackingTestConn{clientID: 99}

	reply := server.Exec(conn, utils.ToCmdLine("CLIENT", "ID"))
	intReply, ok := reply.(*protocol.IntReply)
	if !ok || intReply.Code != 99 {
		t.Fatalf("expected client id 99, got %#v", reply)
	}

	reply = server.Exec(conn, utils.ToCmdLine("CLIENT", "SETNAME", "my-client"))
	assertStatusOK(t, reply)

	reply = server.Exec(conn, utils.ToCmdLine("CLIENT", "GETNAME"))
	bulk, ok := reply.(*protocol.BulkReply)
	if !ok || string(bulk.Arg) != "my-client" {
		t.Fatalf("expected client name my-client, got %#v", reply)
	}
}

func assertStatusOK(t *testing.T, reply redis.Reply) {
	t.Helper()
	switch r := reply.(type) {
	case *protocol.StatusReply:
		if r.Status != "OK" {
			t.Fatalf("expected OK, got %q", r.Status)
		}
	case *protocol.OkReply:
		// OK
	default:
		t.Fatalf("expected OK, got %#v", reply)
	}
}

func assertBulkReply(t *testing.T, reply redis.Reply, expected string) {
	t.Helper()
	bulk, ok := reply.(*protocol.BulkReply)
	if !ok || string(bulk.Arg) != expected {
		t.Fatalf("expected bulk %q, got %#v", expected, reply)
	}
}
