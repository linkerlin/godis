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

func TestM2ahTrackingResp2RequiresRedirect(t *testing.T) {
	c := &trackingTestConn{name: "r2", clientID: 501, protocol: 2}
	r := execClientTrackingConn(c, [][]byte{[]byte("ON")})
	if !protocol.IsErrorReply(r) || !strings.Contains(string(r.ToBytes()), "REDIRECT") {
		t.Fatalf("RESP2 without REDIRECT: %s", r.ToBytes())
	}
}

func TestM2ahTrackingRedirectAndGetRedir(t *testing.T) {
	data := &trackingTestConn{name: "data", clientID: 601, protocol: 2}
	inv := &trackingTestConn{name: "inv", clientID: 602, protocol: 3}
	RegisterClient(data)
	RegisterClient(inv)
	defer UnregisterClient(data)
	defer UnregisterClient(inv)

	asserts.AssertStatusReply(t, execClientTrackingConn(data, [][]byte{
		[]byte("ON"), []byte("REDIRECT"), []byte("602"),
	}), "OK")
	defer DisableTracking(data.GetTrackingID())

	asserts.AssertIntReply(t, execClientGetRedirConn(data, nil), 602)
	asserts.AssertIntReply(t, execClientGetRedirConn(inv, nil), -1)

	id := data.GetTrackingID()
	TrackKeysOnRead(id, []string{"trk"})
	InvalidateKeysOnWrite([]string{"trk"})
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		inv.mu.Lock()
		n := len(inv.writes)
		inv.mu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("expected invalidation on redirect target")
}

func TestM2ahRestoreIdleTimeFreq(t *testing.T) {
	server, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "rk", "v")), "OK")
	dump := server.Exec(c, utils.ToCmdLine("DUMP", "rk"))
	bulk, ok := dump.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("DUMP: %s", dump.ToBytes())
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"RESTORE", "rk2", "0", string(bulk.Arg), "IDLETIME", "42", "FREQ", "7",
	)), "OK")

	idle := server.Exec(c, utils.ToCmdLine("OBJECT", "IDLETIME", "rk2"))
	asserts.AssertIntReplyGreaterThan(t, idle, 40)
	freq := server.Exec(c, utils.ToCmdLine("OBJECT", "FREQ", "rk2"))
	asserts.AssertIntReply(t, freq, 7)
}

func TestM2ahPingBulkMessage(t *testing.T) {
	r := Ping(nil, [][]byte{[]byte("hello")})
	asserts.AssertBulkReply(t, r, "hello")
}

func TestM2ahFindClientByID(t *testing.T) {
	c := connection.NewFakeConn()
	RegisterClient(c)
	defer UnregisterClient(c)
	id := c.GetClientID()
	got := FindClientByID(id)
	if got == nil || got.GetClientID() != id {
		t.Fatalf("FindClientByID(%d) failed", id)
	}
	if FindClientByID(-999) != nil {
		t.Fatal("expected nil for missing id")
	}
	_ = strconv.FormatInt(id, 10)
}
