package database

import (
	"testing"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2tConfigBoolAndSet(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "appendonly", "true")), "OK")
	if !config.Properties.AppendOnly {
		t.Fatal("appendonly true not applied")
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "appendonly", "0")), "OK")
	if config.Properties.AppendOnly {
		t.Fatal("appendonly 0 not applied")
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxclients", "256")), "OK")
	if config.Properties.MaxClients != 256 {
		t.Fatalf("maxclients=%d", config.Properties.MaxClients)
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory", "1048576")), "OK")
	got := server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "maxmemory"))
	multi, ok := got.(*protocol.MultiBulkReply)
	if !ok || len(multi.Args) < 2 || string(multi.Args[1]) != "1048576" {
		t.Fatalf("CONFIG GET maxmemory: %s", got.ToBytes())
	}
}

func TestM2tXAutoClaimMinID(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("XADD", "s", "1-0", "f", "a"))
	db.Exec(nil, utils.ToCmdLine("XADD", "s", "2-0", "f", "b"))
	db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "s", "g", "0-0"))
	db.Exec(nil, utils.ToCmdLine("XREADGROUP", "GROUP", "g", "c1", "STREAMS", "s", ">"))
	r := db.Exec(nil, utils.ToCmdLine(
		"XAUTOCLAIM", "s", "g", "c2", "0", "0-0", "MINID", "2-0",
	))
	if protocol.IsErrorReply(r) {
		t.Fatalf("XAUTOCLAIM MINID: %s", r.ToBytes())
	}
	mr, ok := r.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) < 3 {
		t.Fatalf("unexpected reply: %T %s", r, r.ToBytes())
	}
	// deleted should include 1-0 purged by MINID
	del, ok := mr.Replies[2].(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("deleted: %T", mr.Replies[2])
	}
	found := false
	for _, a := range del.Args {
		if string(a) == "1-0" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected 1-0 in deleted, got %s", mr.Replies[2].ToBytes())
	}
}

func TestM2tPFDebugPeriod(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("PFDEBUG", "PERIOD", "100")), "OK")
	if hllDebugPeriod != 100 {
		t.Fatalf("period=%d", hllDebugPeriod)
	}
}

func TestM2tTrackingOptInCaching(t *testing.T) {
	c := &trackingTestConn{name: "optin", clientID: time.Now().UnixNano()}
	c.SetProtocolVersion(3)
	id := EnableTracking(c, "optin", nil, "", false)
	c.SetTrackingID(id)
	defer DisableTracking(id)

	TrackKeysOnRead(id, []string{"k1"})
	info := GetTrackingInfo(id)
	if n, _ := info["keys"].(int); n != 0 {
		t.Fatalf("optin without CACHING should not track, keys=%d", n)
	}

	asserts.AssertStatusReply(t, execClientCachingConn(c, [][]byte{[]byte("YES")}), "OK")
	TrackKeysOnRead(id, []string{"k2"})
	info = GetTrackingInfo(id)
	if n, _ := info["keys"].(int); n != 1 {
		t.Fatalf("optin+CACHING YES should track, keys=%d", n)
	}
}

func TestM2tTrackingNoLoop(t *testing.T) {
	c := &trackingTestConn{name: "noloop", clientID: 99}
	c.SetProtocolVersion(3)
	id := EnableTracking(c, "", nil, "", true)
	c.SetTrackingID(id)
	defer DisableTracking(id)

	TrackKeysOnRead(id, []string{"nl"})
	InvalidateKeysOnWriteFrom([]string{"nl"}, id)
	// writer with NOLOOP should not receive invalidate
	c.mu.Lock()
	n := len(c.writes)
	c.mu.Unlock()
	if n != 0 {
		t.Fatalf("NOLOOP should skip self invalidate, writes=%d", n)
	}
}

func TestM2tParseConfigBool(t *testing.T) {
	cases := []struct {
		in   string
		ok   bool
		want bool
	}{
		{"yes", true, true},
		{"TRUE", true, true},
		{"1", true, true},
		{"on", true, true},
		{"no", true, false},
		{"false", true, false},
		{"0", true, false},
		{"maybe", false, false},
	}
	for _, tc := range cases {
		ok, v := config.ParseConfigBool(tc.in)
		if ok != tc.ok || v != tc.want {
			t.Fatalf("%s: got (%v,%v)", tc.in, ok, v)
		}
	}
}
