package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestSetBitOffsetLimit verifies SETBIT rejects offsets beyond the 512MB string
// limit (2^32-1) instead of attempting a multi-GB allocation.
func TestSetBitOffsetLimit(t *testing.T) {
	db := makeTestDB()
	r := db.Exec(nil, utils.ToCmdLine("SETBIT", "k", "4294967296", "1"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("SETBIT beyond 2^32-1 should error, got %s", r.ToBytes())
	}
	// Max legal offset works.
	r = db.Exec(nil, utils.ToCmdLine("SETBIT", "k2", "4294967295", "1"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("SETBIT at 2^32-1 should work, got %s", r.ToBytes())
	}
}

// TestKeyspaceCommandEvents verifies command-level keyspace notifications fire
// for HSET/LPUSH/SADD/ZADD/XADD (previously only set/del/expire were emitted).
func TestKeyspaceCommandEvents(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	// Enable keyspace notifications, subscribe a client to keyevent channel.
	oldNotify := config.Properties.NotifyKeyspaceEvents
	config.Properties.NotifyKeyspaceEvents = "KA"
	defer func() { config.Properties.NotifyKeyspaceEvents = oldNotify }()

	sub := connection.NewFakeConn()
	if r := server.Exec(sub, utils.ToCmdLine("PSUBSCRIBE", "__keyevent@0__:*")); protocol.IsErrorReply(r) {
		t.Fatalf("psubscribe: %s", r.ToBytes())
	}
	sub.Bytes() // consume the psubscribe confirmation

	server.Exec(c, utils.ToCmdLine("HSET", "nk:h", "f", "v"))
	server.Exec(c, utils.ToCmdLine("LPUSH", "nk:l", "x"))
	server.Exec(c, utils.ToCmdLine("SADD", "nk:s", "m"))
	server.Exec(c, utils.ToCmdLine("ZADD", "nk:z", "1", "a"))
	server.Exec(c, utils.ToCmdLine("XADD", "nk:st", "*", "f", "v"))

	body := string(sub.Bytes())
	for _, want := range []string{"hset", "lpush", "sadd", "zadd", "xadd"} {
		if !strings.Contains(body, want) {
			t.Fatalf("keyspace events missing %q; got: %s", want, body)
		}
	}
}

// TestResetDeauthenticates verifies RESET clears the authenticated user.
func TestResetDeauthenticates(t *testing.T) {
	server := getTestServer()
	admin := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(admin, utils.ToCmdLine("ACL", "SETUSER", "resetuser", "on", ">pw", "~*", "&*", "+@all")), "OK")
	defer func() {
		_ = server.Exec(admin, utils.ToCmdLine("ACL", "DELUSER", "resetuser"))
	}()
	u := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(u, utils.ToCmdLine("AUTH", "resetuser", "pw")), "OK")
	if u.GetACLUser() != "resetuser" {
		t.Fatalf("expected authenticated as resetuser, got %q", u.GetACLUser())
	}
	// RESET deauthenticates: the user reverts to default.
	r := server.Exec(u, utils.ToCmdLine("RESET"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("RESET reply: %s", r.ToBytes())
	}
	if u.GetACLUser() != "" {
		t.Fatalf("RESET should clear the ACL user, got %q", u.GetACLUser())
	}
}
