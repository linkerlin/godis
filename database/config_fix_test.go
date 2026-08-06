package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
)

// TestNotifyKeyspaceEventsValidation verifies CONFIG SET notify-keyspace-events
// rejects unknown event-class characters.
func TestNotifyKeyspaceEventsValidation(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	if r := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "notify-keyspace-events", "Kz!")); !protocol.IsErrorReply(r) {
		t.Fatalf("invalid event char should be rejected: %s", r.ToBytes())
	}
	if r := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "notify-keyspace-events", "KEA")); protocol.IsErrorReply(r) {
		t.Fatalf("valid event flags should be accepted: %s", r.ToBytes())
	}
}

// TestRequirepassSyncsACLDefault verifies CONFIG SET requirepass keeps the
// default ACL user in sync, so AUTH works against both.
func TestRequirepassSyncsACLDefault(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	old := config.Properties.RequirePass
	defer func() {
		config.Properties.RequirePass = old
		if r := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "requirepass", old)); protocol.IsErrorReply(r) {
			t.Logf("restore requirepass: %s", r.ToBytes())
		}
	}()

	if r := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "requirepass", "secret123")); protocol.IsErrorReply(r) {
		t.Fatalf("set requirepass: %s", r.ToBytes())
	}
	// The default ACL user must now accept secret123.
	u := connection.NewFakeConn()
	if r := server.Exec(u, utils.ToCmdLine("AUTH", "default", "secret123")); protocol.IsErrorReply(r) {
		t.Fatalf("AUTH default secret123 should work after requirepass sync: %s", r.ToBytes())
	}
	// Wrong password rejected.
	bad := connection.NewFakeConn()
	if r := server.Exec(bad, utils.ToCmdLine("AUTH", "default", "wrong")); !protocol.IsErrorReply(r) {
		t.Fatalf("AUTH with wrong password should fail: %s", r.ToBytes())
	}
}

// TestNotifyEmptyStringAccepted verifies an empty value clears notifications.
func TestNotifyEmptyStringAccepted(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	// Ensure no requirepass is active (a prior test may have left one), so the
	// CONFIG command below isn't NOAUTH-rejected.
	config.Properties.RequirePass = ""
	if aclEngine != nil {
		if u, ok := aclEngine.GetUser("default"); ok {
			u.ClearPasswords()
		}
	}
	if r := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "notify-keyspace-events", "")); protocol.IsErrorReply(r) {
		t.Fatalf("empty notify value should be accepted: %s", r.ToBytes())
	}
	if !strings.EqualFold(config.Properties.NotifyKeyspaceEvents, "") {
		t.Fatalf("notify should be cleared, got %q", config.Properties.NotifyKeyspaceEvents)
	}
}
