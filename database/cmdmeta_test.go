package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
)

// TestKeyspacePopEvents verifies pop/removal command events fire
// (LPOP/RPOP/SREM/ZREM/HINCRBY) alongside the add events.
func TestKeyspacePopEvents(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	oldNotify := config.Properties.NotifyKeyspaceEvents
	config.Properties.NotifyKeyspaceEvents = "KA"
	defer func() { config.Properties.NotifyKeyspaceEvents = oldNotify }()

	sub := connection.NewFakeConn()
	if r := server.Exec(sub, utils.ToCmdLine("PSUBSCRIBE", "__keyevent@0__:*")); protocol.IsErrorReply(r) {
		t.Fatalf("psubscribe: %s", r.ToBytes())
	}
	sub.Bytes() // consume confirmation

	server.Exec(c, utils.ToCmdLine("RPUSH", "ev:l", "a", "b"))
	server.Exec(c, utils.ToCmdLine("SADD", "ev:s", "m"))
	server.Exec(c, utils.ToCmdLine("ZADD", "ev:z", "1", "a"))
	server.Exec(c, utils.ToCmdLine("HSET", "ev:h", "f", "1"))
	sub.Bytes() // drain adds

	server.Exec(c, utils.ToCmdLine("LPOP", "ev:l"))
	server.Exec(c, utils.ToCmdLine("RPOP", "ev:l"))
	server.Exec(c, utils.ToCmdLine("SREM", "ev:s", "m"))
	server.Exec(c, utils.ToCmdLine("ZREM", "ev:z", "a"))
	server.Exec(c, utils.ToCmdLine("HINCRBY", "ev:h", "f", "1"))

	body := string(sub.Bytes())
	for _, want := range []string{"lpop", "rpop", "srem", "zrem", "hincrby"} {
		if !strings.Contains(body, want) {
			t.Fatalf("keyspace events missing %q; got: %s", want, body)
		}
	}
}

// TestACLLogEnhancedFields verifies ACL LOG entries carry the Redis 8 fields
// (entry-id, client-info, timestamp-created, timestamp-last-updated).
func TestACLLogEnhancedFields(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	// Force a denied command to generate a log entry.
	if r := server.Exec(c, utils.ToCmdLine("ACL", "SETUSER", "loguser", "on", ">pw", "~*", "&*", "+@read")); protocol.IsErrorReply(r) {
		t.Fatalf("setuser: %s", r.ToBytes())
	}
	defer func() { _ = server.Exec(c, utils.ToCmdLine("ACL", "DELUSER", "loguser")) }()
	u := connection.NewFakeConn()
	if r := server.Exec(u, utils.ToCmdLine("AUTH", "loguser", "pw")); protocol.IsErrorReply(r) {
		t.Fatalf("auth: %s", r.ToBytes())
	}
	_ = server.Exec(u, utils.ToCmdLine("SET", "x", "1")) // denied: +@read only

	r := server.Exec(c, utils.ToCmdLine("ACL", "LOG"))
	body := string(r.ToBytes())
	for _, key := range []string{"entry-id", "client-info", "timestamp-created", "timestamp-last-updated"} {
		if !strings.Contains(body, key) {
			t.Fatalf("ACL LOG missing %q: %s", key, body)
		}
	}
}

// TestCommandDocsAclCategories verifies COMMAND DOCS/INFO expose acl_categories
// and key_specs (Redis 7+ introspection fields).
func TestCommandDocsAclCategories(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	r := server.Exec(c, utils.ToCmdLine("COMMAND", "DOCS", "get"))
	body := string(r.ToBytes())
	if !strings.Contains(body, "acl_categories") {
		t.Fatalf("COMMAND DOCS get missing acl_categories: %s", body)
	}
	if !strings.Contains(body, "key_specs") {
		t.Fatalf("COMMAND DOCS get missing key_specs: %s", body)
	}

	r = server.Exec(c, utils.ToCmdLine("COMMAND", "INFO", "get"))
	body = string(r.ToBytes())
	// INFO replies are nested: the per-command array now has 10 fields, the
	// 7th being acl_categories ("@read" for GET).
	if !strings.Contains(body, "@read") {
		t.Fatalf("COMMAND INFO get should list @read in acl_categories: %s", body)
	}
}
