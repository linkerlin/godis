package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestACLCategoryEnforcement verifies the ACL category table drives permission
// enforcement: a user granted +@string can run GETDEL (previously absent from
// the table so the grant was ineffective), and +@search gates FT commands.
func TestACLCategoryEnforcement(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("ACL", "SETUSER", "catuser", "on", ">pw", "~*", "&*", "+@string", "+@search", "-set")), "OK")
	defer func() {
		_ = server.Exec(c, utils.ToCmdLine("ACL", "DELUSER", "catuser"))
	}()

	u := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(u, utils.ToCmdLine("AUTH", "catuser", "pw")), "OK")

	// Seed data via the admin connection (the test user explicitly lacks -set).
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SET", "ak", "v")), "OK")

	// GETDEL is @string: allowed.
	r := server.Exec(u, utils.ToCmdLine("GETDEL", "ak"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("GETDEL should be allowed via +@string: %s", r.ToBytes())
	}
	// SET is explicitly denied (-set).
	if r := server.Exec(u, utils.ToCmdLine("SET", "ak2", "v")); !protocol.IsErrorReply(r) {
		t.Fatalf("SET should be denied: %s", r.ToBytes())
	}
	// FT.SEARCH via +@search.
	if r := server.Exec(u, utils.ToCmdLine("FT._LIST")); protocol.IsErrorReply(r) {
		t.Fatalf("FT command should be allowed via +@search: %s", r.ToBytes())
	}
	// A command outside granted categories (e.g. LPUSH) is denied.
	if r := server.Exec(u, utils.ToCmdLine("LPUSH", "l", "x")); !protocol.IsErrorReply(r) {
		t.Fatalf("LPUSH should be denied (not in +@string/+@search): %s", r.ToBytes())
	}
}

// TestACLCatListsNewCommands verifies ACL CAT output includes newly classified
// commands (the category table now covers previously missing ones).
func TestACLCatListsNewCommands(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("ACL", "CAT", "string"))
	body := string(r.ToBytes())
	for _, cmd := range []string{"getdel", "getex", "lcs"} {
		if !strings.Contains(strings.ToLower(body), cmd) {
			t.Fatalf("ACL CAT string should list %s: %s", cmd, body)
		}
	}
}
