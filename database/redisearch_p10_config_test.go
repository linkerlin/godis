package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestP10SearchKebabConfigGet verifies CONFIG GET search-* returns the Redis 8.0
// kebab-case search namespace, sourced from the same values FT.CONFIG uses.
func TestP10SearchKebabConfigGet(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	// Set a value via FT.CONFIG, read it back via CONFIG GET search-*.
	setReply := server.Exec(c, utils.ToCmdLine("FT.CONFIG", "SET", "MINPREFIX", "4"))
	t.Logf("FT.CONFIG SET reply = %s", setReply.ToBytes())
	if protocol.IsErrorReply(setReply) {
		t.Fatalf("FT.CONFIG SET errored: %s", setReply.ToBytes())
	}
	r := server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "search-min-prefix"))
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("CONFIG GET shape: %T %s", r, r.ToBytes())
	}
	t.Logf("CONFIG GET search-min-prefix Args = %q", mb.Args)
	found := false
	for i := 0; i+1 < len(mb.Args); i += 2 {
		if string(mb.Args[i]) == "search-min-prefix" && string(mb.Args[i+1]) == "4" {
			found = true
		}
	}
	if !found {
		t.Fatalf("CONFIG GET search-min-prefix should mirror FT.CONFIG MINPREFIX=4: %s", r.ToBytes())
	}
	// Reset.
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("FT.CONFIG", "SET", "MINPREFIX", "2")), "OK")
}

// TestP10SearchKebabConfigSet verifies CONFIG SET search-* updates the value and
// is visible to FT.CONFIG GET (bidirectional interop).
func TestP10SearchKebabConfigSet(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	defer func() {
		_ = server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "search-default-dialect", "1"))
	}()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "search-default-dialect", "2")), "OK")
	r := server.Exec(c, utils.ToCmdLine("FT.CONFIG", "GET", "DEFAULT_DIALECT"))
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("FT.CONFIG GET shape: %T %s", r, r.ToBytes())
	}
	found := false
	for i := 0; i+1 < len(mb.Args); i += 2 {
		if string(mb.Args[i]) == "DEFAULT_DIALECT" && string(mb.Args[i+1]) == "2" {
			found = true
		}
	}
	if !found {
		t.Fatalf("CONFIG SET search-default-dialect 2 should be visible to FT.CONFIG GET: %s", r.ToBytes())
	}
}

// TestP10SearchKebabConfigWildcard verifies CONFIG GET search-* lists the whole
// namespace (the 8.0 ops pattern for inspecting all search settings).
func TestP10SearchKebabConfigWildcard(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "search-*"))
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("CONFIG GET shape: %T %s", r, r.ToBytes())
	}
	if len(mb.Args) == 0 {
		t.Fatalf("CONFIG GET search-* should return entries: %s", r.ToBytes())
	}
	body := string(r.ToBytes())
	// Every documented 8.0 search-* key should appear.
	for _, key := range []string{
		"search-timeout", "search-on-timeout", "search-max-search-results",
		"search-min-prefix", "search-max-expansions", "search-default-dialect",
	} {
		if !strings.Contains(body, key) {
			t.Fatalf("CONFIG GET search-* missing %q: %s", key, body)
		}
	}
}

// TestP10SearchKebabConfigSetInvalid verifies invalid values are rejected.
func TestP10SearchKebabConfigSetInvalid(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	// Non-numeric timeout.
	if r := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "search-timeout", "abc")); !protocol.IsErrorReply(r) {
		t.Fatalf("search-timeout=abc should be rejected: %s", r.ToBytes())
	}
	// Bad on-timeout policy.
	if r := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "search-on-timeout", "MAYBE")); !protocol.IsErrorReply(r) {
		t.Fatalf("search-on-timeout=MAYBE should be rejected: %s", r.ToBytes())
	}
}
