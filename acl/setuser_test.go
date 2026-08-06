package acl

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

// TestSetUserPasswordAppend verifies multiple >password rules accumulate
// (Redis appends, previously the list was replaced).
func TestSetUserPasswordAppend(t *testing.T) {
	e := NewEngine()
	_, err := e.SetUser("ap", []string{"on", ">pw1", ">pw2", "~*", "&*", "+@all"})
	if err != nil {
		t.Fatalf("setuser: %v", err)
	}
	u, ok := e.GetUser("ap")
	if !ok {
		t.Fatal("user missing")
	}
	if len(u.Passwords) != 2 {
		t.Fatalf("want 2 accumulated passwords, got %d", len(u.Passwords))
	}
	// Both must authenticate.
	if _, err := e.Authenticate("ap", "pw1"); err != nil {
		t.Fatalf("pw1 should authenticate: %v", err)
	}
	if _, err := e.Authenticate("ap", "pw2"); err != nil {
		t.Fatalf("pw2 should authenticate: %v", err)
	}
	// <pw1 removes only the first (Redis 7+).
	if _, err := e.SetUser("ap", []string{"<pw1"}); err != nil {
		t.Fatalf("remove pw1: %v", err)
	}
	u, _ = e.GetUser("ap")
	if len(u.Passwords) != 1 {
		t.Fatalf("want 1 password after removal, got %d", len(u.Passwords))
	}
	if _, err := e.Authenticate("ap", "pw1"); err == nil {
		t.Fatal("pw1 should no longer authenticate")
	}
	if _, err := e.Authenticate("ap", "pw2"); err != nil {
		t.Fatalf("pw2 should still authenticate: %v", err)
	}
}

// TestSetUserHashRules verifies #hash append and !hash removal.
func TestSetUserHashRules(t *testing.T) {
	e := NewEngine()
	h1 := sha256.Sum256([]byte("hpass1"))
	h2 := sha256.Sum256([]byte("hpass2"))
	_, err := e.SetUser("hu", []string{"on", "#" + hex.EncodeToString(h1[:]), "#" + hex.EncodeToString(h2[:]), "~*", "&*", "+@all"})
	if err != nil {
		t.Fatalf("setuser: %v", err)
	}
	u, _ := e.GetUser("hu")
	if len(u.Passwords) != 2 {
		t.Fatalf("want 2 hashed passwords, got %d", len(u.Passwords))
	}
	// !hash removes one.
	if _, err := e.SetUser("hu", []string{"!" + hex.EncodeToString(h1[:])}); err != nil {
		t.Fatalf("remove hash: %v", err)
	}
	u, _ = e.GetUser("hu")
	if len(u.Passwords) != 1 {
		t.Fatalf("want 1 hashed password after !, got %d", len(u.Passwords))
	}
	if _, err := e.Authenticate("hu", "hpass2"); err != nil {
		t.Fatalf("hpass2 should authenticate: %v", err)
	}
}

// TestSetUserReset verifies the reset rule clears all permissions and disables
// the user.
func TestSetUserReset(t *testing.T) {
	e := NewEngine()
	_, err := e.SetUser("ru", []string{"on", ">pw", "~*", "&*", "+@all", "+get"})
	if err != nil {
		t.Fatalf("setuser: %v", err)
	}
	if _, err := e.SetUser("ru", []string{"reset"}); err != nil {
		t.Fatalf("reset: %v", err)
	}
	u, ok := e.GetUser("ru")
	if !ok {
		t.Fatal("user should still exist after reset")
	}
	if u.Enabled {
		t.Fatal("user should be disabled after reset")
	}
	if len(u.Passwords) != 0 || len(u.KeyPatterns) != 0 || len(u.Channels) != 0 {
		t.Fatalf("reset should clear passwords/keys/channels: %+v", u)
	}
	if u.Commands.AllCommands {
		t.Fatal("reset should clear allcommands")
	}
	if _, err := e.Authenticate("ru", "pw"); err == nil {
		t.Fatal("reset password should not authenticate")
	}
}

// TestSetUserResetpass verifies resetpass removes all passwords but keeps the
// rest of the rules.
func TestSetUserResetpass(t *testing.T) {
	e := NewEngine()
	if _, err := e.SetUser("rp", []string{"on", ">pw1", ">pw2", "~*", "&*", "+get"}); err != nil {
		t.Fatalf("setuser: %v", err)
	}
	if _, err := e.SetUser("rp", []string{"resetpass"}); err != nil {
		t.Fatalf("resetpass: %v", err)
	}
	u, _ := e.GetUser("rp")
	if len(u.Passwords) != 0 {
		t.Fatalf("resetpass should clear passwords, got %d", len(u.Passwords))
	}
	if len(u.KeyPatterns) == 0 || !u.Commands.AllowedCommands["get"] {
		t.Fatalf("resetpass should keep keys/commands: %+v", u)
	}
	// No passwords = nopass: any credentials authenticate (Redis semantics).
	if _, err := e.Authenticate("rp", "pw1"); err != nil {
		t.Fatalf("nopass user should authenticate with any password: %v", err)
	}
}

// TestBarePasswordRuleErrors verifies bare >, #, <, ! without a value are
// rejected rather than silently ignored.
func TestBarePasswordRuleErrors(t *testing.T) {
	e := NewEngine()
	for _, rule := range []string{">", "#", "<", "!"} {
		if _, err := e.SetUser("bare", []string{rule}); err == nil {
			t.Fatalf("bare rule %q should error", rule)
		}
	}
	_ = strings.ToLower // keep import
}
