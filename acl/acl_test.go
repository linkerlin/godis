package acl

import "testing"

func TestSetUserPlusAllGrantsAllCommands(t *testing.T) {
	engine := NewEngine()
	user, err := engine.SetUser("default", []string{"on", "+@all", "~*"})
	if err != nil {
		t.Fatalf("SetUser: %v", err)
	}
	if !user.CheckCommand("set") {
		t.Fatal("expected +@all to allow SET")
	}
	if !user.CheckCommand("acl") {
		t.Fatal("expected +@all to allow ACL")
	}
}
