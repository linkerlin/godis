package acl

import "testing"

func TestM2asSelectorPermission(t *testing.T) {
	eng := NewEngine()
	u, err := eng.SetUser("sel", []string{"on", "nopass", "+get", "~key1", "(+set ~key2)"})
	if err != nil {
		t.Fatal(err)
	}
	if !u.CheckPermission("get", nil, []string{"key1"}) {
		t.Fatal("GET key1 should allow")
	}
	if u.CheckPermission("get", nil, []string{"key2"}) {
		t.Fatal("GET key2 should deny")
	}
	if !u.CheckPermission("set", []string{"key2"}, nil) {
		t.Fatal("SET key2 should allow via selector")
	}
	if u.CheckPermission("set", []string{"key1"}, nil) {
		t.Fatal("SET key1 should deny")
	}
	if len(u.Selectors) != 1 {
		t.Fatalf("expected 1 selector, got %d", len(u.Selectors))
	}

	_, err = eng.SetUser("sel", []string{"clearselectors"})
	if err != nil {
		t.Fatal(err)
	}
	u, _ = eng.GetUser("sel")
	if len(u.Selectors) != 0 {
		t.Fatal("clearselectors should drop selectors")
	}
	if !u.CheckPermission("get", nil, []string{"key1"}) {
		t.Fatal("root rules should remain after clearselectors")
	}
}

func TestM2asChannelPattern(t *testing.T) {
	eng := NewEngine()
	u, err := eng.SetUser("ch", []string{"on", "nopass", "+@all", "&news:*"})
	if err != nil {
		t.Fatal(err)
	}
	if !u.CheckChannel("news:1") {
		t.Fatal("news:1 should allow")
	}
	if u.CheckChannel("other") {
		t.Fatal("other should deny")
	}
}
