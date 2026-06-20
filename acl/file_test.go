package acl

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestACLFileRoundTrip(t *testing.T) {
	engine := NewEngine()
	_, err := engine.SetUser("default", []string{"on", "nopass", "~*", "+@all"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.SetUser("reader", []string{"on", "-@all", "+get", "~r:*"})
	if err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(t.TempDir(), "users.acl")
	if err := engine.SaveToFile(path); err != nil {
		t.Fatal(err)
	}

	reloaded := NewEngine()
	if err := reloaded.LoadFromFile(path); err != nil {
		t.Fatal(err)
	}

	reader, ok := reloaded.GetUser("reader")
	if !ok {
		t.Fatal("reader user missing after reload")
	}
	if reader.CheckCommand("get") != true || reader.CheckCommand("set") != false {
		t.Fatal("reader permissions mismatch after reload")
	}
}

func TestParseACLFileIgnoresComments(t *testing.T) {
	content := "# comment\nuser default on nopass ~* +@all\n"
	users, err := ParseACLFile(content)
	if err != nil {
		t.Fatal(err)
	}
	if len(users) != 1 || users["default"] == nil {
		t.Fatalf("unexpected users: %#v", users)
	}
}

func TestLoadFromFileMissing(t *testing.T) {
	engine := NewEngine()
	err := engine.LoadFromFile(filepath.Join(t.TempDir(), "missing.acl"))
	if err == nil {
		t.Fatal("expected error for missing acl file")
	}
}

func TestSaveToFileCreatesFile(t *testing.T) {
	engine := NewEngine()
	_, _ = engine.SetUser("default", []string{"on", "nopass", "+@all", "~*"})
	path := filepath.Join(t.TempDir(), "out.acl")
	if err := engine.SaveToFile(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "user default") {
		t.Fatalf("unexpected acl file: %s", data)
	}
}
