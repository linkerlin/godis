package database

import (
	"path/filepath"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestACLFileSaveLoadOnRestart(t *testing.T) {
	dir := t.TempDir()
	aclPath := filepath.Join(dir, "users.acl")

	oldProps := config.Properties
	config.Properties = &config.ServerProperties{
		Dir:     dir,
		AclFile: aclPath,
	}
	defer func() { config.Properties = oldProps }()

	server, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}

	c := connection.NewFakeConn()
	ret := server.Exec(c, utils.ToCmdLine("ACL", "SETUSER", "app", "on", "nopass", "-@all", "+get", "~app:*"))
	asserts.AssertStatusReply(t, ret, "OK")

	ret = server.Exec(c, utils.ToCmdLine("ACL", "SAVE"))
	asserts.AssertStatusReply(t, ret, "OK")

	_, err = NewTestServer()
	if err != nil {
		t.Fatalf("restart server: %v", err)
	}

	user, ok := aclEngine.GetUser("app")
	if !ok {
		t.Fatal("app user should be loaded from aclfile")
	}
	if !user.CheckCommand("get") || user.CheckCommand("set") {
		t.Fatal("app user permissions not restored from aclfile")
	}
}

func TestACLLoadCommand(t *testing.T) {
	dir := t.TempDir()
	aclPath := filepath.Join(dir, "users.acl")

	oldProps := config.Properties
	config.Properties = &config.ServerProperties{
		Dir:     dir,
		AclFile: aclPath,
	}
	defer func() { config.Properties = oldProps }()

	engine := aclEngine
	if engine == nil {
		t.Fatal("acl engine not initialized")
	}
	if err := engine.SaveToFile(aclPath); err != nil {
		t.Fatal(err)
	}

	_, _ = engine.SetUser("temp", []string{"on", "nopass", "+ping"})
	c := connection.NewFakeConn()
	ret := testServer.Exec(c, utils.ToCmdLine("ACL", "LOAD"))
	asserts.AssertStatusReply(t, ret, "OK")

	if _, ok := aclEngine.GetUser("temp"); ok {
		t.Fatal("ACL LOAD should replace in-memory users from file")
	}
}
