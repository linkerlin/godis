package database

import (
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestInitACLEngineOnServerStart(t *testing.T) {
	server, err := NewTestServer()
	if err != nil {
		t.Fatalf("NewTestServer: %v", err)
	}
	if aclEngine == nil {
		t.Fatal("aclEngine should be initialized after NewTestServer")
	}
	users := aclEngine.GetAllUsers()
	if len(users) == 0 {
		t.Fatal("expected at least default ACL user")
	}
	_, ok := aclEngine.GetUser("default")
	if !ok {
		t.Fatal("default ACL user missing")
	}
	_ = server
}

func TestExecAuthRejectsWithoutPassword(t *testing.T) {
	db := getTestDB()
	oldPass := config.Properties.RequirePass
	config.Properties.RequirePass = "secret"
	defer func() { config.Properties.RequirePass = oldPass }()

	reply := db.Exec(nil, utils.ToCmdLine("AUTH", "wrong"))
	asserts.AssertErrReply(t, reply, "ERR invalid password")

	reply = db.Exec(nil, utils.ToCmdLine("AUTH", "secret"))
	asserts.AssertStatusReply(t, reply, "OK")
}

func TestExecAuthWithACLNopassDefault(t *testing.T) {
	db := getTestDB()
	oldPass := config.Properties.RequirePass
	config.Properties.RequirePass = ""
	defer func() { config.Properties.RequirePass = oldPass }()

	// With ACL default user on nopass, AUTH succeeds (no legacy bypass).
	reply := db.Exec(nil, utils.ToCmdLine("AUTH", "anything"))
	asserts.AssertStatusReply(t, reply, "OK")
}

func TestACLSetUserAfterEngineInit(t *testing.T) {
	c := connection.NewFakeConn()
	ret := testServer.Exec(c, utils.ToCmdLine("ACL", "SETUSER", "readonly", "on", "-@all", "+@read"))
	asserts.AssertStatusReply(t, ret, "OK")

	ret = testServer.Exec(c, utils.ToCmdLine("ACL", "GETUSER", "readonly"))
	asserts.AssertNotError(t, ret)
}

func TestACLPermissionDenied(t *testing.T) {
	c := connection.NewFakeConn()
	ret := testServer.Exec(c, utils.ToCmdLine("ACL", "SETUSER", "reader", "on", "-@all", "+get"))
	asserts.AssertStatusReply(t, ret, "OK")

	ret = testServer.Exec(c, utils.ToCmdLine("AUTH", "reader", "nopass"))
	asserts.AssertStatusReply(t, ret, "OK")

	ret = testServer.Exec(c, utils.ToCmdLine("GET", "k"))
	asserts.AssertNotError(t, ret)

	ret = testServer.Exec(c, utils.ToCmdLine("SET", "k", "v"))
	asserts.AssertErrReply(t, ret, "NOPERM User reader has no permissions to run the 'SET' command")
}

func TestHelloAuthUpdatesConnection(t *testing.T) {
	c := connection.NewFakeConn()
	oldPass := config.Properties.RequirePass
	config.Properties.RequirePass = "hello-secret"
	defer func() { config.Properties.RequirePass = oldPass }()

	ret := testServer.Exec(c, utils.ToCmdLine("HELLO", "3", "AUTH", "default", "wrong"))
	asserts.AssertErrReply(t, ret, "WRONGPASS invalid username-password pair")

	ret = testServer.Exec(c, utils.ToCmdLine("HELLO", "3", "AUTH", "default", "hello-secret"))
	asserts.AssertNotError(t, ret)
	if !c.IsACLAuthenticated() {
		t.Fatal("HELLO AUTH should mark connection as ACL-authenticated")
	}
}

func TestAuthLegacyRequirePass(t *testing.T) {
	passwd := utils.RandString(10)
	c := connection.NewFakeConn()
	oldPass := config.Properties.RequirePass
	config.Properties.RequirePass = passwd
	defer func() { config.Properties.RequirePass = oldPass }()

	ret := testServer.Exec(c, utils.ToCmdLine("AUTH"))
	asserts.AssertErrReply(t, ret, "ERR wrong number of arguments for 'auth' command")

	ret = testServer.Exec(c, utils.ToCmdLine("AUTH", passwd+"wrong"))
	asserts.AssertErrReply(t, ret, "ERR invalid password")

	ret = testServer.Exec(c, utils.ToCmdLine("GET", "A"))
	asserts.AssertErrReply(t, ret, "NOAUTH Authentication required")

	ret = testServer.Exec(c, utils.ToCmdLine("AUTH", passwd))
	asserts.AssertStatusReply(t, ret, "OK")

	ret = testServer.Exec(c, utils.ToCmdLine("SET", "A", "1"))
	asserts.AssertStatusReply(t, ret, "OK")
}
