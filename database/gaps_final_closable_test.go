package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGapsFinalHelpAndACLCat(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("ACL", "CAT", "@admin")),
		"ERR Unknown category '@admin'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("ACL", "CAT", "@read")),
		"ERR Unknown category '@read'")

	cats := srv.Exec(c, utils.ToCmdLine("ACL", "CAT"))
	mb, ok := cats.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("ACL CAT: %T", cats)
	}
	joined := string(cats.ToBytes())
	if strings.Contains(joined, "@read") || strings.Contains(joined, "@admin") {
		t.Fatalf("ACL CAT list must not use @ prefix: %s", joined)
	}
	found := false
	for _, a := range mb.Args {
		if string(a) == "admin" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("ACL CAT want admin: %s", joined)
	}

	admin := srv.Exec(c, utils.ToCmdLine("ACL", "CAT", "admin"))
	if _, ok := admin.(*protocol.MultiBulkReply); !ok {
		t.Fatalf("ACL CAT admin: %T %s", admin, admin.ToBytes())
	}

	for _, cmd := range []string{"ACL", "CLIENT", "COMMAND", "PUBSUB", "FUNCTION", "XGROUP"} {
		h := string(srv.Exec(c, utils.ToCmdLine(cmd, "HELP")).ToBytes())
		if !strings.Contains(h, "Subcommands are:") && !strings.Contains(h, "<subcommand>") {
			t.Fatalf("%s HELP layout: %s", cmd, h)
		}
	}
	acl := string(srv.Exec(c, utils.ToCmdLine("ACL", "HELP")).ToBytes())
	if !strings.Contains(acl, "List all commands that belong to <category>") {
		t.Fatalf("ACL HELP: %s", acl)
	}
	cli := string(srv.Exec(c, utils.ToCmdLine("CLIENT", "HELP")).ToBytes())
	if !strings.Contains(cli, "NO-EVICT (ON|OFF)") || !strings.Contains(cli, "Protect current client") {
		t.Fatalf("CLIENT HELP: %s", cli)
	}
}
