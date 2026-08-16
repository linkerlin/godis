package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps54InfoAuthMigrateRestoreHelp(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	info := srv.Exec(c, utils.ToCmdLine("INFO", "default"))
	bulk, ok := info.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) == 0 || !strings.Contains(string(bulk.Arg), "# Server") {
		t.Fatalf("INFO default: %T %s", info, info.ToBytes())
	}

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("AUTH")),
		"ERR wrong number of arguments for 'auth' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("AUTH", "a", "b", "c")),
		"ERR syntax error")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("FUNCTION", "RESTORE", "x")),
		"ERR DUMP payload version or checksum are wrong")

	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("MIGRATE", "127.0.0.1", "6399", "", "0", "100")),
		"NOKEY")

	xh := string(srv.Exec(c, utils.ToCmdLine("XINFO", "HELP")).ToBytes())
	if !strings.Contains(xh, "Show consumers of") || !strings.Contains(xh, "Print this help.") {
		t.Fatalf("XINFO HELP: %s", xh)
	}
	sh := string(srv.Exec(c, utils.ToCmdLine("SCRIPT", "HELP")).ToBytes())
	if !strings.Contains(sh, "DEBUG (YES|SYNC|NO)") || !strings.Contains(sh, "lazyfree-lazy-user-flush") {
		t.Fatalf("SCRIPT HELP: %s", sh)
	}
}
