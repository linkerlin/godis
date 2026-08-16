package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps53ConfigHelpRestore(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "repl-diskless-sync-max-replicas")),
		[]string{"repl-diskless-sync-max-replicas", "0"})
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-diskless-sync-max-replicas", "3")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "repl-diskless-sync-max-replicas")),
		[]string{"repl-diskless-sync-max-replicas", "3"})
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-diskless-sync-max-replicas", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-diskless-sync-max-replicas') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "repl-diskless-sync-max-replicas", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'repl-diskless-sync-max-replicas') - argument must be between 0 and 2147483647 inclusive")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("RESTORE", "k", "0", "bad")),
		"ERR DUMP payload version or checksum are wrong")

	obj := string(srv.Exec(c, utils.ToCmdLine("OBJECT", "HELP")).ToBytes())
	if !strings.Contains(obj, "kind of internal representation") {
		t.Fatalf("OBJECT HELP: %s", obj)
	}
	cfg := string(srv.Exec(c, utils.ToCmdLine("CONFIG", "HELP")).ToBytes())
	if !strings.Contains(cfg, "glob-like") || !strings.Contains(cfg, "SET <directive>") {
		t.Fatalf("CONFIG HELP: %s", cfg)
	}
	sl := string(srv.Exec(c, utils.ToCmdLine("SLOWLOG", "HELP")).ToBytes())
	if !strings.Contains(sl, "-1 mean all") || !strings.Contains(sl, "client name") {
		t.Fatalf("SLOWLOG HELP: %s", sl)
	}
}
