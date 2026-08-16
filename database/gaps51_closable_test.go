package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps51MemoryDoctorHelpPFDebugConfigModule(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	doc := srv.Exec(c, utils.ToCmdLine("MEMORY", "DOCTOR"))
	bulk, ok := doc.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), "this instance is empty or is using very little memory") {
		t.Fatalf("MEMORY DOCTOR empty: %T %s", doc, doc.ToBytes())
	}

	help := srv.Exec(c, utils.ToCmdLine("MEMORY", "HELP"))
	mr, ok := help.(*protocol.MultiRawReply)
	if !ok || len(mr.Replies) < 5 {
		t.Fatalf("MEMORY HELP: %T %s", help, help.ToBytes())
	}
	joined := string(help.ToBytes())
	if !strings.Contains(joined, "Return memory problems reports.") {
		t.Fatalf("MEMORY HELP want Redis DOCTOR line: %s", joined)
	}

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("PFDEBUG", "GETREG", "nosuch")),
		"ERR The specified key does not exist")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("PFDEBUG", "DECODE", "nosuch")),
		"ERR The specified key does not exist")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "protected-directories", "/tmp")),
		"ERR Unknown option or number of arguments for CONFIG SET - 'protected-directories'")

	mod := srv.Exec(c, utils.ToCmdLine("MODULE", "HELP"))
	mh, ok := mod.(*protocol.MultiRawReply)
	if !ok || len(mh.Replies) < 5 {
		t.Fatalf("MODULE HELP: %T %s", mod, mod.ToBytes())
	}
	ms := string(mod.ToBytes())
	if !strings.Contains(ms, "MODULE <subcommand>") || !strings.Contains(ms, "Load a module library from") {
		t.Fatalf("MODULE HELP want Redis layout: %s", ms)
	}
}
