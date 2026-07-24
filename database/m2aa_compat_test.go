package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2aaTSFilterExists(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"TS.CREATE", "ts:a", "LABELS", "region", "east", "env", "prod",
	)), "OK")
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine(
		"TS.CREATE", "ts:b", "LABELS", "region", "west",
	)), "OK")
	db.Exec(nil, utils.ToCmdLine("TS.ADD", "ts:a", "1", "1"))
	db.Exec(nil, utils.ToCmdLine("TS.ADD", "ts:b", "1", "1"))

	// label= → exists
	r := db.Exec(nil, utils.ToCmdLine("TS.QUERYINDEX", "env="))
	mr, ok := r.(*protocol.MultiBulkReply)
	if !ok || len(mr.Args) != 1 || string(mr.Args[0]) != "ts:a" {
		t.Fatalf("env= existence: %s", r.ToBytes())
	}

	// label!= → does not exist
	r2 := db.Exec(nil, utils.ToCmdLine("TS.QUERYINDEX", "env!="))
	mr2, ok := r2.(*protocol.MultiBulkReply)
	if !ok || len(mr2.Args) != 1 || string(mr2.Args[0]) != "ts:b" {
		t.Fatalf("env!= absence: %s", r2.ToBytes())
	}
}

func TestM2aaFunctionEngineFromShebang(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)
	db.Exec(nil, utils.ToCmdLine("FUNCTION", "FLUSH"))
	code := "#!lua name=m2aalib api_version=1.0\n" +
		"redis.register_function('m2aa_one', function(keys, args) return 1 end)"
	r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", code))
	if protocol.IsErrorReply(r) {
		t.Fatalf("LOAD: %s", r.ToBytes())
	}
	list := db.Exec(nil, utils.ToCmdLine("FUNCTION", "LIST"))
	s := string(list.ToBytes())
	if !strings.Contains(s, "engine") || !strings.Contains(strings.ToUpper(s), "LUA") {
		t.Fatalf("expected engine LUA in LIST: %s", s)
	}
	lib, ok := funcEngine.GetLibrary("m2aalib")
	if !ok || lib.Engine != "LUA" {
		t.Fatalf("Library.Engine: ok=%v engine=%q", ok, lib.Engine)
	}
}
