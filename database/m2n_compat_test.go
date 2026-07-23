package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2nJSONGetIndentAndMultiPathOrder(t *testing.T) {
	db := makeTestDB()
	asserts.AssertStatusReply(t, db.Exec(nil, utils.ToCmdLine("JSON.SET", "j", "$", `{"a":1,"b":2}`)), "OK")

	got := db.Exec(nil, utils.ToCmdLine("JSON.GET", "j", "INDENT", "  ", "$"))
	bulk, ok := got.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("JSON.GET INDENT: %T", got)
	}
	s := string(bulk.Arg)
	if !strings.Contains(s, "\n") || !strings.Contains(s, "  ") {
		t.Fatalf("expected pretty JSON, got %q", s)
	}

	multi := db.Exec(nil, utils.ToCmdLine("JSON.GET", "j", "$.b", "$.a"))
	mb, ok := multi.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("JSON.GET multi: %T", multi)
	}
	ms := string(mb.Arg)
	bi := strings.Index(ms, `"$.b"`)
	ai := strings.Index(ms, `"$.a"`)
	if bi < 0 || ai < 0 || bi > ai {
		t.Fatalf("multi-path order broken: %s", ms)
	}
}

func TestM2nFTSugDelLen(t *testing.T) {
	db := makeTestDB()
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("FT.SUGADD", "ac", "hello", "1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("FT.SUGADD", "ac", "help", "1")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("FT.SUGLEN", "ac")), 2)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("FT.SUGDEL", "ac", "hello")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("FT.SUGLEN", "ac")), 1)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("FT.SUGDEL", "ac", "hello")), 0)
	asserts.AssertIntReply(t, db.Exec(nil, utils.ToCmdLine("FT.SUGLEN", "missing")), 0)
}

func TestM2nFunctionAPIVersion(t *testing.T) {
	db := makeTestDB()
	InitFunctionsEngine(db)

	okCode := "#!lua name=mylib api_version=1.0\nredis.register_function('f', function(keys, args) return 1 end)"
	r := db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", okCode))
	if protocol.IsErrorReply(r) {
		t.Fatalf("valid api_version: %s", r.ToBytes())
	}

	bad := "#!lua name=badlib api_version=2.0\nredis.register_function('g', function(keys, args) return 1 end)"
	br := db.Exec(nil, utils.ToCmdLine("FUNCTION", "LOAD", bad))
	if !protocol.IsErrorReply(br) {
		t.Fatalf("api_version=2.0 should fail, got %s", br.ToBytes())
	}
	if !strings.Contains(string(br.ToBytes()), "Invalid API version") {
		t.Fatalf("unexpected error: %s", br.ToBytes())
	}
}

func TestM2nGeoSearchStorePrepare(t *testing.T) {
	write, read := prepareGeoSearchStore([][]byte{[]byte("dest"), []byte("src"), []byte("FROMLONLAT"), []byte("0"), []byte("0"), []byte("BYRADIUS"), []byte("1"), []byte("km")})
	if len(write) != 1 || write[0] != "dest" {
		t.Fatalf("write keys: %#v", write)
	}
	if len(read) != 1 || read[0] != "src" {
		t.Fatalf("read keys: %#v", read)
	}
}
