package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps38FunctionLolwutCommand(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	db := makeTestDB()

	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("FUNCTION", "LIST", "LIBRARYNAME")),
		"ERR library name argument was not given")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("COMMAND", "GETKEYS")),
		"ERR wrong number of arguments for 'command|getkeys' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("COMMAND", "GETKEYSANDFLAGS")),
		"ERR wrong number of arguments for 'command|getkeysandflags' command")

	r := srv.Exec(c, utils.ToCmdLine("LOLWUT", "VERSION"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok || !strings.Contains(string(bulk.Arg), "Godis") {
		t.Fatalf("LOLWUT VERSION: %T %s", r, r.ToBytes())
	}
}

func TestGaps38ConfigZiplistAliases(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "hash-max-ziplist-entries", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'hash-max-ziplist-entries') - argument must be between 0 and 9223372036854775807 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "zset-max-ziplist-entries", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'zset-max-ziplist-entries') - argument must be between 0 and 9223372036854775807 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "list-max-ziplist-size", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'list-max-ziplist-size') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "hash-max-ziplist-value", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'hash-max-ziplist-value') - argument must be a memory value")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "zset-max-ziplist-value", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'zset-max-ziplist-value') - argument must be a memory value")

	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "hash-max-ziplist-entries", "128")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "hash-max-ziplist-entries")),
		[]string{"hash-max-ziplist-entries", "128"})
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "hash-max-listpack-entries")),
		[]string{"hash-max-listpack-entries", "128"})
}
