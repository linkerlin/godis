package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps21ZRangeParseBeforeMiss(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "nosuch", "abc", "1")),
		"ERR value is not an integer or out of range")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZRANGE", "nosuch", "abc", "1", "BYSCORE")),
		"ERR min or max is not a float")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("ZREVRANGE", "nosuch", "abc", "1")),
		"ERR value is not an integer or out of range")
}

func TestGaps21HelloNOPROTO(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("HELLO", "4")),
		"NOPROTO unsupported protocol version")
}

func TestGaps21FlushExtraArgsSyntax(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("FLUSHDB", "ASYNC", "FOO")),
		"ERR syntax error")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("FLUSHALL", "ASYNC", "FOO")),
		"ERR syntax error")
}

func TestGaps21FunctionDeleteWording(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("FUNCTION", "DELETE", "nosuch")),
		"ERR Library not found")
}

func TestGaps21PFCountHLLWrongType(t *testing.T) {
	db := makeTestDB()
	db.Exec(nil, utils.ToCmdLine("SET", "s", "v"))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("PFCOUNT", "s")),
		"WRONGTYPE Key is not a valid HyperLogLog string value.")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("PFADD", "s", "x")),
		"WRONGTYPE Key is not a valid HyperLogLog string value.")
}

func TestGaps21CommandGetKeysArity(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("COMMAND", "GETKEYS", "SET")),
		"ERR Invalid number of arguments specified for command")
}

func TestGaps21ConfigMaxmemoryWording(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'maxmemory') - argument must be a memory value")
}

func TestGaps21TSAddBFReserveWording(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TS.ADD", "t", "abc", "1")),
		"ERR TSDB: invalid timestamp")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BF.RESERVE", "b", "abc", "100")),
		"ERR bad error rate")
}
