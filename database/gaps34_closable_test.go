package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps34ConfigAndPause(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "stream-node-max-bytes", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'stream-node-max-bytes') - argument must be a memory value")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "PAUSE", "100", "FOO")),
		"ERR CLIENT PAUSE mode must be WRITE or ALL")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("SLOWLOG", "LEN", "x")),
		"ERR wrong number of arguments for 'slowlog|len' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("SLOWLOG", "RESET", "x")),
		"ERR wrong number of arguments for 'slowlog|reset' command")
}

func TestGaps34StreamClaimAutoclaim(t *testing.T) {
	db := makeTestDB()
	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XADD", "xs", "*", "a", "1")))
	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATE", "xs", "g", "0")))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XAUTOCLAIM", "xs", "g", "c", "0", "0-0", "COUNT", "0")),
		"ERR COUNT must be > 0")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XAUTOCLAIM", "xs", "g", "c", "0", "0-0", "COUNT", "abc")),
		"ERR COUNT must be > 0")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XCLAIM", "xs", "g", "c", "0", "0-0", "IDLE")),
		"ERR Unrecognized XCLAIM option 'IDLE'")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XCLAIM", "xs", "g", "c", "0", "0-0", "IDLE", "abc")),
		"ERR Invalid IDLE option argument for XCLAIM")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XCLAIM", "xs", "g", "c", "0", "0-0", "TIME", "abc")),
		"ERR Invalid TIME option argument for XCLAIM")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XCLAIM", "xs", "g", "c", "0", "0-0", "RETRYCOUNT", "abc")),
		"ERR Invalid RETRYCOUNT option argument for XCLAIM")
}
