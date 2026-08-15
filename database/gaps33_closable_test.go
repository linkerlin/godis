package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps33ConfigWording(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "hash-max-listpack-value", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'hash-max-listpack-value') - argument must be a memory value")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "zset-max-listpack-value", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'zset-max-listpack-value') - argument must be a memory value")
}

func TestGaps33ClientStreamBloomVector(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "TRACKING", "ON", "BCAST", "OPTIN")),
		"ERR OPTIN and OPTOUT are not compatible with BCAST")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CLIENT", "TRACKING", "ON", "BCAST", "OPTOUT")),
		"ERR OPTIN and OPTOUT are not compatible with BCAST")

	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "HELP", "x")),
		"ERR wrong number of arguments for 'xgroup|help' command")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BF.RESERVE", "bf", "0.01", "0")),
		"ERR capacity must be in the range [1, 1073741824]")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("VADD", "v", "VALUES", "2", "1")),
		"ERR invalid vector specification")
}
