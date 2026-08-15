package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps26ConfigWording(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "protected-mode", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'protected-mode') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "aof-use-rdb-preamble", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'aof-use-rdb-preamble') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "no-appendfsync-on-rewrite", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'no-appendfsync-on-rewrite') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-policy", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'maxmemory-policy') - argument(s) must be one of the following: volatile-lru, volatile-lfu, volatile-random, volatile-ttl, volatile-lrm, allkeys-lru, allkeys-lfu, allkeys-random, allkeys-lrm, noeviction")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replica-announce-port", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'replica-announce-port') - argument couldn't be parsed into an integer")
}

func TestGaps26ACLGenPassAndScriptDebug(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("ACL", "GENPASS", "abc")),
		"ERR value is not an integer or out of range")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("ACL", "GENPASS", "-1")),
		"ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("SCRIPT", "DEBUG", "FOO")),
		"ERR Use SCRIPT DEBUG YES/SYNC/NO")
}

func TestGaps26JSONGeoCMSTopK(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("JSON.TOGGLE", "nosuch", "$")),
		"ERR could not perform this operation on a key that doesn't exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("JSON.ARRINDEX", "nosuch", "$", "1")),
		"ERR Path does not exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("JSON.ARRTRIM", "nosuch", "$", "0", "1")),
		"ERR could not perform this operation on a key that doesn't exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("JSON.ARRINSERT", "nosuch", "$", "0", "1")),
		"ERR could not perform this operation on a key that doesn't exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("JSON.ARRPOP", "nosuch")),
		"ERR could not perform this operation on a key that doesn't exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("JSON.NUMMULTBY", "nosuch", "$", "2")),
		"ERR could not perform this operation on a key that doesn't exist")

	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("GEOADD", "g", "NX", "XX", "1", "1", "m")),
		"ERR syntax error")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("GEOSEARCH", "g", "FROMMEMBER", "m", "BYRADIUS", "1", "xx")),
		"ERR unsupported unit provided. please use M, KM, FT, MI")

	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("CMS.INITBYDIM", "c", "0", "5")),
		"CMS: invalid width")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("CMS.INITBYDIM", "c2", "5", "0")),
		"CMS: invalid depth")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("BF.INFO", "nosuch")),
		"ERR not found")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("CF.INFO", "nosuch")),
		"ERR not found")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TOPK.ADD", "nosuch", "a")),
		"TopK: key does not exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("TOPK.INFO", "nosuch")),
		"TopK: key does not exist")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "SETID", "nosuch", "g", "0")),
		"ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.")
}
