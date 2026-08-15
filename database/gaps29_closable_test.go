package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps29ConfigWording(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "lazyfree-lazy-eviction", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'lazyfree-lazy-eviction') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "jemalloc-bg-thread", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'jemalloc-bg-thread') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "activedefrag", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'activedefrag') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "aof-rewrite-incremental-fsync", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'aof-rewrite-incremental-fsync') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "replica-lazy-flush", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'replica-lazy-flush') - argument must be 'yes' or 'no'")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-announce-port", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-announce-port') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "busy-reply-threshold", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'busy-reply-threshold') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "acllog-max-len", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'acllog-max-len') - argument couldn't be parsed into an integer")
}

func TestGaps29ArityACLFunctionWait(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("MODULE", "HELP", "x")),
		"ERR wrong number of arguments for 'module|help' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("OBJECT", "HELP", "x")),
		"ERR wrong number of arguments for 'object|help' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("BGREWRITEAOF", "x")),
		"ERR wrong number of arguments for 'bgrewriteaof' command")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("FUNCTION", "LIST", "x")),
		"ERR Unknown argument x")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("ACL", "LOG", "abc")),
		"ERR value is not an integer or out of range")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("ACL", "CAT", "x", "y")),
		"ERR unknown subcommand or wrong number of arguments for 'CAT'. Try ACL HELP.")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("WAITAOF", "0", "abc", "0")),
		"ERR value is out of range, must be positive")
}

func TestGaps29XGroupNOGROUP(t *testing.T) {
	db := makeTestDB()
	asserts.AssertNotError(t, db.Exec(nil, utils.ToCmdLine("XADD", "x", "*", "a", "1")))
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "CREATECONSUMER", "x", "nosuch", "c")),
		"NOGROUP No such consumer group 'nosuch' for key name 'x'")
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("XGROUP", "DELCONSUMER", "x", "nosuch", "c")),
		"NOGROUP No such consumer group 'nosuch' for key name 'x'")
}
