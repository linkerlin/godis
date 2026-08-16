package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps47ConfigPortBindMasteruserOOM(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "port", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'port') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "port", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'port') - argument must be between 0 and 65535 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "port", "65536")),
		"ERR CONFIG SET failed (possibly related to argument 'port') - argument must be between 0 and 65535 inclusive")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "port", "6399")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "port")),
		[]string{"port", "6399"})

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "bind", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'bind') - Failed to bind to specified addresses.")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "bind", "127.0.0.1")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "bind")),
		[]string{"bind", "127.0.0.1"})
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "bind", "* -::*")), "OK")

	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "masteruser", "repluser")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "masteruser")),
		[]string{"masteruser", "repluser"})
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "masteruser", "")), "OK")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "oom-score-adj", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'oom-score-adj') - argument(s) must be one of the following: no, yes, relative, absolute")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "oom-score-adj", "1")),
		"ERR CONFIG SET failed (possibly related to argument 'oom-score-adj') - argument(s) must be one of the following: no, yes, relative, absolute")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "oom-score-adj", "absolute")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "oom-score-adj")),
		[]string{"oom-score-adj", "absolute"})
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "oom-score-adj", "relative")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "oom-score-adj")),
		[]string{"oom-score-adj", "yes"})
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "oom-score-adj", "no")), "OK")
}
