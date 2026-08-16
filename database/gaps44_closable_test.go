package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps44ACLLogResetExtraArgs(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	msg := "ERR unknown subcommand or wrong number of arguments for 'LOG'. Try ACL HELP."
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("ACL", "LOG", "RESET", "x")), msg)
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("ACL", "LOG", "1", "x")), msg)
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("ACL", "LOG", "abc")),
		"ERR value is not an integer or out of range")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("ACL", "LOG", "RESET")), "OK")
}

func TestGaps44ConfigActiveExpireAndTLS(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "active-expire-effort", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'active-expire-effort') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "active-expire-effort", "0")),
		"ERR CONFIG SET failed (possibly related to argument 'active-expire-effort') - argument must be between 1 and 10 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "active-expire-effort", "11")),
		"ERR CONFIG SET failed (possibly related to argument 'active-expire-effort') - argument must be between 1 and 10 inclusive")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "active-expire-effort", "5")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "active-expire-effort")),
		[]string{"active-expire-effort", "5"})

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-auth-clients", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'tls-auth-clients') - argument(s) must be one of the following: no, yes, optional")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-auth-clients", "optional")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "tls-auth-clients")),
		[]string{"tls-auth-clients", "optional"})

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-replication", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'tls-replication') - argument must be 'yes' or 'no'")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-replication", "no")), "OK")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-cluster", "no")), "OK")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-session-caching", "yes")), "OK")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-prefer-server-ciphers", "no")), "OK")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-session-cache-size", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'tls-session-cache-size') - argument must be between 0 and 2147483647 inclusive")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-session-cache-size", "0")), "OK")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-session-cache-timeout", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'tls-session-cache-timeout') - argument must be between 0 and 2147483647 inclusive")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-session-cache-timeout", "300")), "OK")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-announce-tls-port", "65536")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-announce-tls-port') - argument must be between 0 and 65535 inclusive")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-announce-tls-port", "0")), "OK")

	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-cert-file", "/tmp/cert.pem")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "tls-cert-file")),
		[]string{"tls-cert-file", "/tmp/cert.pem"})
}
