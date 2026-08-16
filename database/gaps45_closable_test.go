package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps45ConfigTLSPortAndMaxmemoryClients(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-port", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'tls-port') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-port", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'tls-port') - argument must be between 0 and 65535 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-port", "65536")),
		"ERR CONFIG SET failed (possibly related to argument 'tls-port') - argument must be between 0 and 65535 inclusive")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-port", "0")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "tls-port")),
		[]string{"tls-port", "0"})

	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-dh-params-file", "/tmp/dh.pem")), "OK")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-ciphersuites", "TLS_AES_128_GCM_SHA256")), "OK")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-client-cert-file", "/tmp/c.pem")), "OK")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-client-key-file", "/tmp/k.pem")), "OK")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-key-file-pass", "secret")), "OK")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "tls-client-key-file-pass", "secret")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "tls-client-cert-file")),
		[]string{"tls-client-cert-file", "/tmp/c.pem"})

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-clients", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'maxmemory-clients') - argument must be a memory or percent value")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-clients", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'maxmemory-clients') - argument must be a memory or percent value")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-clients", "101%")),
		"ERR CONFIG SET failed (possibly related to argument 'maxmemory-clients') - percentage argument must be less or equal to 100")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-clients", "10%")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "maxmemory-clients")),
		[]string{"maxmemory-clients", "10%"})
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "maxmemory-clients", "0")), "OK")
}
