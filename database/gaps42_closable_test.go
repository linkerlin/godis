package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps42FCallROAndLolwutNegVersion(t *testing.T) {
	db := makeTestDB()
	asserts.AssertErrReply(t, db.Exec(nil, utils.ToCmdLine("FCALL_RO")),
		"ERR wrong number of arguments for 'fcall_ro' command")

	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()
	r := srv.Exec(c, utils.ToCmdLine("LOLWUT", "VERSION", "-1"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("LOLWUT VERSION -1 should succeed like Redis, got %s", r.ToBytes())
	}
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("LOLWUT", "VERSION", "abc")),
		"ERR value is not an integer or out of range")
}

func TestGaps42ConfigCOBShutdownDefrag(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "client-output-buffer-limit", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'client-output-buffer-limit') - Wrong number of arguments in buffer limit configuration.")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "client-output-buffer-limit", "foo 0 0 0")),
		"ERR CONFIG SET failed (possibly related to argument 'client-output-buffer-limit') - Invalid client class specified in buffer limit configuration.")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "client-output-buffer-limit", "normal -1 0 0")),
		"ERR CONFIG SET failed (possibly related to argument 'client-output-buffer-limit') - Error in hard, soft or soft_seconds setting in buffer limit configuration.")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "client-output-buffer-limit", "normal 0 0 0")), "OK")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "shutdown-timeout", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'shutdown-timeout') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "shutdown-timeout", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'shutdown-timeout') - argument must be between 0 and 2147483647 inclusive")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "shutdown-timeout", "0")), "OK")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "shutdown-on-sigint", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'shutdown-on-sigint') - argument(s) must be one of the following: default, save, nosave, now, force")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "shutdown-on-sigint", "default")), "OK")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "shutdown-on-sigterm", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'shutdown-on-sigterm') - argument(s) must be one of the following: default, save, nosave, now, force")

	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-announce-human-nodename", "n1")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-announce-human-nodename")),
		[]string{"cluster-announce-human-nodename", "n1"})
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "locale-collate", "C")), "OK")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "latency-tracking-info-percentiles", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'latency-tracking-info-percentiles') - Invalid latency-tracking-info-percentiles parameters")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "latency-tracking-info-percentiles", "101")),
		"ERR CONFIG SET failed (possibly related to argument 'latency-tracking-info-percentiles') - latency-tracking-info-percentiles parameters should sit between [0.0,100.0]")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "latency-tracking-info-percentiles", "50 99")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "latency-tracking-info-percentiles")),
		[]string{"latency-tracking-info-percentiles", "50 99"})

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "active-defrag-ignore-bytes", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'active-defrag-ignore-bytes') - argument must be a memory value")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "active-defrag-ignore-bytes", "0")),
		"ERR CONFIG SET failed (possibly related to argument 'active-defrag-ignore-bytes') - argument must be between 1 and 9223372036854775807 inclusive")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "active-defrag-ignore-bytes", "100")), "OK")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "active-defrag-threshold-lower", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'active-defrag-threshold-lower') - argument must be between 0 and 1000 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "active-defrag-cycle-min", "100")),
		"ERR CONFIG SET failed (possibly related to argument 'active-defrag-cycle-min') - argument must be between 1 and 99 inclusive")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "active-defrag-max-scan-fields", "0")),
		"ERR CONFIG SET failed (possibly related to argument 'active-defrag-max-scan-fields') - argument must be between 1 and 9223372036854775807 inclusive")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "active-defrag-cycle-max", "25")), "OK")
}
