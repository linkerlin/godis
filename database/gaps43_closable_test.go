package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps43ConfigOOMPropClusterProc(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "oom-score-adj-values", "0 200")),
		"ERR CONFIG SET failed (possibly related to argument 'oom-score-adj-values') - wrong number of arguments")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "oom-score-adj-values", "0 abc 800")),
		"ERR CONFIG SET failed (possibly related to argument 'oom-score-adj-values') - Invalid oom-score-adj-values, elements must be between -2000 and 2000.")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "oom-score-adj-values", "0 200 800")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "oom-score-adj-values")),
		[]string{"oom-score-adj-values", "0 200 800"})

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "propagation-error-behavior", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'propagation-error-behavior') - argument(s) must be one of the following: ignore, panic, panic-on-replicas")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "propagation-error-behavior", "ignore")), "OK")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "hide-user-data-from-log", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'hide-user-data-from-log') - argument must be 'yes' or 'no'")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "hide-user-data-from-log", "yes")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "hide-user-data-from-log")),
		[]string{"hide-user-data-from-log", "yes"})

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-replica-no-failover", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-replica-no-failover') - argument must be 'yes' or 'no'")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-replica-no-failover", "no")), "OK")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-allow-pubsubshard-when-down", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-allow-pubsubshard-when-down') - argument must be 'yes' or 'no'")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-allow-pubsubshard-when-down", "yes")), "OK")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "proc-title-template", "{nope}")),
		"ERR CONFIG SET failed (possibly related to argument 'proc-title-template') - template format is invalid or contains unknown variables")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "proc-title-template", "{title}")),
		"ERR CONFIG SET failed (possibly related to argument 'proc-title-template') - template format is invalid or contains unknown variables")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "proc-title-template", "")),
		"ERR CONFIG SET failed (possibly related to argument 'proc-title-template') - template format is invalid or contains unknown variables")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "proc-title-template", "{title} {listen-addr} {server-mode}")), "OK")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "proc-title-template", "godis")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "proc-title-template")),
		[]string{"proc-title-template", "godis"})
}
