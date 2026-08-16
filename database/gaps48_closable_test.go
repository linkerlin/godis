package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps48SlaveAliasesInfoPFSelfTest(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-slave-validity-factor", "abc")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-slave-validity-factor') - argument couldn't be parsed into an integer")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-slave-validity-factor", "-1")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-slave-validity-factor') - argument must be between 0 and 2147483647 inclusive")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-slave-validity-factor", "7")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-slave-validity-factor")),
		[]string{"cluster-slave-validity-factor", "7"})
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-replica-validity-factor")),
		[]string{"cluster-replica-validity-factor", "7"})

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-slave-no-failover", "foo")),
		"ERR CONFIG SET failed (possibly related to argument 'cluster-slave-no-failover') - argument must be 'yes' or 'no'")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "cluster-slave-no-failover", "yes")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-slave-no-failover")),
		[]string{"cluster-slave-no-failover", "yes"})

	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slave-ignore-maxmemory", "no")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "slave-ignore-maxmemory")),
		[]string{"slave-ignore-maxmemory", "no"})
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slave-lazy-flush", "yes")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "slave-lazy-flush")),
		[]string{"slave-lazy-flush", "yes"})

	unknown := srv.Exec(c, utils.ToCmdLine("INFO", "nosuchsection"))
	bulk, ok := unknown.(*protocol.BulkReply)
	if !ok || len(bulk.Arg) != 0 {
		t.Fatalf("INFO unknown section want empty bulk, got %T %q", unknown, unknown.ToBytes())
	}
	multi := srv.Exec(c, utils.ToCmdLine("INFO", "server", "clients"))
	mb, ok := multi.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO multi: %T", multi)
	}
	s := string(mb.Arg)
	if !strings.Contains(s, "# Server") || !strings.Contains(s, "# Clients") {
		t.Fatalf("INFO server clients missing sections: %s", s)
	}

	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("PFSELFTEST")), "OK")
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("PFSELFTEST", "x")),
		"ERR wrong number of arguments for 'pfselftest' command")
}
