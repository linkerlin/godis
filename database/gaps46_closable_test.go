package database

import (
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps46QuitExtraArgsAndImmutableConfig(t *testing.T) {
	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("QUIT")), "OK")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("QUIT", "x")), "OK")
	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("QUIT", "a", "b")), "OK")

	immutable := []string{
		"databases", "disable-thp", "enable-debug-command", "enable-module-command",
		"enable-protected-configs", "socket-mark-id", "unixsocket", "unixsocketperm",
		"syslog-enabled", "syslog-ident", "syslog-facility", "cluster-port",
		"cluster-config-file", "supervised",
	}
	for _, key := range immutable {
		asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", key, "foo")),
			"ERR CONFIG SET failed (possibly related to argument '"+key+"') - can't set immutable config")
	}

	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "disable-thp")),
		[]string{"disable-thp", "yes"})
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "supervised")),
		[]string{"supervised", "no"})
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "cluster-config-file")),
		[]string{"cluster-config-file", "nodes.conf"})
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "unixsocketperm")),
		[]string{"unixsocketperm", "0"})
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "enable-debug-command")),
		[]string{"enable-debug-command", "no"})
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "syslog-ident")),
		[]string{"syslog-ident", "redis"})
}
