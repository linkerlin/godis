package database

import (
	"strings"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestGaps52WaitAOFConfigLatencyHelp(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{Databases: 16, AppendOnly: false}
	defer func() { config.Properties = old }()

	srv := MustNewStandaloneServer()
	c := connection.NewFakeConn()

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("WAITAOF", "1", "0", "0")),
		"ERR WAITAOF cannot be used when numlocal is set but appendonly is disabled.")
	r := srv.Exec(c, utils.ToCmdLine("WAITAOF", "0", "0", "0"))
	raw, ok := r.(*protocol.MultiRawReply)
	if !ok || len(raw.Replies) != 2 {
		t.Fatalf("WAITAOF: %T %s", r, r.ToBytes())
	}
	asserts.AssertIntReply(t, raw.Replies[0], 0)
	asserts.AssertIntReply(t, raw.Replies[1], 0)

	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "rdb-del-sync-files")),
		[]string{"rdb-del-sync-files", "no"})
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "aof-disable-auto-gc")),
		[]string{"aof-disable-auto-gc", "no"})
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "appenddirname")),
		[]string{"appenddirname", "appendonlydir"})

	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "rdb-del-sync-files", "yes")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "rdb-del-sync-files")),
		[]string{"rdb-del-sync-files", "yes"})
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "rdb-del-sync-files", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'rdb-del-sync-files') - argument must be 'yes' or 'no'")

	asserts.AssertStatusReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "aof-disable-auto-gc", "yes")), "OK")
	asserts.AssertMultiBulkReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "GET", "aof-disable-auto-gc")),
		[]string{"aof-disable-auto-gc", "yes"})
	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "aof-disable-auto-gc", "maybe")),
		"ERR CONFIG SET failed (possibly related to argument 'aof-disable-auto-gc') - argument must be 'yes' or 'no'")

	asserts.AssertErrReply(t, srv.Exec(c, utils.ToCmdLine("CONFIG", "SET", "appenddirname", "other")),
		"ERR CONFIG SET failed (possibly related to argument 'appenddirname') - can't set immutable config")

	help := srv.Exec(c, utils.ToCmdLine("LATENCY", "HELP"))
	hs := string(help.ToBytes())
	if !strings.Contains(hs, "LATENCY <subcommand>") || !strings.Contains(hs, "human readable latency analysis") {
		t.Fatalf("LATENCY HELP want Redis layout: %s", hs)
	}
}
