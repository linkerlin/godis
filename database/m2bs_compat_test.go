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

func TestM2bsHelloModeFollowsCluster(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{ClusterEnable: false, Databases: 16}
	defer func() { config.Properties = old }()

	c := connection.NewFakeConn()
	r := Hello(c, utils.ToCmdLine("2"))
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("HELLO: %T", r)
	}
	found := false
	for i := 0; i+1 < len(mb.Args); i += 2 {
		if string(mb.Args[i]) == "mode" && string(mb.Args[i+1]) == "standalone" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("want mode=standalone: %s", r.ToBytes())
	}

	config.Properties.ClusterEnable = true
	r = Hello(c, utils.ToCmdLine("2"))
	mb = r.(*protocol.MultiBulkReply)
	found = false
	for i := 0; i+1 < len(mb.Args); i += 2 {
		if string(mb.Args[i]) == "mode" && string(mb.Args[i+1]) == "cluster" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("want mode=cluster: %s", r.ToBytes())
	}
}

func TestM2bsACLHelpIncludesHelp(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("ACL", "HELP"))
	mb, ok := r.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("ACL HELP: %T %s", r, r.ToBytes())
	}
	joined := string(bytesJoin(mb.Args))
	if !strings.Contains(joined, "HELP") || !strings.Contains(joined, "WHOAMI") {
		t.Fatalf("ACL HELP missing HELP/WHOAMI: %s", joined)
	}
}

func TestM2bsInfoErrorstats(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	r := server.Exec(c, utils.ToCmdLine("INFO", "errorstats"))
	bulk, ok := r.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO errorstats: %T %s", r, r.ToBytes())
	}
	if !strings.Contains(string(bulk.Arg), "# Errorstats") {
		t.Fatalf("want # Errorstats: %s", bulk.Arg)
	}
}

func TestM2bsNotifyKeyspaceEventsConfig(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{Databases: 16, NotifyKeyspaceEvents: ""}
	defer func() { config.Properties = old }()

	server := MustNewStandaloneServer()
	defer server.Close()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "notify-keyspace-events", "KEA")), "OK")
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "notify-keyspace-events")),
		[]string{"notify-keyspace-events", "KEA"})
}

func TestM2bsPubsubShardChannels(t *testing.T) {
	server := MustNewStandaloneServer()
	defer server.Close()
	sub := connection.NewFakeConn()
	pub := connection.NewFakeConn()
	_ = server.Exec(sub, utils.ToCmdLine("SSUBSCRIBE", "sch:m2bs"))

	ch := server.Exec(pub, utils.ToCmdLine("PUBSUB", "SHARDCHANNELS"))
	mb, ok := ch.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("SHARDCHANNELS: %T %s", ch, ch.ToBytes())
	}
	found := false
	for _, a := range mb.Args {
		if string(a) == "sch:m2bs" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("missing sch:m2bs: %s", ch.ToBytes())
	}

	asserts.AssertIntReply(t, server.Exec(pub, utils.ToCmdLine("PUBSUB", "NUMSHARDCHANNELS")), 1)
	asserts.AssertMultiBulkReply(t, server.Exec(pub, utils.ToCmdLine("PUBSUB", "SHARDNUMSUB", "sch:m2bs")),
		[]string{"sch:m2bs", "1"})
}
