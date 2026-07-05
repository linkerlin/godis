package database

import (
	"strconv"
	"strings"
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
)

func TestInfoClientTrackingFields(t *testing.T) {
	c := connection.NewFakeConn()
	_ = EnableTracking(c, "default", nil, "", false)

	ret := testServer.Exec(c, utils.ToCmdLine("INFO", "client"))
	bulk, ok := ret.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("expected bulk reply, got %T", ret)
	}
	body := string(bulk.Arg)
	for _, field := range []string{
		"connected_clients:",
		"blocked_clients:",
		"tracking_clients:",
		"tracking_total_keys:",
	} {
		if !strings.Contains(body, field) {
			t.Fatalf("INFO client missing %s in:\n%s", field, body)
		}
	}

	DisableTracking(strconv.FormatInt(c.GetClientID(), 10))
}

func TestInfoCPUSectionNonNegative(t *testing.T) {
	ret := testServer.Exec(connection.NewFakeConn(), utils.ToCmdLine("INFO", "cpu"))
	bulk, ok := ret.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("expected bulk reply, got %T", ret)
	}
	if !strings.Contains(string(bulk.Arg), "used_cpu_user:") {
		t.Fatalf("missing used_cpu_user in cpu section")
	}
}

func TestInfoClientMaxClientsField(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{MaxClients: 42}
	defer func() { config.Properties = old }()

	ret := testServer.Exec(connection.NewFakeConn(), utils.ToCmdLine("INFO", "client"))
	bulk, ok := ret.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("expected bulk reply, got %T", ret)
	}
	body := string(bulk.Arg)
	if !strings.Contains(body, "maxclients:42") {
		t.Fatalf("INFO client missing maxclients:42 in:\n%s", body)
	}
}
