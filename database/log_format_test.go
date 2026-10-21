package database

import (
	"testing"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestConfigLogFormat verifies CONFIG GET/SET log-format and the logger switch.
func TestConfigLogFormat(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	old := config.Properties.LogFormat
	t.Cleanup(func() {
		_ = server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "log-format", "text"))
		config.Properties.LogFormat = old
	})

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "log-format", "json")), "OK")
	if got := logger.Format(); got != "json" {
		t.Fatalf("logger.Format() = %q, want json", got)
	}
	asserts.AssertMultiBulkReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "log-format")),
		[]string{"log-format", "json"})

	r := server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "log-format", "yaml"))
	if !protocol.IsErrorReply(r) {
		t.Fatalf("invalid log-format should fail, got %s", r.ToBytes())
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "log-format", "text")), "OK")
}
