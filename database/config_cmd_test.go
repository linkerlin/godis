package database

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestConfigGetSlowlogAndAclLog(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	ret := server.Exec(c, utils.ToCmdLine("CONFIG", "GET", "slowlog-max-len", "acllog-max-len"))
	got := string(ret.ToBytes())
	if !strings.Contains(got, "slowlog-max-len") || !strings.Contains(got, "acllog-max-len") {
		t.Fatalf("missing config keys in %q", got)
	}
	if _, ok := ret.(*protocol.MapReply); !ok {
		t.Fatalf("CONFIG GET should return MapReply, got %T", ret)
	}
	if protocol.ReplyToRESP3(ret)[0] != '%' {
		t.Fatalf("CONFIG GET RESP3 should be map: %q", protocol.ReplyToRESP3(ret))
	}
}

func TestConfigSetSlowlogHotReload(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slowlog-max-len", "2")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slowlog-log-slower-than", "0")), "OK")

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("slowlog-hot-%d", i)
		server.Exec(c, utils.ToCmdLine("SET", key, "v"))
	}

	ret := server.Exec(c, utils.ToCmdLine("SLOWLOG", "LEN"))
	intReply, ok := ret.(*protocol.IntReply)
	if !ok || intReply.Code != 2 {
		t.Fatalf("expected slowlog len 2, got %v", ret)
	}

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "slowlog-max-len", "1")), "OK")
	ret = server.Exec(c, utils.ToCmdLine("SLOWLOG", "LEN"))
	intReply, ok = ret.(*protocol.IntReply)
	if !ok || intReply.Code != 1 {
		t.Fatalf("expected slowlog len 1 after shrink, got %v", ret)
	}
}

func TestConfigSetAclLogMaxLenHotReload(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	resetACLLog()
	addACLLogEntry(nil, "command", "toplevel", "set", "u1")
	addACLLogEntry(nil, "command", "toplevel", "get", "u2")
	addACLLogEntry(nil, "command", "toplevel", "del", "u3")

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CONFIG", "SET", "acllog-max-len", "2")), "OK")

	ret := server.Exec(c, utils.ToCmdLine("ACL", "LOG"))
	multi, ok := ret.(*protocol.MultiBulkReply)
	if !ok {
		t.Fatalf("expected multi bulk, got %T", ret)
	}
	if len(multi.Args) != 2 {
		t.Fatalf("expected 2 acl log entries after trim, got %d", len(multi.Args))
	}
	if !strings.Contains(string(multi.Args[0]), "get") || !strings.Contains(string(multi.Args[1]), "del") {
		t.Fatalf("expected newest acl log entries, got %v", multi.Args)
	}
}

func TestSlowLogger_SetMaxLen(t *testing.T) {
	logger := NewSlowLogger(5, 0)
	for i := 0; i < 5; i++ {
		start := time.Now()
		logger.Record(start, utils.ToCmdLine("CMD", strconv.Itoa(i)), "client", "")
	}
	if logger.Len() != 5 {
		t.Fatalf("expected 5 entries, got %d", logger.Len())
	}

	logger.SetMaxLen(2)
	if logger.Len() != 2 {
		t.Fatalf("expected 2 entries after shrink, got %d", logger.Len())
	}
	entries := logger.GetEntries(2)
	if len(entries) != 2 || string(entries[0].Command[1]) != "4" || string(entries[1].Command[1]) != "3" {
		t.Fatalf("unexpected entries after shrink: %+v", entries)
	}

	logger.SetThreshold(1000)
	logger.SetMaxLen(4)
	if logger.threshold != 1000 {
		t.Fatalf("threshold not updated")
	}
}
