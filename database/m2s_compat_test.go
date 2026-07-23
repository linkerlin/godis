package database

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2sXReadWokenByXAdd(t *testing.T) {
	testDB.Flush()
	key := "m2s:xr"
	var wg sync.WaitGroup
	wg.Add(1)
	var result redis.Reply
	go func() {
		defer wg.Done()
		result = testDB.Exec(nil, utils.ToCmdLine(
			"XREAD", "BLOCK", "2000", "STREAMS", key, "$",
		))
	}()
	time.Sleep(50 * time.Millisecond)
	idReply := testDB.Exec(nil, utils.ToCmdLine("XADD", key, "*", "f", "v"))
	if protocol.IsErrorReply(idReply) {
		t.Fatalf("XADD: %s", idReply.ToBytes())
	}
	wg.Wait()
	mb, ok := result.(*protocol.MultiBulkReply)
	if !ok || len(mb.Args) < 2 {
		t.Fatalf("XREAD: expected data, got %T %v", result, result)
	}
	if string(mb.Args[0]) != key {
		t.Fatalf("XREAD key: %q", mb.Args[0])
	}
}

func TestM2sXReadBlockTimeout(t *testing.T) {
	testDB.Flush()
	start := time.Now()
	result := testDB.Exec(nil, utils.ToCmdLine(
		"XREAD", "BLOCK", "150", "STREAMS", "m2s:empty", "$",
	))
	asserts.AssertNullBulk(t, result)
	if time.Since(start) < 100*time.Millisecond {
		t.Fatal("XREAD returned too early")
	}
}

func TestM2sFailover(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	noRepl := server.Exec(c, utils.ToCmdLine("FAILOVER"))
	if !protocol.IsErrorReply(noRepl) || !strings.Contains(string(noRepl.ToBytes()), "replicas") {
		t.Fatalf("expected no-replicas error, got %s", noRepl.ToBytes())
	}
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("FAILOVER", "ABORT")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("FAILOVER", "FORCE")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine(
		"FAILOVER", "TO", "127.0.0.1", "6399", "TIMEOUT", "1000",
	)), "OK")
}
