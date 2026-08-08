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
	sr, ok := result.(*StreamReadReply)
	if !ok || len(sr.buckets) == 0 {
		t.Fatalf("XREAD: expected data, got %T %v", result, result)
	}
	if sr.buckets[0].key != key {
		t.Fatalf("XREAD key: %q", sr.buckets[0].key)
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
	// ABORT with no failover in progress errors (Redis semantics).
	if r := server.Exec(c, utils.ToCmdLine("FAILOVER", "ABORT")); !protocol.IsErrorReply(r) ||
		!strings.Contains(string(r.ToBytes()), "No failover in progress") {
		t.Fatalf("FAILOVER ABORT without progress should error, got %s", r.ToBytes())
	}
	// With no connected replicas, FORCE/TO must still fail (real FAILOVER
	// semantics, unlike the old stub which returned OK unconditionally).
	for _, args := range [][]string{
		{"FAILOVER", "FORCE"},
		{"FAILOVER", "TO", "127.0.0.1", "6399", "TIMEOUT", "1000"},
	} {
		if r := server.Exec(c, utils.ToCmdLine(args...)); !protocol.IsErrorReply(r) {
			t.Fatalf("FAILOVER %v without replicas should error, got %s", args, r.ToBytes())
		}
	}
}
