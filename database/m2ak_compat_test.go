package database

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/pubsub"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestM2akClientPauseBlocksWrite(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	t.Cleanup(func() { _ = server.Exec(c, utils.ToCmdLine("CLIENT", "UNPAUSE")) })
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CLIENT", "PAUSE", "300", "WRITE")), "OK")

	done := make(chan redis.Reply, 1)
	go func() {
		done <- server.Exec(c, utils.ToCmdLine("SET", "pause-k", "1"))
	}()

	select {
	case <-done:
		t.Fatal("SET should block while WRITE pause is active")
	case <-time.After(80 * time.Millisecond):
		// still blocked — good
	}

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CLIENT", "UNPAUSE")), "OK")
	select {
	case r := <-done:
		asserts.AssertStatusReply(t, r, "OK")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("SET did not complete after UNPAUSE")
	}

	// GET should not block under WRITE pause
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("CLIENT", "PAUSE", "200", "WRITE")), "OK")
	got := server.Exec(c, utils.ToCmdLine("GET", "pause-k"))
	asserts.AssertBulkReply(t, got, "1")
	server.Exec(c, utils.ToCmdLine("CLIENT", "UNPAUSE"))
}

func TestM2akInfoPubsubAndOps(t *testing.T) {
	server := getTestServer()
	sub := connection.NewFakeConn()
	_ = pubsub.Subscribe(server.hub, sub, [][]byte{[]byte("m2ak-ch")})
	_ = pubsub.PSubscribe(server.hub, sub, [][]byte{[]byte("m2ak-*")})

	for i := 0; i < 30; i++ {
		RecordCommand("set", 10, false)
	}

	c := connection.NewFakeConn()
	info := server.Exec(c, utils.ToCmdLine("INFO", "stats"))
	bulk, ok := info.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("INFO stats: %T %s", info, info.ToBytes())
	}
	body := string(bulk.Arg)
	if server.hub.NumChannels() < 1 {
		t.Fatal("expected hub channel")
	}
	if strings.Contains(body, "pubsub_channels:0\r\n") {
		t.Fatalf("pubsub_channels still 0:\n%s", body)
	}
	if server.hub.NumPat() < 1 {
		t.Fatal("expected hub pattern")
	}
	if strings.Contains(body, "pubsub_patterns:0\r\n") {
		t.Fatalf("pubsub_patterns still 0:\n%s", body)
	}
	if !strings.Contains(body, "instantaneous_ops_per_sec:") {
		t.Fatalf("missing ops/sec:\n%s", body)
	}
	if getInstantaneousOpsPerSec() <= 0 {
		t.Fatalf("instantaneous_ops_per_sec=%d want >0", getInstantaneousOpsPerSec())
	}
}

func TestM2akScriptFunctionFlushMode(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SCRIPT", "FLUSH", "ASYNC")), "OK")
	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("SCRIPT", "FLUSH", "SYNC")), "OK")
	r := server.Exec(c, utils.ToCmdLine("SCRIPT", "FLUSH", "FAST"))
	asserts.AssertErrReply(t, r, "ERR SCRIPT FLUSH only support SYNC|ASYNC mode")

	asserts.AssertStatusReply(t, server.Exec(c, utils.ToCmdLine("FUNCTION", "FLUSH", "ASYNC")), "OK")
	bad := server.Exec(c, utils.ToCmdLine("FUNCTION", "FLUSH", "NOW"))
	asserts.AssertErrReply(t, bad, "ERR FUNCTION FLUSH only supports SYNC|ASYNC option")
}

func TestM2akOpsWindow(t *testing.T) {
	resetOpsWindow()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			noteOps()
		}()
	}
	wg.Wait()
	if getInstantaneousOpsPerSec() <= 0 {
		t.Fatal("expected ops window activity")
	}
}
