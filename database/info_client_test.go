package database

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

func TestInfoReplicationSection(t *testing.T) {
	c := connection.NewFakeConn()
	testServer.Exec(c, utils.ToCmdLine("SET", "info-repl-key", "v"))
	ret := testServer.Exec(c, utils.ToCmdLine("INFO", "replication"))
	bulk, ok := ret.(*protocol.BulkReply)
	if !ok {
		t.Fatalf("expected bulk reply, got %T", ret)
	}
	body := string(bulk.Arg)
	if !strings.Contains(body, "master_repl_offset:") {
		t.Fatal("INFO replication missing master_repl_offset")
	}
	if strings.Contains(body, "master_repl_offset:0\r\n") && testServer.masterStatus != nil &&
		testServer.masterStatus.backlog != nil && testServer.masterStatus.backlog.currentOffset > 0 {
		t.Fatalf("expected non-zero repl offset, got:\n%s", body)
	}
}

func TestClientPauseAndUnpause(t *testing.T) {
	server := getTestServer()
	c := connection.NewFakeConn()

	ret := testServer.Exec(c, utils.ToCmdLine("CLIENT", "PAUSE", "500", "WRITE"))
	asserts.AssertStatusReply(t, ret, "OK")
	if !server.CheckClientPause(true) {
		t.Fatal("write commands should be paused")
	}
	if server.CheckClientPause(false) {
		t.Fatal("read commands should not be paused in WRITE mode")
	}

	ret = testServer.Exec(c, utils.ToCmdLine("CLIENT", "UNPAUSE"))
	asserts.AssertStatusReply(t, ret, "OK")
	if server.CheckClientPause(true) {
		t.Fatal("pause should be cleared after UNPAUSE")
	}
}

func TestClientPauseConcurrent(t *testing.T) {
	server := getTestServer()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mode := "WRITE"
			if i%2 == 0 {
				mode = "ALL"
			}
			server.setClientPause(50, mode)
			_ = server.CheckClientPause(true)
			server.clearClientPause()
		}(i)
	}
	wg.Wait()
}

func TestClientPauseExpires(t *testing.T) {
	server := getTestServer()
	server.setClientPause(10, "ALL")
	time.Sleep(20 * time.Millisecond)
	if server.CheckClientPause(true) {
		t.Fatal("pause should expire automatically")
	}
}
