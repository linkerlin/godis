package database

import (
	"strconv"
	"testing"

	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/redis/protocol/asserts"
)

// TestCountSyncedSlavesRequiresAck proves WAIT/countSyncedSlaves use REPLCONF
// ACK offset, not the send-path watermark (sentOffset).
func TestCountSyncedSlavesRequiresAck(t *testing.T) {
	server := mockServer()
	fake := connection.NewFakeConn()
	slave := &slaveClient{
		conn:       fake,
		state:      slaveStateOnline,
		offset:     0,
		sentOffset: 0,
	}
	server.masterStatus.mu.Lock()
	server.masterStatus.bgSaveState = bgSaveFinish
	server.masterStatus.slaveMap[fake] = slave
	server.masterStatus.onlineSlaves[slave] = struct{}{}
	// Master tip is ahead of any ACK
	server.masterStatus.backlog.appendBytes([]byte("*1\r\n$4\r\nPING\r\n"))
	server.masterStatus.mu.Unlock()

	if n := server.countSyncedSlaves(); n != 0 {
		t.Fatalf("no ACK yet: countSyncedSlaves=%d want 0", n)
	}

	// Streaming write advances sentOffset only
	if err := server.masterSendUpdatesToSlave(); err != nil {
		t.Fatalf("masterSendUpdatesToSlave: %v", err)
	}
	server.masterStatus.mu.RLock()
	sent := slave.sentOffset
	acked := slave.offset
	tip := server.masterStatus.backlog.currentOffset
	server.masterStatus.mu.RUnlock()
	if sent < tip {
		t.Fatalf("sentOffset=%d should reach tip=%d after send", sent, tip)
	}
	if acked != 0 {
		t.Fatalf("ACK offset must stay 0 after send-only path, got %d", acked)
	}
	if n := server.countSyncedSlaves(); n != 0 {
		t.Fatalf("after send without ACK: countSyncedSlaves=%d want 0", n)
	}

	// WAIT with short timeout must also report 0
	waitReply := server.execWait([][]byte{[]byte("1"), []byte("20")})
	asserts.AssertIntReply(t, waitReply, 0)

	// Real REPLCONF ACK catches the slave up
	ack := server.execReplConf(fake, utils.ToCmdLine("ACK", strconv.FormatInt(tip, 10)))
	if protocol.IsErrorReply(ack) {
		t.Fatalf("REPLCONF ACK: %s", ack.ToBytes())
	}
	if n := server.countSyncedSlaves(); n != 1 {
		t.Fatalf("after ACK: countSyncedSlaves=%d want 1", n)
	}
	waitReply = server.execWait([][]byte{[]byte("1"), []byte("50")})
	asserts.AssertIntReply(t, waitReply, 1)
}

// TestSetSlaveOnlineBaselineCountsUntilNewWrites: after full/partial sync,
// setSlaveOnline baselines ACK to current tip; only later writes need a new ACK.
func TestSetSlaveOnlineBaselineCountsUntilNewWrites(t *testing.T) {
	server := mockServer()
	fake := connection.NewFakeConn()
	slave := &slaveClient{conn: fake}
	server.masterStatus.mu.Lock()
	server.masterStatus.bgSaveState = bgSaveFinish
	server.masterStatus.slaveMap[fake] = slave
	server.masterStatus.backlog.appendBytes([]byte("*1\r\n$4\r\nPING\r\n"))
	tip := server.masterStatus.backlog.currentOffset
	server.masterStatus.mu.Unlock()

	server.setSlaveOnline(slave, tip)
	if n := server.countSyncedSlaves(); n != 1 {
		t.Fatalf("baseline after setSlaveOnline: %d want 1", n)
	}

	server.masterStatus.mu.Lock()
	server.masterStatus.backlog.appendBytes([]byte("*1\r\n$4\r\nPING\r\n"))
	server.masterStatus.mu.Unlock()
	_ = server.masterSendUpdatesToSlave()
	if n := server.countSyncedSlaves(); n != 0 {
		t.Fatalf("new writes without ACK must not count: %d", n)
	}
}
