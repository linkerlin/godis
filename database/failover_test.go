package database

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linkerlin/godis/aof"
	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/connection"
	"github.com/linkerlin/godis/redis/parser"
	"github.com/linkerlin/godis/redis/protocol"
)

// startTestReplicaPair starts a master and a replica server on ephemeral ports
// and wires the replica to the master. Returns cleanup.
func startTestReplicaPair(t *testing.T) (*Server, *Server, func()) {
	t.Helper()
	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:          1,
		AppendOnly:         false,
		Bind:               "127.0.0.1",
		Port:               0,
		ReplicaReadOnly:    true,
		RDBFilename:        filepath.Join(t.TempDir(), "dump.rdb"),
	}
	master, err := NewTestServer()
	if err != nil {
		t.Fatalf("master: %v", err)
	}
	replica, err := NewTestServer()
	if err != nil {
		t.Fatalf("replica: %v", err)
	}
	// The master needs a persister to generate the replication RDB (not created
	// when AppendOnly=false). bindPersister's addAof closure checks AppendOnly,
	// so wire the DBs directly to the persister for the backlog path.
	persister, err := NewPersister(master, filepath.Join(t.TempDir(), "master.aof"), false, aof.FsyncEverySec)
	if err != nil {
		t.Fatalf("persister: %v", err)
	}
	master.bindPersister(persister)
	for _, holder := range master.dbSet {
		db := holder.Load().(*DB)
		db.addAof = func(line CmdLine) { persister.SaveCmdLine(0, line) }
	}
	// Use real TCP listeners so replication handshake works.
	masterAddr, closeMasterLn := startTCPListener(t, master)
	replicaAddr, closeReplicaLn := startTCPListener(t, replica)
	_, replicaPortStr := splitAddr(replicaAddr)
	// Tell the replica to announce itself to the master with a real address
	// (FAILOVER demotion needs the replica's host/port).
	oldAnnIP := config.Properties.SlaveAnnounceIP
	oldAnnPort := config.Properties.SlaveAnnouncePort
	config.Properties.SlaveAnnounceIP = "127.0.0.1"
	config.Properties.SlaveAnnouncePort = mustAtoi(t, replicaPortStr)

	// Wire the replica to the master.
	rc := connection.NewFakeConn()
	host, portStr := splitAddr(masterAddr)
	if r := replica.Exec(rc, utils.ToCmdLine("REPLICAOF", host, portStr)); protocol.IsErrorReply(r) {
		t.Fatalf("replicaof: %s", r.ToBytes())
	}
	// Wait for the replica to come online on the master.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		master.masterStatus.mu.RLock()
		n := len(master.masterStatus.onlineSlaves)
		master.masterStatus.mu.RUnlock()
		if n >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The replica must have announced its address (FAILOVER demotion needs it).
	master.masterStatus.mu.RLock()
	n := len(master.masterStatus.onlineSlaves)
	var s *slaveClient
	for sl := range master.masterStatus.onlineSlaves {
		s = sl
	}
	master.masterStatus.mu.RUnlock()
	if n < 1 {
		replica.slaveStatus.mutex.Lock()
		host, port, off := replica.slaveStatus.masterHost, replica.slaveStatus.masterPort, replica.slaveStatus.replOffset
		replica.slaveStatus.mutex.Unlock()
		t.Fatalf("replica should come online on the master (replica cfg: host=%q port=%d off=%d role=%d)",
			host, port, off, atomic.LoadInt32(&replica.role))
	}
	if s == nil || s.announceIp == "" || s.announcePort == 0 {
		t.Fatalf("replica should announce its address for FAILOVER")
	}
	cleanup := func() {
		closeMasterLn()
		closeReplicaLn()
		master.Close()
		replica.Close()
		atomic.StoreInt32(&failoverState, failoverIdle)
		config.Properties.SlaveAnnounceIP = oldAnnIP
		config.Properties.SlaveAnnouncePort = oldAnnPort
		config.Properties = old
	}
	return master, replica, cleanup
}

// startTCPListener binds a real listener and serves commands for server
// (mirroring the std handler's parse/exec loop, enough for replication).
// closeFn stops Accept and closes all accepted conns so replica receiveAOF
// loops unblock; t.Cleanup also invokes it.
func startTCPListener(t *testing.T, server *Server) (addr string, closeFn func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var mu sync.Mutex
	conns := make(map[net.Conn]struct{})
	closeFn = sync.OnceFunc(func() {
		_ = ln.Close()
		mu.Lock()
		for c := range conns {
			_ = c.Close()
		}
		conns = nil
		mu.Unlock()
	})
	t.Cleanup(closeFn)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			if conns == nil {
				mu.Unlock()
				_ = conn.Close()
				return
			}
			conns[conn] = struct{}{}
			mu.Unlock()
			go func(c net.Conn) {
				defer func() {
					mu.Lock()
					delete(conns, c)
					mu.Unlock()
				}()
				serveConn(server, c)
			}(conn)
		}
	}()
	return ln.Addr().String(), closeFn
}

func serveConn(server *Server, conn net.Conn) {
	client := connection.NewConn(conn)
	RegisterClient(client)
	defer UnregisterClient(client)
	defer func() { _ = client.Close() }()
	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			return
		}
		if payload.Data == nil {
			continue
		}
		r, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			continue
		}
		result := server.Exec(client, r.Args)
		if result != nil {
			_, _ = client.Write(result.ToBytes())
		}
	}
}

func splitAddr(addr string) (string, string) {
	host, port, _ := net.SplitHostPort(addr)
	return host, port
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("atoi %s: %v", s, err)
	}
	return n
}

// TestFailoverPromotesReplica verifies FAILOVER drives a real role swap: the
// replica becomes master, the original master becomes its replica.
func TestFailoverPromotesReplica(t *testing.T) {
	master, replica, cleanup := startTestReplicaPair(t)
	defer cleanup()

	mc := connection.NewFakeConn()
	rc := connection.NewFakeConn()

	// Seed data on the master and let it replicate.
	for i := 0; i < 10; i++ {
		if r := master.Exec(mc, utils.ToCmdLine("SET", fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))); protocol.IsErrorReply(r) {
			t.Fatalf("set: %s", r.ToBytes())
		}
	}
	// Wait until the replica has actually APPLIED the data (master-side offsets
	// reflect "pushed", not "applied"). The replication cron runs every 10s, so
	// drive it manually here.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		master.masterCron()
		if b, ok := replica.Exec(rc, utils.ToCmdLine("GET", "k5")).(*protocol.BulkReply); ok && string(b.Arg) == "v5" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if b, ok := replica.Exec(rc, utils.ToCmdLine("GET", "k5")).(*protocol.BulkReply); !ok || string(b.Arg) != "v5" {
		t.Fatalf("replica should have applied replicated data before failover: %s", replica.Exec(rc, utils.ToCmdLine("GET", "k5")).ToBytes())
	}

	// Pre-sync ACK so TIMEOUT is not racing backlog lag (Windows flake).
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_ = master.masterSendUpdatesToSlave()
		master.nudgeSlavesForWait()
		master.masterStatus.mu.RLock()
		cur := master.masterStatus.backlog.currentOffset
		var off int64
		for sl := range master.masterStatus.onlineSlaves {
			off = sl.offset
			break
		}
		master.masterStatus.mu.RUnlock()
		if off >= cur {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// TIMEOUT bounds sync wait; 10s gives headroom after pre-sync on slow CI.
	r := master.Exec(mc, utils.ToCmdLine("FAILOVER", "TIMEOUT", "10000"))
	if protocol.IsErrorReply(r) {
		t.Fatalf("failover: %s", r.ToBytes())
	}

	// The replica should have promoted itself to master role.
	deadline = time.Now().Add(5 * time.Second)
	promoted := false
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&replica.role) == masterRole {
			promoted = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !promoted {
		t.Fatalf("replica should be promoted to master, role=%d", replica.role)
	}

	// The original master should have demoted to replica and reconnected to
	// the new master (replica's listener).
	deadline = time.Now().Add(5 * time.Second)
	demoted := false
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&master.role) == slaveRole && master.slaveStatus.masterHost != "" {
			demoted = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !demoted {
		t.Fatalf("original master should demote to replica")
	}

	// Data preserved on the promoted replica.
	if b, ok := replica.Exec(rc, utils.ToCmdLine("GET", "k5")).(*protocol.BulkReply); !ok || string(b.Arg) != "v5" {
		t.Fatalf("promoted replica should have replicated data: %s", replica.Exec(rc, utils.ToCmdLine("GET", "k5")).ToBytes())
	}

	// ROLE / INFO replication must reflect the swap.
	if r, ok := replica.Exec(rc, utils.ToCmdLine("ROLE")).(*protocol.MultiRawReply); !ok ||
		len(r.Replies) < 1 {
		t.Fatalf("promoted replica ROLE should be master: %s", replica.Exec(rc, utils.ToCmdLine("ROLE")).ToBytes())
	} else if roleStr, ok := r.Replies[0].(*protocol.BulkReply); !ok || string(roleStr.Arg) != "master" {
		t.Fatalf("promoted replica ROLE should be master: %s", replica.Exec(rc, utils.ToCmdLine("ROLE")).ToBytes())
	}
	if r, ok := master.Exec(mc, utils.ToCmdLine("ROLE")).(*protocol.MultiRawReply); !ok ||
		len(r.Replies) < 1 {
		t.Fatalf("demoted master ROLE should be slave: %s", master.Exec(mc, utils.ToCmdLine("ROLE")).ToBytes())
	} else if roleStr, ok := r.Replies[0].(*protocol.BulkReply); !ok || string(roleStr.Arg) != "slave" {
		t.Fatalf("demoted master ROLE should be slave: %s", master.Exec(mc, utils.ToCmdLine("ROLE")).ToBytes())
	}
	for _, tt := range []struct {
		server *Server
		conn   redis.Connection
		want   string
	}{
		{replica, rc, "role:master"},
		{master, mc, "role:slave"},
	} {
		if b, ok := tt.server.Exec(tt.conn, utils.ToCmdLine("INFO", "replication")).(*protocol.BulkReply); !ok ||
			!strings.Contains(string(b.Arg), tt.want) {
			t.Fatalf("INFO replication should contain %q: %s", tt.want,
				tt.server.Exec(tt.conn, utils.ToCmdLine("INFO", "replication")).ToBytes())
		}
	}
}

// TestFailoverRequiresReplicas verifies FAILOVER without replicas errors.
func TestFailoverRequiresReplicas(t *testing.T) {
	old := config.Properties
	config.Properties = &config.ServerProperties{
		Databases:        1,
		AppendOnly:       false,
		ReplicaReadOnly:  true,
		RDBFilename:      filepath.Join(t.TempDir(), "dump.rdb"),
	}
	defer func() { config.Properties = old }()
	server, err := NewTestServer()
	if err != nil {
		t.Fatalf("server: %v", err)
	}
	defer server.Close()
	c := connection.NewFakeConn()
	if r := server.Exec(c, utils.ToCmdLine("FAILOVER")); !protocol.IsErrorReply(r) {
		t.Fatalf("FAILOVER without replicas should error: %s", r.ToBytes())
	}
	// ABORT without a pending failover errors (Redis semantics).
	if r := server.Exec(c, utils.ToCmdLine("FAILOVER", "ABORT")); !protocol.IsErrorReply(r) ||
		!strings.Contains(string(r.ToBytes()), "No failover in progress") {
		t.Fatalf("FAILOVER ABORT without progress should error: %s", r.ToBytes())
	}
	// With a failover in progress (simulated), ABORT succeeds and resets state.
	atomic.StoreInt32(&failoverState, failoverWaitingSync)
	t.Cleanup(func() { atomic.StoreInt32(&failoverState, failoverIdle) })
	if r := server.Exec(c, utils.ToCmdLine("FAILOVER", "ABORT")); protocol.IsErrorReply(r) {
		t.Fatalf("FAILOVER ABORT should succeed: %s", r.ToBytes())
	}
	if atomic.LoadInt32(&failoverState) != failoverIdle {
		t.Fatalf("FAILOVER ABORT should reset state to idle, got %d", failoverState)
	}
	// A second FAILOVER while one is in progress errors.
	atomic.StoreInt32(&failoverState, failoverWaitingSync)
	if r := server.Exec(c, utils.ToCmdLine("FAILOVER")); !protocol.IsErrorReply(r) ||
		!strings.Contains(string(r.ToBytes()), "already in progress") {
		t.Fatalf("FAILOVER while in progress should error: %s", r.ToBytes())
	}
}

// TestFailoverForceSwitchesLaggedReplica verifies FORCE skips the sync wait and
// still drives the role swap even when the replica is behind.
func TestFailoverForceSwitchesLaggedReplica(t *testing.T) {
	master, replica, cleanup := startTestReplicaPair(t)
	defer cleanup()

	mc := connection.NewFakeConn()

	// Seed data on the master; do NOT wait for the replica to apply it — FORCE
	// must proceed regardless of the lag.
	for i := 0; i < 50; i++ {
		if r := master.Exec(mc, utils.ToCmdLine("SET", fmt.Sprintf("f%d", i), "x")); protocol.IsErrorReply(r) {
			t.Fatalf("set: %s", r.ToBytes())
		}
	}

	if r := master.Exec(mc, utils.ToCmdLine("FAILOVER", "FORCE", "TIMEOUT", "3000")); protocol.IsErrorReply(r) {
		t.Fatalf("failover force: %s", r.ToBytes())
	}

	// Role swap completes: replica is master, original master demoted.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&replica.role) == masterRole && atomic.LoadInt32(&master.role) == slaveRole {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("FORCE failover should swap roles: master=%d replica=%d",
		atomic.LoadInt32(&master.role), atomic.LoadInt32(&replica.role))
}

// TestSlaveCloseAfterReplicationDoesNotHang guards the stopSlaveWithMutex
// deadlock: receiveAOF needs slaveStatus.mutex after reading a payload, while
// Close used to Wait on receiveAOF while holding that same mutex.
func TestSlaveCloseAfterReplicationDoesNotHang(t *testing.T) {
	master, replica, cleanup := startTestReplicaPair(t)
	defer cleanup()

	mc := connection.NewFakeConn()
	for i := 0; i < 20; i++ {
		if r := master.Exec(mc, utils.ToCmdLine("SET", fmt.Sprintf("h%d", i), "x")); protocol.IsErrorReply(r) {
			t.Fatalf("set: %s", r.ToBytes())
		}
	}
	// Keep the replication stream busy so Close races with a mid-payload Lock.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			master.masterCron()
			_ = master.Exec(mc, utils.ToCmdLine("SET", "busy", strconv.Itoa(i)))
			time.Sleep(2 * time.Millisecond)
		}
	}()

	deadline := time.After(3 * time.Second)
	closed := make(chan struct{})
	go func() {
		time.Sleep(20 * time.Millisecond)
		replica.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-deadline:
		t.Fatal("replica.Close() hung — likely stopSlaveWithMutex Wait holding mutex")
	}
	<-done
	// master.Close runs in cleanup; give it a bounded wait too via later join.
}
