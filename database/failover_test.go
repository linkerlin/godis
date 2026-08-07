package database

import (
	"fmt"
	"net"
	"strconv"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/linkerlin/godis/aof"
	"github.com/linkerlin/godis/config"
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
	masterAddr := startTCPListener(t, master)
	replicaAddr := startTCPListener(t, replica)
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
	for s := range master.masterStatus.onlineSlaves {
		if s.announceIp == "" || s.announcePort == 0 {
			master.masterStatus.mu.RUnlock()
			t.Fatalf("replica should announce its address for FAILOVER")
		}
	}
	master.masterStatus.mu.RUnlock()
	cleanup := func() {
		master.Close()
		replica.Close()
		config.Properties.SlaveAnnounceIP = oldAnnIP
		config.Properties.SlaveAnnouncePort = oldAnnPort
		config.Properties = old
	}
	return master, replica, cleanup
}

// startTCPListener binds a real listener and serves commands for server
// (mirroring the std handler's parse/exec loop, enough for replication).
func startTCPListener(t *testing.T, server *Server) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveConn(server, conn)
		}
	}()
	return ln.Addr().String()
}

func serveConn(server *Server, conn net.Conn) {
	client := connection.NewConn(conn)
	RegisterClient(client)
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

	// Run FAILOVER on the master.
	r := master.Exec(mc, utils.ToCmdLine("FAILOVER"))
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
	// ABORT without a pending failover is fine.
	if r := server.Exec(c, utils.ToCmdLine("FAILOVER", "ABORT")); protocol.IsErrorReply(r) {
		t.Fatalf("FAILOVER ABORT should succeed: %s", r.ToBytes())
	}
}
