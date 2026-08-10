package database

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// Coordinated FAILOVER (Redis 8 compatible subset):
//
//  1. Validate: run on the master, choose a target replica (TO host port, or
//     the most up-to-date online replica).
//  2. Wait for the target's REPLCONF ACK offset to reach the master's
//     backlog offset (skipped with FORCE), bounded by TIMEOUT.
//  3. Inject a REPLCONF FAILOVER <token> directive into the replication
//     backlog; the target replica's receiveAOF loop recognizes it and promotes
//     itself (slaveOfNone), closing its master connection.
//  4. Wait for the target to disappear from the master's slave map (its
//     connection closed on promotion), confirming the handoff.
//  5. Demote the master: REPLICAOF <target host> <target port> so it syncs
//     from the new master.
//
// ABORT cancels a pending failover; TIMEOUT bounds the sync wait.

const (
	failoverIdle = int32(iota)
	failoverWaitingSync
	failoverPromoting
)

var failoverState int32 = failoverIdle

// execFailover handles FAILOVER [TO host port [FORCE]] [ABORT] [TIMEOUT milliseconds]
func execFailover(server *Server, args [][]byte) redis.Reply {
	abort := false
	force := false
	toHost, toPort := "", ""
	timeout := 60 * time.Second

	for i := 0; i < len(args); {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "ABORT":
			abort = true
			i++
		case "FORCE":
			force = true
			i++
		case "TIMEOUT":
			if i+1 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			ms, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ms < 0 {
				return protocol.MakeErrReply("ERR TIMEOUT must be a non-negative integer")
			}
			timeout = time.Duration(ms) * time.Millisecond
			i += 2
		case "TO":
			if i+2 >= len(args) {
				return protocol.MakeSyntaxErrReply()
			}
			toHost = string(args[i+1])
			toPort = string(args[i+2])
			i += 3
			if i < len(args) && strings.EqualFold(string(args[i]), "FORCE") {
				force = true
				i++
			}
		default:
			return protocol.MakeSyntaxErrReply()
		}
	}

	if abort {
		// Redis semantics: ABORT with no failover in progress errors.
		if atomic.LoadInt32(&failoverState) == failoverIdle {
			return protocol.MakeErrReply("ERR No failover in progress.")
		}
		atomic.StoreInt32(&failoverState, failoverIdle)
		return protocol.MakeOkReply()
	}

	if atomic.CompareAndSwapInt32(&failoverState, failoverIdle, failoverWaitingSync) == false {
		return protocol.MakeErrReply("ERR A failover is already in progress.")
	}
	defer atomic.StoreInt32(&failoverState, failoverIdle)

	if atomic.LoadInt32(&server.role) != masterRole {
		return protocol.MakeErrReply("ERR FAILOVER can only be executed by the master")
	}

	target := pickFailoverTarget(server, toHost, toPort)
	if target == nil {
		return protocol.MakeErrReply("ERR FAILOVER requires connected replicas.")
	}

	// Wait for the target replica to catch up to the master's write offset.
	// Aborting (ABORT) resets failoverState to idle and breaks the wait.
	if !force {
		if !waitSlaveInSync(server, target, timeout) {
			return protocol.MakeErrReply("ERR FAILOVER target not in sync")
		}
	}

	atomic.StoreInt32(&failoverState, failoverPromoting)

	// Inject the promotion directive into the replication stream. The target
	// replica's receiveAOF loop executes it and promotes itself.
	token := failoverToken()
	server.masterStatus.mu.Lock()
	server.masterStatus.backlog.appendBytes(
		protocol.MakeMultiBulkReply(utils.ToCmdLine("REPLCONF", "FAILOVER", token)).ToBytes(),
	)
	server.masterStatus.mu.Unlock()
	// Push immediately (and again after a beat) so the directive reaches the
	// replica promptly; a closed replica connection triggers removeSlave.
	_ = server.masterSendUpdatesToSlave()

	// Wait for the target to drop off the slave map (its master connection is
	// closed on promotion). Promotion is asynchronous; keep pushing so a closed
	// replica connection triggers removeSlave via the write-failure path.
	// TIMEOUT only bounds the sync wait above; promote confirmation uses a
	// fixed cap so FORCE with a large backlog is not starved by a short TIMEOUT.
	if !waitSlaveGone(server, target, 10*time.Second) {
		return protocol.MakeErrReply("ERR FAILOVER target did not promote")
	}

	// Demote the (former) master so it syncs from the new master. The target
	// host/port come from REPLCONF listening-port/ip-address when known,
	// falling back to the TO arguments.
	host, port := target.announceIp, strconv.Itoa(target.announcePort)
	if host == "" || port == "0" {
		host, port = toHost, toPort
	}
	if host == "" {
		return protocol.MakeErrReply("ERR FAILOVER target address unknown, use TO host port")
	}
	server.execSlaveOf(nil, [][]byte{[]byte(host), []byte(port)})

	logger.Info("FAILOVER complete: demoted to replica of " + host + ":" + port)
	return protocol.MakeOkReply()
}

// pickFailoverTarget selects the replica addressed by TO, or the one with the
// largest replicated offset.
func pickFailoverTarget(server *Server, host, port string) *slaveClient {
	server.masterStatus.mu.RLock()
	defer server.masterStatus.mu.RUnlock()
	var best *slaveClient
	for slave := range server.masterStatus.onlineSlaves {
		if host != "" {
			if slave.announceIp == host && strconv.Itoa(slave.announcePort) == port {
				return slave
			}
			continue
		}
		if best == nil || slave.offset > best.offset {
			best = slave
		}
	}
	return best
}

// waitSlaveInSync polls until the replica's acked offset reaches the master's
// current backlog offset, or timeout elapses. Returns false early if the
// failover is aborted (failoverState reset to idle) or timed out.
func waitSlaveInSync(server *Server, target *slaveClient, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&failoverState) != failoverWaitingSync {
			return false // aborted
		}
		// Push pending backlog and poke GETACK so REPLCONF ACK can advance
		// target.offset (send watermark alone is not sync).
		_ = server.masterSendUpdatesToSlave()
		server.nudgeSlavesForWait()
		server.masterStatus.mu.RLock()
		current := server.masterStatus.backlog.currentOffset
		offset := target.offset
		server.masterStatus.mu.RUnlock()
		if offset >= current {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

// waitSlaveGone polls until the target replica leaves the slave map (its
// master-side connection closed upon promotion). Each poll pushes the backlog
// (so a closed connection surfaces via the write-failure path) and, when the
// backlog is empty, writes an explicit ping byte — an empty write to a closed
// connection never errors, so the replica's disconnect would go unnoticed.
func waitSlaveGone(server *Server, target *slaveClient, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&failoverState) != failoverPromoting {
			return false // aborted
		}
		_ = server.masterSendUpdatesToSlave()
		if _, err := target.conn.Write(pingBytes); err != nil {
			server.removeSlave(target)
			return true
		}
		server.masterStatus.mu.RLock()
		_, exists := server.masterStatus.slaveMap[target.conn]
		server.masterStatus.mu.RUnlock()
		if !exists {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// failoverToken returns a random 128-bit hex token for the promotion directive.
func failoverToken() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "failover"
	}
	return hex.EncodeToString(buf)
}

func init() {
	registerSpecialCommand("Failover", -1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
}
