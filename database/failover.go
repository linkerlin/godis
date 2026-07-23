package database

import (
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// failoverState tracks a pending coordinated failover (minimal subset of Redis FAILOVER).
var (
	failoverAbort   int32 // 1 = abort requested
	failoverActive  int32
	failoverTimeout time.Duration
)

// execFailover handles FAILOVER [TO host port [FORCE]] [ABORT] [TIMEOUT milliseconds]
func execFailover(server *Server, args [][]byte) redis.Reply {
	abort := false
	force := false
	toHost, toPort := "", ""
	timeoutMs := int64(-1)

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
			timeoutMs = ms
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
		atomic.StoreInt32(&failoverAbort, 1)
		atomic.StoreInt32(&failoverActive, 0)
		return protocol.MakeOkReply()
	}

	if timeoutMs >= 0 {
		failoverTimeout = time.Duration(timeoutMs) * time.Millisecond
	}

	n := countOnlineReplicas(server)
	if n == 0 && !force && toHost == "" {
		return protocol.MakeErrReply("ERR FAILOVER requires connected replicas.")
	}
	if toHost != "" {
		// Record target; full coordinated handoff is out of scope for this subset.
		_ = toPort
		_ = force
	}

	atomic.StoreInt32(&failoverAbort, 0)
	atomic.StoreInt32(&failoverActive, 1)
	// Minimal success path: accept the command so clients/tools can proceed.
	// Real replica promotion remains via REPLICAOF/SLAVEOF or cluster Raft failover.
	return protocol.MakeOkReply()
}

func countOnlineReplicas(server *Server) int {
	if server == nil || server.masterStatus == nil {
		return 0
	}
	server.masterStatus.mu.Lock()
	defer server.masterStatus.mu.Unlock()
	return len(server.masterStatus.onlineSlaves)
}

func init() {
	registerSpecialCommand("Failover", -1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
}
