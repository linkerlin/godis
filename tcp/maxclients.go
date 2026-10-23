package tcp

import (
	"net"
	"sync/atomic"

	"github.com/linkerlin/godis/config"
)

// MaxClientsErrReply is the Redis-compatible error returned when maxclients is reached.
var MaxClientsErrReply = []byte("-ERR max number of clients reached\r\n")

// maxClientsLimit mirrors config maxclients atomically: per-accept reads of
// the global Properties pointer raced test/config writers (nightly -race).
// 0 = unlimited. Seeded from config at listener startup and kept current by
// CONFIG SET maxclients (SetMaxClients).
var maxClientsLimit int64

// SetMaxClients updates the atomic limit (CONFIG SET maxclients; startup seed).
func SetMaxClients(n int) {
	atomic.StoreInt64(&maxClientsLimit, int64(n))
}

// GetMaxClients returns the current atomic limit (tests restore what they set).
func GetMaxClients() int {
	return int(atomic.LoadInt64(&maxClientsLimit))
}

// SeedMaxClientsFromConfig snapshots the configured limit once at startup
// (std engines seed via ListenAndServe; gnet seeds in OnBoot).
func SeedMaxClientsFromConfig() {
	if config.Properties != nil {
		atomic.StoreInt64(&maxClientsLimit, int64(config.Properties.MaxClients))
	}
}

// TryAcceptClient increments the client counter when under maxclients limit.
func TryAcceptClient() bool {
	if limit := atomic.LoadInt64(&maxClientsLimit); limit > 0 {
		if int64(atomic.LoadInt32(&ClientCounter)) >= limit {
			atomic.AddUint64(&RejectedConnections, 1)
			return false
		}
	}
	atomic.AddInt32(&ClientCounter, 1)
	return true
}

// ReleaseClient decrements the active client counter.
func ReleaseClient() {
	atomic.AddInt32(&ClientCounter, -1)
}

// RejectConnectionMaxClients writes the standard error, counts rejection, and closes conn.
func RejectConnectionMaxClients(conn net.Conn) {
	atomic.AddUint64(&RejectedConnections, 1)
	_, _ = conn.Write(MaxClientsErrReply)
	_ = conn.Close()
}
