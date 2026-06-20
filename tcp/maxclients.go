package tcp

import (
	"net"
	"sync/atomic"

	"github.com/linkerlin/godis/config"
)

// MaxClientsErrReply is the Redis-compatible error returned when maxclients is reached.
var MaxClientsErrReply = []byte("-ERR max number of clients reached\r\n")

// TryAcceptClient increments the client counter when under maxclients limit.
func TryAcceptClient() bool {
	if config.Properties != nil && config.Properties.MaxClients > 0 {
		if int(atomic.LoadInt32(&ClientCounter)) >= config.Properties.MaxClients {
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
