package database

import (
	"fmt"
	"sync"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// MonitorManager manages monitor clients
type MonitorManager struct {
	clients map[redis.Connection]bool
	mu      sync.RWMutex
}

var monitorManager = &MonitorManager{
	clients: make(map[redis.Connection]bool),
}

// execMonitor starts monitoring commands
// MONITOR
func execMonitor(db *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'monitor' command")
	}

	// Note: In real implementation, this would add the connection to monitor clients
	// and start streaming commands. Simplified: return OK.
	return protocol.MakeOkReply()
}

// BroadcastMonitor broadcasts a command to all monitor clients
func BroadcastMonitor(cmd string, args [][]byte, client redis.Connection) {
	monitorManager.mu.RLock()
	defer monitorManager.mu.RUnlock()

	if len(monitorManager.clients) == 0 {
		return
	}

	// Format: timestamp [db client_ip:port] "command" "arg1" "arg2" ...
	timestamp := float64(time.Now().UnixMicro()) / 1000000

	// Build command string
	cmdLine := fmt.Sprintf("\"%s\"", cmd)
	for _, arg := range args {
		cmdLine += fmt.Sprintf(" \"%s\"", string(arg))
	}

	msg := fmt.Sprintf("%.6f %s\r\n", timestamp, cmdLine)

	// Send to all monitor clients
	for conn := range monitorManager.clients {
		conn.Write([]byte(msg))
	}
}

// AddMonitorClient adds a client to monitor list
func AddMonitorClient(conn redis.Connection) {
	monitorManager.mu.Lock()
	defer monitorManager.mu.Unlock()
	monitorManager.clients[conn] = true
}

// RemoveMonitorClient removes a client from monitor list
func RemoveMonitorClient(conn redis.Connection) {
	monitorManager.mu.Lock()
	defer monitorManager.mu.Unlock()
	delete(monitorManager.clients, conn)
}

func init() {
	registerCommand("Monitor", execMonitor, noPrepare, nil, 1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
}
