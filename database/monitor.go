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

// execMonitor is handled in DB.Exec (needs connection); kept for COMMAND table.
func execMonitor(_ *DB, args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'monitor' command")
	}
	return protocol.MakeOkReply()
}

// BroadcastMonitor broadcasts a command to all monitor clients.
func BroadcastMonitor(cmd string, args [][]byte, client redis.Connection) {
	monitorManager.mu.RLock()
	defer monitorManager.mu.RUnlock()

	if len(monitorManager.clients) == 0 {
		return
	}

	timestamp := float64(time.Now().UnixMicro()) / 1_000_000
	dbIdx := 0
	addr := "127.0.0.1:0"
	if client != nil {
		dbIdx = client.GetDBIndex()
		if a := client.RemoteAddr(); a != "" {
			addr = a
		}
	}

	cmdLine := fmt.Sprintf("\"%s\"", cmd)
	for _, arg := range args {
		cmdLine += fmt.Sprintf(" \"%s\"", string(arg))
	}
	msg := fmt.Sprintf("%.6f [%d %s] %s\r\n", timestamp, dbIdx, addr, cmdLine)

	for conn := range monitorManager.clients {
		if client != nil && conn == client {
			continue // do not echo to the originator
		}
		_, _ = conn.Write([]byte(msg))
	}
}

// AddMonitorClient adds a client to monitor list
func AddMonitorClient(conn redis.Connection) {
	if conn == nil {
		return
	}
	monitorManager.mu.Lock()
	defer monitorManager.mu.Unlock()
	monitorManager.clients[conn] = true
}

// RemoveMonitorClient removes a client from monitor list
func RemoveMonitorClient(conn redis.Connection) {
	if conn == nil {
		return
	}
	monitorManager.mu.Lock()
	defer monitorManager.mu.Unlock()
	delete(monitorManager.clients, conn)
}

// IsMonitorClient reports whether conn is in MONITOR mode (CLIENT LIST flag O).
func IsMonitorClient(conn redis.Connection) bool {
	if conn == nil {
		return false
	}
	monitorManager.mu.RLock()
	defer monitorManager.mu.RUnlock()
	return monitorManager.clients[conn]
}

func init() {
	registerCommand("Monitor", execMonitor, noPrepare, nil, 1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript}, 0, 0, 0)
}
