package core

import (
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/redis/protocol"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// CmdFunc represents the handler of a redis command
type CmdFunc func(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply

var commands = make(map[string]CmdFunc)

// RegisterCmd add command handler into cluster
func RegisterCmd(name string, cmd CmdFunc) {
	name = strings.ToLower(name)
	commands[name] = cmd
}

// Exec executes command on cluster
func (cluster *Cluster) Exec(c redis.Connection, cmdLine [][]byte) (result redis.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &protocol.UnknownErrReply{}
		}
	}()
	// Record the start time of command execution
	GodisExecCommandStartUnixTime := time.Now()

	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "auth" {
		return database.Auth(c, cmdLine[1:])
	}
	if cmdName == "ping" {
		return database.Ping(c, cmdLine[1:])
	}
	if cmdName == "dbsize" {
		dbsize, _, _ := cluster.db.GetDBSize(0)
		return protocol.MakeIntReply(int64(dbsize))
	}

	if cmdName == "info" {
		if server, ok := cluster.db.(*database.Server); ok {
			return database.Info(server, cmdLine[1:])
		}
	}

	if cmdName == "slowlog" {
		return cluster.slogLogger.HandleSlowlogCommand(cmdLine)
	}

	if !isAuthenticated(c) {
		return protocol.MakeErrReply("NOAUTH Authentication required")
	}
	// Enforce ACL (users, keys, channels) in cluster mode too. Previously only
	// requirepass was checked, so ACL users were silently bypassed on clusters.
	if reply := database.CheckACLPermission(c, cmdName, cmdLine[1:]); reply != nil {
		return reply
	}
	cmdFunc, ok := commands[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}
	exec := cmdFunc(cluster, c, cmdLine)
	cluster.slogLogger.Record(GodisExecCommandStartUnixTime, cmdLine, c.RemoteAddr(), c.GetClientName())
	return exec

}

func isAuthenticated(c redis.Connection) bool {
	if config.Properties.RequirePass == "" {
		return true
	}
	return c.GetPassword() == config.Properties.RequirePass
}

func RegisterDefaultCmd(name string) {
	RegisterCmd(name, DefaultFunc)
}

// DefaultFunc routes a single-key command or returns MOVED/ASK for Redis Cluster clients.
// Internal multi-key helpers still call Relay explicitly.
func DefaultFunc(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments")
	}
	key := string(args[1])
	slotId := cluster.GetSlot(key)
	peer := cluster.PickNode(slotId)
	self := cluster.SelfID()

	asking := false
	if cc := asClusterConn(c); cc != nil {
		asking = cc.ConsumeAsking()
	}

	// ASKING: allow serving on importing node even if topology still points at exporter.
	if asking {
		st := cluster.slotsManager.getSlot(slotId)
		st.mu.RLock()
		importing := st.state == slotStateImporting
		st.mu.RUnlock()
		if importing || peer == self || peer == "" {
			return cluster.db.Exec(c, args)
		}
	}

	if peer == "" || peer == self {
		// Exporter mid-migration without local key → ASK importer.
		st := cluster.slotsManager.getSlot(slotId)
		st.mu.RLock()
		exporting := st.state == slotStateExporting
		st.mu.RUnlock()
		if exporting && !cluster.keyExistsLocal(key) {
			if target := cluster.migrationTargetForSlot(slotId); target != "" && target != self {
				return protocol.MakeAskErrReply(slotId, target)
			}
		}
		return cluster.db.Exec(c, args)
	}

	// In-process test clusters act as a smart proxy (Relay). Production returns MOVED.
	if cluster.inmemProxy {
		return cluster.Relay(peer, c, args)
	}
	return protocol.MakeMovedErrReply(slotId, peer)
}
