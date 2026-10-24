package core

import (
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/linkerlin/godis/cluster/raft"
	"github.com/linkerlin/godis/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

// CmdFunc represents the handler of a redis command
type CmdFunc func(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply

// commands is written by RegisterCmd — normally only from package init, but
// tests register at runtime while other tests' clusters Exec concurrently
// (the nightly -race gate caught exactly that pair), so guard it.
var (
	commandsMu sync.RWMutex
	commands   = make(map[string]CmdFunc)
)

// RegisterCmd add command handler into cluster
func RegisterCmd(name string, cmd CmdFunc) {
	name = strings.ToLower(name)
	commandsMu.Lock()
	commands[name] = cmd
	commandsMu.Unlock()
}

func lookupCmd(name string) (CmdFunc, bool) {
	commandsMu.RLock()
	cmd, ok := commands[name]
	commandsMu.RUnlock()
	return cmd, ok
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
	// HELLO is pre-gate like AUTH — it carries the AUTH credentials for
	// RESP3 clients (go-redis with Username/Password sends HELLO n AUTH …);
	// behind the gate it answered NOAUTH and the connection never came up.
	if cmdName == "hello" {
		roleName := "master"
		if server, ok := cluster.db.(*database.Server); ok {
			roleName = server.RoleName()
		}
		return database.HelloWithRole(c, cmdLine[1:], roleName)
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
	// ping — post-gate like Redis (unauthenticated PING gets NOAUTH when
	// requirepass is set)
	if cmdName == "ping" {
		return database.Ping(c, cmdLine[1:])
	}
	// Enforce ACL (users, keys, channels) in cluster mode too. Previously only
	// requirepass was checked, so ACL users were silently bypassed on clusters.
	if reply := database.CheckACLPermission(c, cmdName, cmdLine[1:]); reply != nil {
		return reply
	}
	// Classic PUBLISH broadcasts cluster-wide in Redis: subscribers live on
	// every node, so execute locally and relay to each peer master (the
	// relay command runs PUBLISH locally WITHOUT re-broadcasting, or the
	// peers would ping-pong forever). SPUBLISH is the slot-routed sharded
	// variant and keeps its DefaultFunc routing.
	if cmdName == "publish" && len(cmdLine) == 3 {
		reply := cluster.db.Exec(c, cmdLine)
		cluster.relayPublishToPeers(cmdLine)
		cluster.slogLogger.Record(GodisExecCommandStartUnixTime, cmdLine, c.RemoteAddr(), c.GetClientName())
		return reply
	}
	cmdFunc, ok := lookupCmd(cmdName)
	if !ok {
		keys, keylessLocal, exists := database.ResolveClusterCommandKeys(cmdLine)
		if !exists {
			return protocol.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
		}
		if keylessLocal || len(keys) == 0 {
			exec := cluster.db.Exec(c, cmdLine)
			cluster.slogLogger.Record(GodisExecCommandStartUnixTime, cmdLine, c.RemoteAddr(), c.GetClientName())
			return exec
		}
		slotId := cluster.GetSlot(keys[0])
		for _, k := range keys[1:] {
			if cluster.GetSlot(k) != slotId {
				return protocol.MakeErrReply("CROSSSLOT Keys in request don't hash to the same slot")
			}
		}
		exec := routeByKeyAndSlot(cluster, c, cmdLine, keys[0], slotId)
		cluster.slogLogger.Record(GodisExecCommandStartUnixTime, cmdLine, c.RemoteAddr(), c.GetClientName())
		return exec
	}
	exec := cmdFunc(cluster, c, cmdLine)
	cluster.slogLogger.Record(GodisExecCommandStartUnixTime, cmdLine, c.RemoteAddr(), c.GetClientName())
	return exec

}

// isAuthenticated delegates to the shared database gate. The old local copy
// only compared GetPassword() against requirepass, so non-default ACL users
// authenticated with their own credentials were still rejected with NOAUTH
// (requirepass is a shortcut for the DEFAULT user's password in Redis).
func isAuthenticated(c redis.Connection) bool {
	return database.IsConnectionAuthenticated(c)
}

func RegisterDefaultCmd(name string) {
	RegisterCmd(name, DefaultFunc)
}

// pubsubRelayCommand carries a PUBLISH to a peer master. The peer runs the
// PUBLISH locally WITHOUT re-broadcasting (that would ping-pong forever).
const pubsubRelayCommand = "cluster.pubsub.relay"

func init() {
	RegisterCmd(pubsubRelayCommand, execPubsubRelay)
}

func execPubsubRelay(cluster *Cluster, _ redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) != 3 {
		return protocol.MakeArgNumErrReply(pubsubRelayCommand)
	}
	return cluster.db.Exec(nil, utils.ToCmdLine("publish", string(cmdLine[1]), string(cmdLine[2])))
}

// relayPublishToPeers fans a just-executed PUBLISH out to every other master
// (best-effort: a down peer simply misses the message, like a partitioned
// Redis node). Replica-local subscribers are not covered — masters only.
func (cluster *Cluster) relayPublishToPeers(publishCmd CmdLine) {
	if cluster.raftNode == nil || cluster.raftNode.FSM == nil {
		return
	}
	self := cluster.SelfID()
	var peers []string
	cluster.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
		for _, ms := range fsm.MasterSlaves {
			if ms != nil && ms.MasterId != self {
				peers = append(peers, ms.MasterId)
			}
		}
	})
	if len(peers) == 0 {
		return
	}
	relay := utils.ToCmdLine(pubsubRelayCommand, string(publishCmd[1]), string(publishCmd[2]))
	for _, peer := range peers { // sends stay outside the FSM read lock
		peerConn, err := cluster.connections.BorrowPeerClient(peer)
		if err != nil {
			logger.Warn(fmt.Sprintf("pubsub relay to %s failed: %v", peer, err))
			continue
		}
		_ = peerConn.Send(relay)
		cluster.connections.ReturnPeerClient(peerConn)
	}
}

// DefaultFunc routes a single-key (or same-slot multi-key) command or returns
// MOVED/ASK/CROSSSLOT for Redis Cluster clients.
// Internal multi-key helpers still call Relay explicitly.
func DefaultFunc(cluster *Cluster, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments")
	}
	if keys, keylessLocal, exists := database.ResolveClusterCommandKeys(args); exists && !keylessLocal && len(keys) > 0 {
		slotId := cluster.GetSlot(keys[0])
		for _, k := range keys[1:] {
			if cluster.GetSlot(k) != slotId {
				return protocol.MakeErrReply("CROSSSLOT Keys in request don't hash to the same slot")
			}
		}
		return routeByKeyAndSlot(cluster, c, args, keys[0], slotId)
	}
	key := string(args[1])
	slotId := cluster.GetSlot(key)
	return routeByKeyAndSlot(cluster, c, args, key, slotId)
}

func routeByKeyAndSlot(cluster *Cluster, c redis.Connection, args [][]byte, key string, slotId uint32) redis.Reply {
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

	// READONLY replica serving: a client that sent READONLY may read slots
	// owned by this node's master from this replica (Redis cluster replica
	// reads). Writes still land in the paths below and are rejected by the
	// replica read-only gate in the server layer.
	if cc := asClusterConn(c); cc != nil && cc.IsClusterReadOnly() &&
		cluster.raftNode != nil && cluster.raftNode.FSM != nil {
		if master := cluster.raftNode.FSM.GetMaster(cluster.SelfID()); master != "" && master == peer {
			return cluster.db.Exec(c, args)
		}
	}

	if peer == "" || peer == self {
		// Mid-migration without local key → ASK importer.
		// Uses FSM Migratings first, then local SETSLOT migratePeer (see migrationTargetForSlot).
		// Do not require local exporting: finish may clear local state before FSM finishes.
		if !cluster.keyExistsLocal(key) {
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
