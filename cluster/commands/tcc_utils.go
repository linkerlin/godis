package commands

import (
	"sort"
	"strconv"
	"sync/atomic"

	"github.com/linkerlin/godis/cluster/core"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/redis/protocol"
)

var tccTxSeq uint64

type CmdLine = [][]byte

// node -> keys on the node
type RouteMap map[string][]string

func getRouteMap(cluster *core.Cluster, keys []string) RouteMap {
	m := make(RouteMap)
	for _, key := range keys {
		slot := cluster.GetSlot(key)
		node := cluster.PickNode(slot)
		m[node] = append(m[node], key)
	}
	return m
}

type TccTx struct {
	rawCmdLine CmdLine
	routeMap   RouteMap
	cmdLines   map[string]CmdLine // node -> CmdLine
}

// execute tcc
// returns node->result map
func doTcc(cluster *core.Cluster, c redis.Connection, tx *TccTx) (map[string]redis.Reply, protocol.ErrorReply) {
	seq := atomic.AddUint64(&tccTxSeq, 1)
	txId := strconv.FormatUint(seq, 36) + "-" + utils.RandString(6)

	// Sort participant nodes deterministically to prevent AB-BA distributed deadlocks.
	prepNodes := make([]string, 0, len(tx.cmdLines))
	for node := range tx.cmdLines {
		prepNodes = append(prepNodes, node)
	}
	sort.Strings(prepNodes)

	// send prepare request
	for _, node := range prepNodes {
		cmdLine := tx.cmdLines[node]
		prepareCmd := utils.ToCmdLine("prepare", txId)
		prepareCmd = append(prepareCmd, cmdLine...)
		reply := cluster.Relay(node, c, prepareCmd)
		if err := protocol.Try2ErrorReply(reply); err != nil {
			requestRollback(cluster, c, txId, tx.routeMap)
			return nil, protocol.MakeErrReply("prepare failed: " + err.Error())
		}
	}

	commitNodes := make([]string, 0, len(tx.routeMap))
	for node := range tx.routeMap {
		commitNodes = append(commitNodes, node)
	}
	sort.Strings(commitNodes)

	// send commit request
	commiteCmd := utils.ToCmdLine("commit", txId)
	result := make(map[string]redis.Reply)
	for _, node := range commitNodes {
		reply := cluster.Relay(node, c, commiteCmd)
		if err := protocol.Try2ErrorReply(reply); err != nil {
			requestRollback(cluster, c, txId, tx.routeMap)
			return nil, protocol.MakeErrReply("commit failed: " + err.Error())
		}
		result[node] = reply
	}

	return result, nil
}

func sortedRouteNodes(routeMap RouteMap) []string {
	nodes := make([]string, 0, len(routeMap))
	for node := range routeMap {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	return nodes
}

func requestRollback(cluster *core.Cluster, c redis.Connection, txId string, routeMap RouteMap) {
	nodes := sortedRouteNodes(routeMap)
	rollbackCmd := utils.ToCmdLine("rollback", txId)
	for _, node := range nodes {
		cluster.Relay(node, c, rollbackCmd)
	}
}
