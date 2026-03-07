package core

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// execCluster 处理 CLUSTER 用户命令
func execCluster(cluster *Cluster, c redis.Connection, cmdLine CmdLine) redis.Reply {
	if len(cmdLine) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster' command")
	}

	subCmd := strings.ToUpper(string(cmdLine[1]))

	switch subCmd {
	case "NODES":
		return execClusterNodes(cluster)
	case "INFO":
		return execClusterInfo(cluster)
	case "SLOTS":
		return execClusterSlots(cluster)
	case "KEYSLOT":
		if len(cmdLine) < 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|keyslot' command")
		}
		return execClusterKeyslot(cluster, string(cmdLine[2]))
	case "HELP":
		return execClusterHelp()
	default:
		return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown subcommand '%s'", subCmd))
	}
}

// execClusterNodes 返回集群节点信息
func execClusterNodes(cluster *Cluster) redis.Reply {
	selfID := cluster.SelfID()
	
	// 简化的节点信息
	flags := "master,myself"
	nodeLine := fmt.Sprintf("%s %s:%d %s - 0 0 0 connected 0-16383\n",
		selfID, "127.0.0.1", 6379, flags)
	
	return protocol.MakeBulkReply([]byte(nodeLine))
}

// execClusterInfo 返回集群状态信息
func execClusterInfo(cluster *Cluster) redis.Reply {
	info := "cluster_state:ok\n"
	info += "cluster_slots_assigned:16384\n"
	info += "cluster_slots_ok:16384\n"
	info += "cluster_slots_pfail:0\n"
	info += "cluster_slots_fail:0\n"
	info += "cluster_known_nodes:1\n"
	info += "cluster_size:1\n"
	info += "cluster_current_epoch:0\n"
	info += "cluster_my_epoch:0\n"
	info += "cluster_stats_messages_sent:0\n"
	info += "cluster_stats_messages_received:0\n"
	
	return protocol.MakeBulkReply([]byte(info))
}

// execClusterSlots 返回槽位到节点的映射
func execClusterSlots(cluster *Cluster) redis.Reply {
	result := make([]redis.Reply, 0)
	
	// 槽位范围 0-16383
	slotRange := []redis.Reply{
		protocol.MakeIntReply(0),
		protocol.MakeIntReply(16383),
	}
	
	// 节点信息
	selfID := cluster.SelfID()
	host := "127.0.0.1"
	port := int64(6379)
	
	nodeInfo := []redis.Reply{
		protocol.MakeBulkReply([]byte(host)),
		protocol.MakeIntReply(port),
		protocol.MakeBulkReply([]byte(selfID)),
	}
	
	slotRange = append(slotRange, nodeInfo...)
	result = append(result, protocol.MakeMultiRawReply(slotRange))
	
	return protocol.MakeMultiRawReply(result)
}

// execClusterKeyslot 计算键的槽位
func execClusterKeyslot(cluster *Cluster, key string) redis.Reply {
	slot := cluster.GetSlot(key)
	return protocol.MakeIntReply(int64(slot))
}

// execClusterHelp 获取帮助
func execClusterHelp() redis.Reply {
	help := []string{
		"CLUSTER NODES",
		"    Return cluster configuration view.",
		"CLUSTER INFO",
		"    Return information about the cluster state.",
		"CLUSTER SLOTS",
		"    Return information about slots range mappings.",
		"CLUSTER KEYSLOT key",
		"    Return the hash slot for the specified key.",
		"CLUSTER HELP",
		"    Print this help.",
	}

	result := make([]redis.Reply, len(help))
	for i, h := range help {
		result[i] = protocol.MakeBulkReply([]byte(h))
	}
	return protocol.MakeMultiRawReply(result)
}

// parseAddr 解析地址字符串
func parseAddr(addr string) (string, int64) {
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return addr, 6379
	}
	port, _ := strconv.ParseInt(parts[1], 10, 64)
	if port == 0 {
		port = 6379
	}
	return parts[0], port
}

func init() {
	RegisterCmd("cluster", execCluster)
}
