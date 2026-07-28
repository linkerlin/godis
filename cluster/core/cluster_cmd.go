package core

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
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
	case "MYID":
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		return protocol.MakeBulkReply([]byte(cluster.SelfID()))
	case "COUNTKEYSINSLOT":
		if len(cmdLine) != 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|countkeysinslot' command")
		}
		return execClusterCountKeysInSlot(cluster, string(cmdLine[2]))
	case "SHARDS":
		return execClusterShards(cluster)
	case "COUNT-FAILURE-REPORTS":
		if len(cmdLine) != 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|count-failure-reports' command")
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		return protocol.MakeIntReply(0)
	case "BUMPEPOCH":
		if len(cmdLine) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|bumpepoch' command")
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		return protocol.MakeStatusReply("BUMPED 0")
	case "REPLICAS", "SLAVES":
		if len(cmdLine) != 3 {
			return protocol.MakeErrReply(fmt.Sprintf("ERR wrong number of arguments for 'cluster|%s' command", strings.ToLower(subCmd)))
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		return protocol.MakeEmptyMultiBulkReply()
	case "LINKS":
		if len(cmdLine) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|links' command")
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		return protocol.MakeEmptyMultiBulkReply()
	case "SET-CONFIG-EPOCH":
		if len(cmdLine) != 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|set-config-epoch' command")
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		epoch, err := strconv.ParseInt(string(cmdLine[2]), 10, 64)
		if err != nil || epoch < 0 {
			return protocol.MakeErrReply("ERR Invalid config epoch")
		}
		return protocol.MakeOkReply()
	case "GETKEYSINSLOT":
		if len(cmdLine) != 4 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|getkeysinslot' command")
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		slot, err := strconv.ParseInt(string(cmdLine[2]), 10, 64)
		if err != nil || slot < 0 || slot > 16383 {
			return protocol.MakeErrReply("ERR Invalid slot")
		}
		count, err := strconv.ParseInt(string(cmdLine[3]), 10, 64)
		if err != nil || count < 0 {
			return protocol.MakeErrReply("ERR Invalid count")
		}
		_ = count
		return protocol.MakeEmptyMultiBulkReply()
	case "FORGET":
		if len(cmdLine) != 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|forget' command")
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		return protocol.MakeOkReply()
	case "RESET":
		if len(cmdLine) > 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|reset' command")
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		if len(cmdLine) == 3 {
			mode := strings.ToUpper(string(cmdLine[2]))
			if mode != "HARD" && mode != "SOFT" {
				return protocol.MakeErrReply("ERR Invalid RESET mode. Try HARD or SOFT")
			}
		}
		return protocol.MakeOkReply()
	case "SAVECONFIG":
		if len(cmdLine) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|saveconfig' command")
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		return protocol.MakeOkReply()
	case "FLUSHSLOTS":
		if len(cmdLine) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|flushslots' command")
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		return protocol.MakeOkReply()
	case "MYSHARDID":
		if len(cmdLine) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|myshardid' command")
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		return protocol.MakeBulkReply([]byte(cluster.SelfID()))
	case "KEYSLOT":
		if len(cmdLine) < 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|keyslot' command")
		}
		return execClusterKeyslot(cluster, string(cmdLine[2]))
	case "HELP":
		return execClusterHelp()
	default:
		return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for '%s'. Try CLUSTER HELP.", subCmd))
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

// execClusterCountKeysInSlot returns key count hosted in a slot on this node.
func execClusterCountKeysInSlot(cluster *Cluster, slotStr string) redis.Reply {
	if cluster == nil {
		return protocol.MakeErrReply("ERR This instance has cluster support disabled")
	}
	slot, err := strconv.ParseInt(slotStr, 10, 64)
	if err != nil || slot < 0 || slot > 16383 {
		return protocol.MakeErrReply("ERR slot out of range or invalid")
	}
	if cluster.slotsManager == nil {
		return protocol.MakeIntReply(0)
	}
	st := cluster.slotsManager.getSlot(uint32(slot))
	if st == nil || st.keys == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(st.keys.Len()))
}

// execClusterShards returns a shallow Redis-compatible SHARDS view (single shard).
func execClusterShards(cluster *Cluster) redis.Reply {
	if cluster == nil {
		return protocol.MakeErrReply("ERR This instance has cluster support disabled")
	}
	selfID := cluster.SelfID()
	shard := protocol.MakeMultiRawReply([]redis.Reply{
		protocol.MakeBulkReply([]byte("slots")),
		protocol.MakeMultiRawReply([]redis.Reply{
			protocol.MakeIntReply(0),
			protocol.MakeIntReply(16383),
		}),
		protocol.MakeBulkReply([]byte("nodes")),
		protocol.MakeMultiRawReply([]redis.Reply{
			protocol.MakeMultiRawReply([]redis.Reply{
				protocol.MakeBulkReply([]byte("id")),
				protocol.MakeBulkReply([]byte(selfID)),
				protocol.MakeBulkReply([]byte("endpoint")),
				protocol.MakeBulkReply([]byte("127.0.0.1")),
				protocol.MakeBulkReply([]byte("ip")),
				protocol.MakeBulkReply([]byte("127.0.0.1")),
				protocol.MakeBulkReply([]byte("port")),
				protocol.MakeIntReply(6379),
				protocol.MakeBulkReply([]byte("role")),
				protocol.MakeBulkReply([]byte("master")),
				protocol.MakeBulkReply([]byte("replication-offset")),
				protocol.MakeIntReply(0),
				protocol.MakeBulkReply([]byte("health")),
				protocol.MakeBulkReply([]byte("online")),
			}),
		}),
	})
	return protocol.MakeMultiRawReply([]redis.Reply{shard})
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
		"CLUSTER MYID",
		"    Return the node id of this node.",
		"CLUSTER COUNTKEYSINSLOT slot",
		"    Return the number of local keys in the specified hash slot.",
		"CLUSTER SHARDS",
		"    Return details about slots mappings and shard nodes.",
		"CLUSTER COUNT-FAILURE-REPORTS node-id",
		"    Return the number of failure reports for the specified node.",
		"CLUSTER BUMPEPOCH",
		"    Advance the cluster config epoch.",
		"CLUSTER REPLICAS node-id",
		"    List replica nodes of the specified master node.",
		"CLUSTER SLAVES node-id",
		"    Legacy alias for CLUSTER REPLICAS.",
		"CLUSTER LINKS",
		"    Return a list of cluster peer links.",
		"CLUSTER SET-CONFIG-EPOCH epoch",
		"    Set the config epoch for this node.",
		"CLUSTER GETKEYSINSLOT slot count",
		"    Return local keys in the specified hash slot.",
		"CLUSTER FORGET node-id",
		"    Remove a node from the nodes table.",
		"CLUSTER RESET [HARD|SOFT]",
		"    Reset a Redis Cluster node.",
		"CLUSTER SAVECONFIG",
		"    Force save the nodes.conf file.",
		"CLUSTER FLUSHSLOTS",
		"    Delete the node's own slots information.",
		"CLUSTER MYSHARDID",
		"    Return the shard id of this node.",
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
