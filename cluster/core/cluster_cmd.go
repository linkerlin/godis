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
		return protocol.MakeErrReply("ERR CLUSTER SET-CONFIG-EPOCH is not supported")
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
	case "ADDSLOTSRANGE":
		return execClusterAddSlotsRange(cluster, c, cmdLine)
	case "ADDSLOTS":
		return execClusterAddSlots(cluster, c, cmdLine)
	case "DELSLOTS":
		return execClusterDelSlots(cluster, c, cmdLine)
	case "MEET":
		return execClusterMeet(cluster, c, cmdLine)
	case "SETSLOT":
		return execClusterSetSlot(cluster, c, cmdLine)
	case "FORGET":
		if len(cmdLine) != 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|forget' command")
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		return protocol.MakeErrReply("ERR CLUSTER FORGET is not supported")
	case "SETNAME":
		if len(cmdLine) != 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|setname' command")
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		cluster.humanName = string(cmdLine[2])
		return protocol.MakeOkReply()
	case "GETNAME":
		if len(cmdLine) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|getname' command")
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		if cluster.humanName == "" {
			return protocol.MakeNullBulkReply()
		}
		return protocol.MakeBulkReply([]byte(cluster.humanName))
	case "REPLICATE":
		if len(cmdLine) != 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|replicate' command")
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		return protocol.MakeErrReply("ERR CLUSTER REPLICATE is not supported")
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
		return protocol.MakeErrReply("ERR CLUSTER RESET is not supported")
	case "FAILOVER":
		if len(cmdLine) > 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|failover' command")
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		if len(cmdLine) == 3 {
			opt := strings.ToUpper(string(cmdLine[2]))
			if opt != "FORCE" && opt != "TAKEOVER" {
				return protocol.MakeErrReply("ERR FAILOVER bad option. Use FORCE or TAKEOVER")
			}
		}
		return protocol.MakeErrReply("ERR CLUSTER FAILOVER is not supported (use standalone FAILOVER)")
	case "SAVECONFIG":
		if len(cmdLine) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'cluster|saveconfig' command")
		}
		if cluster == nil {
			return protocol.MakeErrReply("ERR This instance has cluster support disabled")
		}
		return protocol.MakeErrReply("ERR CLUSTER SAVECONFIG is not supported")
	case "DELSLOTSRANGE":
		return execClusterDelSlotsRange(cluster, c, cmdLine)
	case "FLUSHSLOTS":
		return execClusterFlushSlots(cluster, c, cmdLine)
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

// execClusterNodes 返回集群节点信息（读 Raft FSM；无 FSM 时本节点占满槽）
func execClusterNodes(cluster *Cluster) redis.Reply {
	if cluster == nil {
		return protocol.MakeErrReply("ERR This instance has cluster support disabled")
	}
	return protocol.MakeBulkReply(cluster.snapshotClusterView().nodesBulk())
}

// execClusterInfo 返回集群状态信息
func execClusterInfo(cluster *Cluster) redis.Reply {
	if cluster == nil {
		return protocol.MakeErrReply("ERR This instance has cluster support disabled")
	}
	return protocol.MakeBulkReply(cluster.snapshotClusterView().infoBulk())
}

// execClusterSlots 返回槽位到节点的映射
func execClusterSlots(cluster *Cluster) redis.Reply {
	if cluster == nil {
		return protocol.MakeErrReply("ERR This instance has cluster support disabled")
	}
	return cluster.snapshotClusterView().slotsReply()
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

// execClusterShards returns a Redis-compatible SHARDS view from FSM topology.
// Outer array of shard Maps; each node is a Map (RESP3 % / RESP2 flat via Map.ToBytes).
func execClusterShards(cluster *Cluster) redis.Reply {
	if cluster == nil {
		return protocol.MakeErrReply("ERR This instance has cluster support disabled")
	}
	return cluster.snapshotClusterView().shardsReply()
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
		"    Not supported.",
		"CLUSTER GETKEYSINSLOT slot count",
		"    Return local keys in the specified hash slot.",
		"CLUSTER ADDSLOTS slot [slot ...]",
		"    Assign hash slots to this node (writes Raft FSM).",
		"CLUSTER ADDSLOTSRANGE start-slot end-slot [start-slot end-slot ...]",
		"    Assign slots ranges to this node (writes Raft FSM).",
		"CLUSTER DELSLOTS slot [slot ...]",
		"    Remove hash slots from this node (writes Raft FSM).",
		"CLUSTER MEET ip port [raft-port]",
		"    Join peer into cluster via Raft/FSM (not gossip). Raft-ready nodes require raft-port.",
		"CLUSTER SETSLOT slot MIGRATING|IMPORTING|STABLE|NODE ...",
		"    MIGRATING/IMPORTING/STABLE update local slotsManager (ASK/ASKING).",
		"    NODE assigns slot ownership in the Raft FSM and clears local migrate state.",
		"CLUSTER FORGET node-id",
		"    Not supported.",
		"CLUSTER REPLICATE node-id",
		"    Not supported.",
		"CLUSTER RESET [HARD|SOFT]",
		"    Not supported.",
		"CLUSTER FAILOVER [FORCE|TAKEOVER]",
		"    Not supported (use standalone FAILOVER).",
		"CLUSTER SAVECONFIG",
		"    Not supported.",
		"CLUSTER DELSLOTSRANGE start-slot end-slot [start-slot end-slot ...]",
		"    Remove slots ranges from this node (writes Raft FSM).",
		"CLUSTER FLUSHSLOTS",
		"    Delete this node's slots from the FSM.",
		"CLUSTER MYSHARDID",
		"    Return the shard id of this node.",
		"CLUSTER SETNAME name",
		"    Set a human readable node name.",
		"CLUSTER GETNAME",
		"    Return the human readable node name.",
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
