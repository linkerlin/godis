package core

import (
	"fmt"
	"sort"
	"strings"

	"github.com/linkerlin/godis/cluster/raft"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/hashslot"
	"github.com/linkerlin/godis/redis/protocol"
)

// clusterView is a snapshot of topology for CLUSTER NODES/SLOTS/INFO/SHARDS.
type clusterView struct {
	selfID       string
	nodeSlots    map[string][]uint32 // sorted slots per master that owns them
	masterSlave  map[string][]string // master -> slaves
	slaveMaster  map[string]string   // slave -> master
	allNodeIDs   []string
}

func (cluster *Cluster) snapshotClusterView() clusterView {
	self := ""
	if cluster != nil {
		self = cluster.SelfID()
	}
	v := clusterView{
		selfID:      self,
		nodeSlots:   make(map[string][]uint32),
		masterSlave: make(map[string][]string),
		slaveMaster: make(map[string]string),
	}

	if cluster != nil && cluster.raftNode != nil && cluster.raftNode.FSM != nil {
		cluster.raftNode.FSM.WithReadLock(func(fsm *raft.FSM) {
			for node, slots := range fsm.Node2Slot {
				cp := append([]uint32(nil), slots...)
				v.nodeSlots[node] = cp
			}
			for master, ms := range fsm.MasterSlaves {
				if ms == nil {
					continue
				}
				slaves := append([]string(nil), ms.Slaves...)
				v.masterSlave[master] = slaves
				for _, s := range slaves {
					v.slaveMaster[s] = master
				}
				if _, ok := v.nodeSlots[master]; !ok {
					v.nodeSlots[master] = nil
				}
			}
			for slave, master := range fsm.SlaveMasters {
				v.slaveMaster[slave] = master
				if _, ok := v.masterSlave[master]; !ok {
					v.masterSlave[master] = nil
				}
			}
		})
	} else if self != "" {
		// No Raft FSM (unit tests / disabled): own all slots locally.
		slots := make([]uint32, hashslot.Count)
		for i := 0; i < hashslot.Count; i++ {
			slots[i] = uint32(i)
		}
		v.nodeSlots[self] = slots
	}

	seen := make(map[string]struct{})
	for id := range v.nodeSlots {
		seen[id] = struct{}{}
	}
	for id := range v.slaveMaster {
		seen[id] = struct{}{}
	}
	for id := range seen {
		v.allNodeIDs = append(v.allNodeIDs, id)
	}
	sort.Strings(v.allNodeIDs)
	return v
}

func (v clusterView) assignedSlotCount() int {
	n := 0
	for _, slots := range v.nodeSlots {
		n += len(slots)
	}
	return n
}

func (v clusterView) masterCount() int {
	n := 0
	for id, slots := range v.nodeSlots {
		if _, isSlave := v.slaveMaster[id]; isSlave {
			continue
		}
		if len(slots) > 0 || len(v.masterSlave[id]) > 0 {
			n++
		}
	}
	if n == 0 && v.selfID != "" {
		return 1
	}
	return n
}

// slotRangePairs merges sorted slots into inclusive [start,end] pairs.
func slotRangePairs(slots []uint32) [][2]uint32 {
	if len(slots) == 0 {
		return nil
	}
	var out [][2]uint32
	start, prev := slots[0], slots[0]
	for i := 1; i < len(slots); i++ {
		s := slots[i]
		if s == prev+1 {
			prev = s
			continue
		}
		out = append(out, [2]uint32{start, prev})
		start, prev = s, s
	}
	out = append(out, [2]uint32{start, prev})
	return out
}

func formatSlotRanges(slots []uint32) string {
	pairs := slotRangePairs(slots)
	parts := make([]string, 0, len(pairs))
	for _, p := range pairs {
		if p[0] == p[1] {
			parts = append(parts, fmt.Sprintf("%d", p[0]))
		} else {
			parts = append(parts, fmt.Sprintf("%d-%d", p[0], p[1]))
		}
	}
	return strings.Join(parts, " ")
}

func (v clusterView) formatNodeLine(id string) string {
	host, port := parseAddr(id)
	flags := []string{}
	if id == v.selfID {
		flags = append(flags, "myself")
	}
	masterField := "-"
	if master, isSlave := v.slaveMaster[id]; isSlave {
		flags = append(flags, "slave")
		masterField = master
	} else {
		flags = append(flags, "master")
	}
	slots := formatSlotRanges(v.nodeSlots[id])
	line := fmt.Sprintf("%s %s:%d %s %s 0 0 0 connected",
		id, host, port, strings.Join(flags, ","), masterField)
	if slots != "" {
		line += " " + slots
	}
	return line
}

func (v clusterView) nodesBulk() []byte {
	var b strings.Builder
	for _, id := range v.allNodeIDs {
		b.WriteString(v.formatNodeLine(id))
		b.WriteByte('\n')
	}
	return []byte(b.String())
}

func (v clusterView) slotsReply() redis.Reply {
	var entries []redis.Reply
	for _, id := range v.allNodeIDs {
		if _, isSlave := v.slaveMaster[id]; isSlave {
			continue
		}
		host, port := parseAddr(id)
		nodeInfo := []redis.Reply{
			protocol.MakeBulkReply([]byte(host)),
			protocol.MakeIntReply(port),
			protocol.MakeBulkReply([]byte(id)),
		}
		// Append replicas (Redis CLUSTER SLOTS node list: master then replicas).
		for _, slave := range v.masterSlave[id] {
			sh, sp := parseAddr(slave)
			nodeInfo = append(nodeInfo,
				protocol.MakeBulkReply([]byte(sh)),
				protocol.MakeIntReply(sp),
				protocol.MakeBulkReply([]byte(slave)),
			)
		}
		for _, rg := range slotRangePairs(v.nodeSlots[id]) {
			entry := []redis.Reply{
				protocol.MakeIntReply(int64(rg[0])),
				protocol.MakeIntReply(int64(rg[1])),
			}
			entry = append(entry, nodeInfo...)
			entries = append(entries, protocol.MakeMultiRawReply(entry))
		}
	}
	return protocol.MakeMultiRawReply(entries)
}

func (v clusterView) infoBulk(stats busStatsSnap) []byte {
	assigned := v.assignedSlotCount()
	known := len(v.allNodeIDs)
	if known == 0 {
		known = 1
	}
	size := v.masterCount()
	state := "ok"
	if assigned < hashslot.Count {
		// Partial coverage still reported ok for Godis seed bootstrap edge cases;
		// fail only when nothing assigned.
		if assigned == 0 {
			state = "fail"
		}
	}
	// cluster_bus_port stays 0: no Redis gossip listen port.
	// ping/pong/meet counters come from Godis peer RPC (heartbeat/MEET), not CLUSTERMSG frames.
	sent := stats.pingSent + stats.pongSent + stats.meetSent
	recv := stats.pingReceived + stats.pongReceived
	info := fmt.Sprintf(
		"cluster_state:%s\n"+
			"cluster_slots_assigned:%d\n"+
			"cluster_slots_ok:%d\n"+
			"cluster_slots_pfail:0\n"+
			"cluster_slots_fail:0\n"+
			"cluster_known_nodes:%d\n"+
			"cluster_size:%d\n"+
			"cluster_current_epoch:0\n"+
			"cluster_my_epoch:0\n"+
			"cluster_bus_port:0\n"+
			"cluster_stats_messages_sent:%d\n"+
			"cluster_stats_messages_received:%d\n"+
			"cluster_stats_messages_ping_sent:%d\n"+
			"cluster_stats_messages_ping_received:%d\n"+
			"cluster_stats_messages_pong_sent:%d\n"+
			"cluster_stats_messages_pong_received:%d\n"+
			"cluster_stats_messages_meet_sent:%d\n"+
			"cluster_stats_messages_meet_received:0\n"+
			"cluster_stats_messages_fail_sent:0\n"+
			"cluster_stats_messages_fail_received:0\n"+
			"cluster_stats_messages_publish_sent:0\n"+
			"cluster_stats_messages_publish_received:0\n"+
			"cluster_stats_messages_auth-req_sent:0\n"+
			"cluster_stats_messages_auth-req_received:0\n"+
			"cluster_stats_messages_auth-ack_sent:0\n"+
			"cluster_stats_messages_auth-ack_received:0\n"+
			"cluster_stats_messages_update_sent:0\n"+
			"cluster_stats_messages_update_received:0\n"+
			"cluster_stats_messages_mfstart_sent:0\n"+
			"cluster_stats_messages_mfstart_received:0\n"+
			"cluster_stats_messages_module_sent:0\n"+
			"cluster_stats_messages_module_received:0\n",
		state, assigned, assigned, known, size,
		sent, recv,
		stats.pingSent, stats.pingReceived,
		stats.pongSent, stats.pongReceived,
		stats.meetSent,
	)
	return []byte(info)
}

func (v clusterView) shardsReply() redis.Reply {
	var shards []redis.Reply
	for _, id := range v.allNodeIDs {
		if _, isSlave := v.slaveMaster[id]; isSlave {
			continue
		}
		pairs := slotRangePairs(v.nodeSlots[id])
		slotReplies := make([]redis.Reply, 0, len(pairs)*2)
		for _, p := range pairs {
			slotReplies = append(slotReplies,
				protocol.MakeIntReply(int64(p[0])),
				protocol.MakeIntReply(int64(p[1])),
			)
		}
		var nodes []redis.Reply
		nodes = append(nodes, shardNodeMap(id, "master"))
		for _, slave := range v.masterSlave[id] {
			nodes = append(nodes, shardNodeMap(slave, "replica"))
		}
		shard := protocol.MakeMapReply()
		shard.Put("slots", protocol.MakeMultiRawReply(slotReplies))
		shard.Put("nodes", protocol.MakeMultiRawReply(nodes))
		shards = append(shards, shard)
	}
	if len(shards) == 0 && v.selfID != "" {
		// Degenerate empty FSM
		return execClusterShardsFallback(v.selfID)
	}
	return protocol.MakeMultiRawReply(shards)
}

func shardNodeMap(id, role string) redis.Reply {
	host, port := parseAddr(id)
	node := protocol.MakeMapReply()
	node.Put("id", protocol.MakeBulkReply([]byte(id)))
	node.Put("endpoint", protocol.MakeBulkReply([]byte(host)))
	node.Put("ip", protocol.MakeBulkReply([]byte(host)))
	node.Put("port", protocol.MakeIntReply(port))
	node.Put("role", protocol.MakeBulkReply([]byte(role)))
	node.Put("replication-offset", protocol.MakeIntReply(0))
	node.Put("health", protocol.MakeBulkReply([]byte("online")))
	return node
}

func execClusterShardsFallback(selfID string) redis.Reply {
	node := shardNodeMap(selfID, "master")
	shard := protocol.MakeMapReply()
	shard.Put("slots", protocol.MakeMultiRawReply([]redis.Reply{
		protocol.MakeIntReply(0),
		protocol.MakeIntReply(int64(hashslot.Count - 1)),
	}))
	shard.Put("nodes", protocol.MakeMultiRawReply([]redis.Reply{node}))
	return protocol.MakeMultiRawReply([]redis.Reply{shard})
}
