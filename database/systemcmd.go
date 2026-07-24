package database

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/stats"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/scripting"
	"github.com/linkerlin/godis/tcp"
)

// Server stats for INFO command
type ServerStats struct {
	TotalCommandsProcessed   uint64
	TotalConnectionsReceived uint64
	ExpiredKeys              uint64
	EvictedKeys              uint64
	KeyspaceHits             uint64
	KeyspaceMisses           uint64
	ExpiredStale             int64 // Number of expired keys that were accessed but already expired
}

var serverStats = &ServerStats{}

// resetServerStats clears INFO stats counters (CONFIG RESETSTAT).
func resetServerStats() {
	atomic.StoreUint64(&serverStats.TotalCommandsProcessed, 0)
	atomic.StoreUint64(&serverStats.TotalConnectionsReceived, 0)
	atomic.StoreUint64(&serverStats.ExpiredKeys, 0)
	atomic.StoreUint64(&serverStats.EvictedKeys, 0)
	atomic.StoreUint64(&serverStats.KeyspaceHits, 0)
	atomic.StoreUint64(&serverStats.KeyspaceMisses, 0)
	serverStats.ExpiredStale = 0
	ResetCommandStats()
	stats.Reset()
	atomic.StoreUint64(&tcp.RejectedConnections, 0)
}

// Ping the server
func Ping(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return &protocol.PongReply{}
	} else if len(args) == 1 {
		return protocol.MakeStatusReply(string(args[0]))
	} else {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

// Info the information of the godis server returned by the INFO command
func Info(db *Server, args [][]byte) redis.Reply {
	if len(args) == 0 {
		infoCommandList := [...]string{"server", "client", "memory", "persistence", "stats", "replication", "cpu", "commandstats", "cluster", "keyspace"}
		var allSection []byte
		for _, s := range infoCommandList {
			allSection = append(allSection, GenGodisInfoString(s, db)...)
		}
		return protocol.MakeBulkReply(allSection)
	} else if len(args) == 1 {
		section := strings.ToLower(string(args[0]))
		switch section {
		case "server":
			reply := GenGodisInfoString("server", db)
			return protocol.MakeBulkReply(reply)
		case "client":
			return protocol.MakeBulkReply(GenGodisInfoString("client", db))
		case "memory":
			return protocol.MakeBulkReply(GenGodisInfoString("memory", db))
		case "persistence":
			return protocol.MakeBulkReply(GenGodisInfoString("persistence", db))
		case "stats":
			return protocol.MakeBulkReply(GenGodisInfoString("stats", db))
		case "replication":
			return protocol.MakeBulkReply(GenGodisInfoString("replication", db))
		case "cpu":
			return protocol.MakeBulkReply(GenGodisInfoString("cpu", db))
		case "commandstats":
			return protocol.MakeBulkReply(GenGodisInfoString("commandstats", db))
		case "cluster":
			return protocol.MakeBulkReply(GenGodisInfoString("cluster", db))
		case "keyspace":
			return protocol.MakeBulkReply(GenGodisInfoString("keyspace", db))
		default:
			return protocol.MakeErrReply("ERR Invalid section for 'info' command")
		}
	}
	return protocol.MakeArgNumErrReply("info")
}

// Auth validate client's password — implemented in auth.go

func DbSize(c redis.Connection, db *Server) redis.Reply {
	keys, _, _ := db.GetDBSize(c.GetDBIndex())
	return protocol.MakeIntReply(int64(keys))
}

func GenGodisInfoString(section string, db *Server) []byte {
	startUpTimeFromNow := getGodisRuninngTime()
	switch section {
	case "server":
		s := fmt.Sprintf("# Server\r\n"+
			"redis_version:%s\r\n"+
			"godis_version:%s\r\n"+
			"godis_mode:%s\r\n"+
			"os:%s %s\r\n"+
			"arch_bits:%d\r\n"+
			"go_version:%s\r\n"+
			"process_id:%d\r\n"+
			"run_id:%s\r\n"+
			"tcp_port:%d\r\n"+
			"server_time_usec:%d\r\n"+
			"uptime_in_seconds:%d\r\n"+
			"uptime_in_days:%d\r\n"+
			"hz:%d\r\n"+
			"lru_clock:%d\r\n"+
			"config_file:%s\r\n",
			godisVersion,
			godisVersion,
			getGodisRunningMode(),
			runtime.GOOS, runtime.GOARCH,
			32<<(^uint(0)>>63),
			runtime.Version(),
			os.Getpid(),
			config.Properties.RunID,
			config.Properties.Port,
			time.Now().UnixMicro(),
			int64(startUpTimeFromNow.Seconds()),
			int64(startUpTimeFromNow.Hours()/24),
			10, // hz - default event loop frequency
			getLRUClock(),
			config.GetConfigFilePath())
		return []byte(s)
	case "client":
		blockedClients := getBlockedClientsCount()
		s := fmt.Sprintf("# Clients\r\n"+
			"connected_clients:%d\r\n"+
			"cluster_connections:%d\r\n"+
			"maxclients:%d\r\n"+
			"blocked_clients:%d\r\n"+
			"tracking_clients:%d\r\n"+
			"tracking_total_keys:%d\r\n"+
			"clients_in_timeout_table:%d\r\n",
			atomic.LoadInt32(&tcp.ClientCounter),
			0, // cluster_connections
			config.Properties.MaxClients,
			blockedClients,
			GetTrackingClientsCount(),
			GetTotalTrackedKeys(),
			0, // clients_in_timeout_table
		)
		return []byte(s)
	case "memory":
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		s := fmt.Sprintf("# Memory\r\n"+
			"used_memory:%d\r\n"+
			"used_memory_human:%s\r\n"+
			"used_memory_rss:%d\r\n"+
			"used_memory_peak:%d\r\n"+
			"used_memory_peak_human:%s\r\n"+
			"used_memory_lua:%d\r\n"+
			"mem_fragmentation_ratio:%.2f\r\n"+
			"mem_allocator:%s\r\n",
			m.TotalAlloc,
			humanReadableSize(m.TotalAlloc),
			m.Sys,
			m.TotalAlloc, // Simplified peak
			humanReadableSize(m.TotalAlloc),
			scripting.GetGlobalLuaMemory(),
			float64(m.Sys)/float64(m.TotalAlloc),
			"go",
		)
		return []byte(s)
	case "stats":
		s := fmt.Sprintf("# Stats\r\n"+
			"total_connections_received:%d\r\n"+
			"total_commands_processed:%d\r\n"+
			"instantaneous_ops_per_sec:%d\r\n"+
			"total_net_input_bytes:%d\r\n"+
			"total_net_output_bytes:%d\r\n"+
			"instantaneous_input_kbps:%.2f\r\n"+
			"instantaneous_output_kbps:%.2f\r\n"+
			"rejected_connections:%d\r\n"+
			"sync_full:%d\r\n"+
			"sync_partial_ok:%d\r\n"+
			"sync_partial_err:%d\r\n"+
			"expired_keys:%d\r\n"+
			"expired_stale_perc:%.2f\r\n"+
			"expired_time_cap_reached_count:%d\r\n"+
			"evict_keys:%d\r\n"+
			"keyspace_hits:%d\r\n"+
			"keyspace_misses:%d\r\n"+
			"pubsub_channels:%d\r\n"+
			"pubsub_patterns:%d\r\n"+
			"latest_fork_usec:%d\r\n"+
			"migrate_cached_sockets:%d\r\n"+
			"slave_expires_tracked_keys:%d\r\n"+
			"active_defrag_hits:%d\r\n"+
			"active_defrag_misses:%d\r\n"+
			"active_defrag_key_hits:%d\r\n"+
			"active_defrag_key_misses:%d\r\n",
			serverStats.TotalConnectionsReceived,
			serverStats.TotalCommandsProcessed,
			getInstantaneousOpsPerSec(),
			getNetInputBytes(),
			getNetOutputBytes(),
			getInstantaneousInputKbps(),
			getInstantaneousOutputKbps(),
			tcp.GetRejectedConnections(),
			uint64(0), // sync_full - TODO
			uint64(0), // sync_partial_ok - TODO
			uint64(0), // sync_partial_err - TODO
			serverStats.ExpiredKeys,
			getExpiredStalePerc(),
			uint64(0), // expired_time_cap_reached_count - TODO
			serverStats.EvictedKeys,
			serverStats.KeyspaceHits,
			serverStats.KeyspaceMisses,
			getPubsubChannelsCount(),
			getPubsubPatternsCount(),
			0, // latest_fork_usec - N/A in Go
			0, // migrate_cached_sockets - TODO
			0, // slave_expires_tracked_keys - TODO
			0, // active_defrag_hits - N/A
			0, // active_defrag_misses - N/A
			0, // active_defrag_key_hits - N/A
			0, // active_defrag_key_misses - N/A
		)
		return []byte(s)
	case "cluster":
		if getGodisRunningMode() == config.ClusterMode {
			s := fmt.Sprintf("# Cluster\r\n"+
				"cluster_enabled:%s\r\n",
				"1",
			)
			return []byte(s)
		} else {
			s := fmt.Sprintf("# Cluster\r\n"+
				"cluster_enabled:%s\r\n",
				"0",
			)
			return []byte(s)
		}
	case "persistence":
		s := genPersistenceInfo(db)
		return []byte(s)
	case "replication":
		s := genReplicationInfo(db)
		return []byte(s)
	case "cpu":
		s := genCPUInfo()
		return []byte(s)
	case "commandstats":
		s := genCommandStatsInfo()
		return []byte(s)
	case "keyspace":
		dbCount := config.Properties.Databases
		var serv []byte
		for i := 0; i < dbCount; i++ {
			keys, expiresKeys, _ := db.GetDBSize(i)
			if keys != 0 {
				ttlSampleAverage, _ := db.GetAvgTTL(i, 20)
				serv = append(serv, getDbSize(i, keys, expiresKeys, ttlSampleAverage)...)
			}
		}
		prefix := []byte("# Keyspace\r\n")
		keyspaceInfo := append(prefix, serv...)
		return keyspaceInfo
	}
	return []byte("")
}

// genPersistenceInfo generates persistence section for INFO
func genPersistenceInfo(db *Server) string {
	aofEnabled := config.Properties.AppendOnly

	var aofSize int64 = 0
	if db.persister != nil {
		if stats := db.persister.Stats(); stats["enabled"].(bool) {
			if size, ok := stats["size"].(int64); ok {
				aofSize = size
			}
		}
	}

	rdbBgsaveInProgress := 0
	if db.masterStatus != nil {
		db.masterStatus.mu.RLock()
		if db.masterStatus.bgSaveState == bgSaveRunning {
			rdbBgsaveInProgress = 1
		}
		db.masterStatus.mu.RUnlock()
	}

	var rdbLastSaveTime int64
	if config.Properties.RDBFilename != "" {
		if info, err := os.Stat(config.GetTmpDir() + "/" + config.Properties.RDBFilename); err == nil {
			rdbLastSaveTime = info.ModTime().Unix()
		}
	}

	return fmt.Sprintf("# Persistence\r\n"+
		"loading:%d\r\n"+
		"rdb_changes_since_last_save:%d\r\n"+
		"rdb_bgsave_in_progress:%d\r\n"+
		"rdb_last_save_time:%d\r\n"+
		"rdb_last_bgsave_status:%s\r\n"+
		"rdb_last_bgsave_time_sec:%d\r\n"+
		"rdb_current_bgsave_time_sec:%d\r\n"+
		"aof_enabled:%d\r\n"+
		"aof_rewrite_in_progress:%d\r\n"+
		"aof_rewrite_scheduled:%d\r\n"+
		"aof_last_rewrite_time_sec:%d\r\n"+
		"aof_current_rewrite_time_sec:%d\r\n"+
		"aof_last_bgrewrite_status:%s\r\n"+
		"aof_last_write_status:%s\r\n"+
		"aof_current_size:%d\r\n"+
		"aof_base_size:%d\r\n",
		0,
		db.DirtyChanges(),
		rdbBgsaveInProgress,
		rdbLastSaveTime,
		"ok",
		-1,
		-1,
		boolToInt(aofEnabled),
		0,
		0,
		-1,
		-1,
		"ok",
		"ok",
		aofSize,
		aofSize,
	)
}

// genReplicationInfo generates replication section for INFO
func genReplicationInfo(db *Server) string {
	role := "master"
	if atomic.LoadInt32(&db.role) == slaveRole {
		role = "slave"
	}

	slaves := 0
	var replOffset int64
	var backlogSize, backlogFirstOffset, backlogHistLen int64
	replBacklogActive := 0

	if db.masterStatus != nil {
		db.masterStatus.mu.RLock()
		slaves = len(db.masterStatus.onlineSlaves)
		if db.masterStatus.backlog != nil {
			bl := db.masterStatus.backlog
			replOffset = bl.currentOffset
			backlogFirstOffset = bl.beginOffset
			backlogHistLen = bl.histLen()
			backlogSize = int64(bl.capacity())
			replBacklogActive = 1
		}
		db.masterStatus.mu.RUnlock()
	}

	return fmt.Sprintf("# Replication\r\n"+
		"role:%s\r\n"+
		"connected_slaves:%d\r\n"+
		"master_replid:%s\r\n"+
		"master_replid2:%s\r\n"+
		"master_repl_offset:%d\r\n"+
		"second_repl_offset:%d\r\n"+
		"repl_backlog_active:%d\r\n"+
		"repl_backlog_size:%d\r\n"+
		"repl_backlog_first_byte_offset:%d\r\n"+
		"repl_backlog_histlen:%d\r\n",
		role,
		slaves,
		config.Properties.RunID,
		"",
		replOffset,
		-1,
		replBacklogActive,
		backlogSize,
		backlogFirstOffset,
		backlogHistLen,
	)
}

// genCPUInfo generates CPU section for INFO
func genCPUInfo() string {
	userSec, sysSec := stats.GetProcessCPUTime()
	return fmt.Sprintf("# CPU\r\n"+
		"used_cpu_sys:%.2f\r\n"+
		"used_cpu_user:%.2f\r\n"+
		"used_cpu_sys_children:%.2f\r\n"+
		"used_cpu_user_children:%.2f\r\n",
		sysSec,
		userSec,
		0.0,
		0.0,
	)
}

// genCommandStatsInfo generates commandstats section for INFO
func genCommandStatsInfo() string {
	var sb strings.Builder
	sb.WriteString("# Commandstats\r\n")

	stats := GetAllCommandStats()
	for cmdName, stat := range stats {
		if stat.calls > 0 {
			sb.WriteString(fmt.Sprintf(
				"cmdstat_%s:calls=%d,usec=%d,usec_per_call=%.2f\r\n",
				cmdName,
				stat.calls,
				stat.usec,
				stat.usecPerCall,
			))
		}
	}

	return sb.String()
}

// boolToInt converts bool to int (1/0)
func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Helper functions for INFO command
func humanReadableSize(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func getLRUClock() uint32 {
	// Simplified LRU clock - 24 bits, increments every 10 seconds
	return uint32(time.Now().Unix() / 10)
}

func getBlockedClientsCount() int64 {
	return GetBlockedListClientsCount() + GetBlockedStreamClientsCount()
}

func getInstantaneousOpsPerSec() int64 {
	// TODO: implement ops tracking
	return 0
}

func getPubsubChannelsCount() int64 {
	// TODO: implement pubsub tracking
	return 0
}

func getPubsubPatternsCount() int64 {
	// TODO: implement pubsub tracking
	return 0
}

func getNetInputBytes() uint64 {
	input, _ := stats.GetStats()
	return input
}

func getNetOutputBytes() uint64 {
	_, output := stats.GetStats()
	return output
}

func getInstantaneousInputKbps() float64 {
	inputKBps, _ := stats.GetRates()
	return inputKBps
}

func getInstantaneousOutputKbps() float64 {
	_, outputKBps := stats.GetRates()
	return outputKBps
}

func getExpiredStalePerc() float64 {
	if serverStats.ExpiredKeys == 0 {
		return 0.0
	}
	return float64(serverStats.ExpiredStale) / float64(serverStats.ExpiredKeys) * 100
}

// getGodisRunningMode return godis running mode
func getGodisRunningMode() string {
	if config.Properties.ClusterEnable {
		return config.ClusterMode
	} else {
		return config.StandaloneMode
	}
}

// getGodisRuninngTime return the running time of godis
func getGodisRuninngTime() time.Duration {
	return time.Since(config.EachTimeServerInfo.StartUpTime) / time.Second
}

func getDbSize(dbIndex, keys, expiresKeys int, ttl int64) []byte {
	s := fmt.Sprintf("db%d:keys=%d,expires=%d,avg_ttl=%d\r\n",
		dbIndex, keys, expiresKeys, ttl)
	return []byte(s)
}
