package database

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/stats"
	"github.com/linkerlin/godis/redis/protocol"
	"github.com/linkerlin/godis/scripting"
	"github.com/linkerlin/godis/tcp"
)

// memoryStartupBytes is a coarse baseline for used_memory_startup (captured once).
var memoryStartupBytes uint64

func init() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	memoryStartupBytes = m.Sys
}

// Server stats for INFO command
type ServerStats struct {
	TotalCommandsProcessed   uint64
	TotalReadsProcessed      uint64
	TotalWritesProcessed     uint64
	TotalConnectionsReceived uint64
	TotalErrorReplies        uint64
	ExpiredKeys              uint64
	EvictedKeys              uint64
	KeyspaceHits             uint64
	KeyspaceMisses           uint64
	ExpiredStale             int64 // Number of expired keys that were accessed but already expired
}

var serverStats = &ServerStats{}

// errorReplyCounts tracks INFO errorstats by error prefix (ERR, WRONGTYPE, …).
var (
	errorReplyMu     sync.Mutex
	errorReplyCounts = map[string]uint64{}
)

func recordErrorReply(reply redis.Reply) {
	if reply == nil || !protocol.IsErrorReply(reply) {
		return
	}
	b := reply.ToBytes()
	if len(b) < 2 || b[0] != '-' {
		return
	}
	s := string(b[1:])
	if i := strings.IndexByte(s, '\r'); i >= 0 {
		s = s[:i]
	}
	code := s
	if i := strings.IndexByte(s, ' '); i > 0 {
		code = s[:i]
	}
	if code == "" {
		code = "ERR"
	}
	errorReplyMu.Lock()
	errorReplyCounts[code]++
	errorReplyMu.Unlock()
}

func genErrorStatsInfo() string {
	errorReplyMu.Lock()
	defer errorReplyMu.Unlock()
	var b strings.Builder
	b.WriteString("# Errorstats\r\n")
	if len(errorReplyCounts) == 0 {
		return b.String()
	}
	keys := make([]string, 0, len(errorReplyCounts))
	for k := range errorReplyCounts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "errorstat_%s:count=%d\r\n", k, errorReplyCounts[k])
	}
	return b.String()
}

// Replication sync counters for INFO stats (Redis-compatible field names).
var (
	syncFullCount       uint64
	syncPartialOKCount  uint64
	syncPartialErrCount uint64
)

func noteSyncFull() {
	atomic.AddUint64(&syncFullCount, 1)
}

func noteSyncPartialOK() {
	atomic.AddUint64(&syncPartialOKCount, 1)
}

func noteSyncPartialErr() {
	atomic.AddUint64(&syncPartialErrCount, 1)
}

// Sliding 1-second window for INFO instantaneous_ops_per_sec.
var (
	opsWindowSec    int64
	opsWindowCount  int64
	opsLastSecCount int64
)

func noteOps() {
	now := time.Now().Unix()
	for {
		sec := atomic.LoadInt64(&opsWindowSec)
		if sec == now {
			atomic.AddInt64(&opsWindowCount, 1)
			return
		}
		if atomic.CompareAndSwapInt64(&opsWindowSec, sec, now) {
			prev := atomic.SwapInt64(&opsWindowCount, 1)
			if sec != 0 {
				atomic.StoreInt64(&opsLastSecCount, prev)
			}
			return
		}
	}
}

func resetOpsWindow() {
	atomic.StoreInt64(&opsWindowSec, 0)
	atomic.StoreInt64(&opsWindowCount, 0)
	atomic.StoreInt64(&opsLastSecCount, 0)
}

// resetServerStats clears INFO stats counters (CONFIG RESETSTAT).
func resetServerStats() {
	atomic.StoreUint64(&serverStats.TotalCommandsProcessed, 0)
	atomic.StoreUint64(&serverStats.TotalReadsProcessed, 0)
	atomic.StoreUint64(&serverStats.TotalWritesProcessed, 0)
	atomic.StoreUint64(&serverStats.TotalConnectionsReceived, 0)
	atomic.StoreUint64(&serverStats.TotalErrorReplies, 0)
	atomic.StoreUint64(&serverStats.ExpiredKeys, 0)
	atomic.StoreUint64(&serverStats.EvictedKeys, 0)
	atomic.StoreUint64(&serverStats.KeyspaceHits, 0)
	atomic.StoreUint64(&serverStats.KeyspaceMisses, 0)
	serverStats.ExpiredStale = 0
	errorReplyMu.Lock()
	errorReplyCounts = map[string]uint64{}
	errorReplyMu.Unlock()
	ResetCommandStats()
	resetOpsWindow()
	stats.Reset()
	atomic.StoreUint64(&tcp.RejectedConnections, 0)
	atomic.StoreUint64(&syncFullCount, 0)
	atomic.StoreUint64(&syncPartialOKCount, 0)
	atomic.StoreUint64(&syncPartialErrCount, 0)
}

// Ping the server
func Ping(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) == 0 {
		return &protocol.PongReply{}
	} else if len(args) == 1 {
		return protocol.MakeBulkReply(args[0])
	} else {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ping' command")
	}
}

// Info the information of the godis server returned by the INFO command
func Info(db *Server, args [][]byte) redis.Reply {
	defaultSections := [...]string{"server", "client", "memory", "persistence", "stats", "replication", "cpu", "commandstats", "errorstats", "cluster", "keyspace"}
	allSections := [...]string{"server", "client", "memory", "persistence", "stats", "replication", "cpu", "commandstats", "errorstats", "cluster", "modules", "latency", "keyspace"}
	if len(args) == 0 {
		var allSection []byte
		for _, s := range defaultSections {
			allSection = append(allSection, GenGodisInfoString(s, db)...)
		}
		return protocol.MakeBulkReply(allSection)
	} else if len(args) == 1 {
		section := strings.ToLower(string(args[0]))
		if section == "everything" || section == "all" {
			var buf []byte
			for _, s := range allSections {
				buf = append(buf, GenGodisInfoString(s, db)...)
			}
			return protocol.MakeBulkReply(buf)
		}
		switch section {
		case "server":
			reply := GenGodisInfoString("server", db)
			return protocol.MakeBulkReply(reply)
		case "client", "clients":
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
		case "errorstats":
			return protocol.MakeBulkReply(GenGodisInfoString("errorstats", db))
		case "cluster":
			return protocol.MakeBulkReply(GenGodisInfoString("cluster", db))
		case "modules":
			return protocol.MakeBulkReply(GenGodisInfoString("modules", db))
		case "latency":
			return protocol.MakeBulkReply(GenGodisInfoString("latency", db))
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
			"redis_git_sha1:%s\r\n"+
			"redis_git_dirty:%d\r\n"+
			"redis_build_id:%s\r\n"+
			"redis_mode:%s\r\n"+
			"godis_mode:%s\r\n"+
			"os:%s %s\r\n"+
			"arch_bits:%d\r\n"+
			"atomicvar_api:%s\r\n"+
			"gcc_version:%s\r\n"+
			"go_version:%s\r\n"+
			"process_id:%d\r\n"+
			"process_supervised:%s\r\n"+
			"run_id:%s\r\n"+
			"tcp_port:%d\r\n"+
			"server_time_usec:%d\r\n"+
			"uptime_in_seconds:%d\r\n"+
			"uptime_in_days:%d\r\n"+
			"hz:%d\r\n"+
			"configured_hz:%d\r\n"+
			"lru_clock:%d\r\n"+
			"executable:%s\r\n"+
			"multiplexing_api:%s\r\n"+
			"monotonic_clock:%s\r\n"+
			"config_file:%s\r\n",
			godisVersion,
			godisVersion,
			"00000000",
			0,
			"0000000000000000",
			getRedisMode(),
			getGodisRunningMode(),
			runtime.GOOS, runtime.GOARCH,
			32<<(^uint(0)>>63),
			"go-atomic",
			"0",
			runtime.Version(),
			os.Getpid(),
			"no",
			config.Properties.RunID,
			config.Properties.Port,
			time.Now().UnixMicro(),
			int64(startUpTimeFromNow.Seconds()),
			int64(startUpTimeFromNow.Hours()/24),
			getServerHz(),
			getServerHz(),
			getLRUClock(),
			os.Args[0],
			"go",
			"go-time",
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
			"tracking_total_items:%d\r\n"+
			"tracking_total_prefixes:%d\r\n"+
			"pubsub_clients:%d\r\n"+
			"watching_clients:%d\r\n"+
			"clients_in_timeout_table:%d\r\n"+
			"unblocked_clients:%d\r\n"+
			"total_watched_keys:%d\r\n"+
			"io_threads_active:%d\r\n",
			atomic.LoadInt32(&tcp.ClientCounter),
			0, // cluster_connections
			config.Properties.MaxClients,
			blockedClients,
			GetTrackingClientsCount(),
			GetTotalTrackedKeys(),
			GetTotalTrackedItems(),
			GetTotalTrackedPrefixes(),
			countPubsubClients(),
			countWatchingClients(),
			blockedClients, // clients_in_timeout_table ≈ blocked waiters
			0,             // unblocked_clients
			countTotalWatchedKeys(),
			0, // io_threads_active (single-threaded Go net)
		)
		return []byte(s)
	case "memory":
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		maxMem := int64(0)
		maxMemPolicy := "noeviction"
		if config.Properties != nil {
			maxMem = config.Properties.Maxmemory
			if config.Properties.MaxmemoryPolicy != "" {
				maxMemPolicy = config.Properties.MaxmemoryPolicy
			}
		}
		maxMemU := uint64(0)
		if maxMem > 0 {
			maxMemU = uint64(maxMem)
		}
		peak := m.TotalAlloc
		if m.Sys > peak {
			peak = m.Sys
		}
		peakPerc := 0.0
		if peak > 0 {
			peakPerc = float64(m.Alloc) * 100.0 / float64(peak)
		}
		fragBytes := int64(m.Sys) - int64(m.Alloc)
		if fragBytes < 0 {
			fragBytes = 0
		}
		keyCount := int64(0)
		if db != nil {
			for _, holder := range db.dbSet {
				dbi := holder.Load().(*DB)
				keyCount += int64(dbi.data.Len())
			}
		}
		dataset := uint64(keyCount * bytesPerKeyEstimate)
		overhead := uint64(0)
		if m.Alloc > dataset {
			overhead = m.Alloc - dataset
		}
		datasetPerc := 0.0
		if m.Alloc > 0 {
			datasetPerc = float64(dataset) * 100.0 / float64(m.Alloc)
			if datasetPerc > 100 {
				datasetPerc = 100
			}
		}
		totalSys := getTotalSystemMemoryBytes()
		s := fmt.Sprintf("# Memory\r\n"+
			"used_memory:%d\r\n"+
			"used_memory_human:%s\r\n"+
			"used_memory_rss:%d\r\n"+
			"used_memory_rss_human:%s\r\n"+
			"used_memory_peak:%d\r\n"+
			"used_memory_peak_human:%s\r\n"+
			"used_memory_peak_perc:%.2f\r\n"+
			"used_memory_startup:%d\r\n"+
			"used_memory_dataset:%d\r\n"+
			"used_memory_dataset_perc:%.2f\r\n"+
			"used_memory_overhead:%d\r\n"+
			"used_memory_lua:%d\r\n"+
			"maxmemory:%d\r\n"+
			"maxmemory_human:%s\r\n"+
			"maxmemory_policy:%s\r\n"+
			"total_system_memory:%d\r\n"+
			"total_system_memory_human:%s\r\n"+
			"allocator_allocated:%d\r\n"+
			"allocator_active:%d\r\n"+
			"allocator_resident:%d\r\n"+
			"allocator_frag_ratio:%.2f\r\n"+
			"mem_not_counted_for_evict:%d\r\n"+
			"mem_fragmentation_ratio:%.2f\r\n"+
			"mem_fragmentation_bytes:%d\r\n"+
			"mem_clients_slaves:%d\r\n"+
			"mem_clients_normal:%d\r\n"+
			"mem_cluster_links:%d\r\n"+
			"active_defrag_running:%d\r\n"+
			"mem_allocator:%s\r\n",
			m.Alloc,
			humanReadableSize(m.Alloc),
			m.Sys,
			humanReadableSize(m.Sys),
			peak,
			humanReadableSize(peak),
			peakPerc,
			memoryStartupBytes,
			dataset,
			datasetPerc,
			overhead,
			scripting.GetGlobalLuaMemory(),
			maxMem,
			humanReadableSize(maxMemU),
			maxMemPolicy,
			totalSys,
			humanReadableSize(totalSys),
			m.HeapAlloc,
			m.HeapSys,
			m.Sys,
			float64(m.HeapSys)/float64(max(m.HeapAlloc, 1)),
			0, // mem_not_counted_for_evict
			float64(m.Sys)/float64(max(m.Alloc, 1)),
			fragBytes,
			0, // mem_clients_slaves
			0, // mem_clients_normal
			0, // mem_cluster_links
			0, // active_defrag_running
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
			"total_net_repl_input_bytes:%d\r\n"+
			"total_net_repl_output_bytes:%d\r\n"+
			"instantaneous_input_kbps:%.2f\r\n"+
			"instantaneous_output_kbps:%.2f\r\n"+
			"rejected_connections:%d\r\n"+
			"sync_full:%d\r\n"+
			"sync_partial_ok:%d\r\n"+
			"sync_partial_err:%d\r\n"+
			"expired_keys:%d\r\n"+
			"expired_stale_perc:%.2f\r\n"+
			"expired_time_cap_reached_count:%d\r\n"+
			"evicted_keys:%d\r\n"+
			"keyspace_hits:%d\r\n"+
			"keyspace_misses:%d\r\n"+
			"total_error_replies:%d\r\n"+
			"total_reads_processed:%d\r\n"+
			"total_writes_processed:%d\r\n"+
			"pubsub_channels:%d\r\n"+
			"pubsub_patterns:%d\r\n"+
			"latest_fork_usec:%d\r\n"+
			"total_forks:%d\r\n"+
			"total_blocking_keys:%d\r\n"+
			"total_blocking_keys_on_keys:%d\r\n"+
			"current_cow_size:%d\r\n"+
			"current_cow_size_age:%d\r\n"+
			"current_fork_perc:%.2f\r\n"+
			"migrate_cached_sockets:%d\r\n"+
			"slave_expires_tracked_keys:%d\r\n"+
			"active_defrag_hits:%d\r\n"+
			"active_defrag_misses:%d\r\n"+
			"active_defrag_key_hits:%d\r\n"+
			"active_defrag_key_misses:%d\r\n"+
			"eventloop_cycles:%d\r\n"+
			"eventloop_duration_sum:%d\r\n"+
			"eventloop_duration_max:%d\r\n",
			serverStats.TotalConnectionsReceived,
			serverStats.TotalCommandsProcessed,
			getInstantaneousOpsPerSec(),
			getNetInputBytes(),
			getNetOutputBytes(),
			uint64(0), // total_net_repl_input_bytes
			uint64(0), // total_net_repl_output_bytes
			getInstantaneousInputKbps(),
			getInstantaneousOutputKbps(),
			tcp.GetRejectedConnections(),
			atomic.LoadUint64(&syncFullCount),
			atomic.LoadUint64(&syncPartialOKCount),
			atomic.LoadUint64(&syncPartialErrCount),
			serverStats.ExpiredKeys,
			getExpiredStalePerc(),
			uint64(0), // expired_time_cap_reached_count - TODO
			serverStats.EvictedKeys,
			serverStats.KeyspaceHits,
			serverStats.KeyspaceMisses,
			atomic.LoadUint64(&serverStats.TotalErrorReplies),
			atomic.LoadUint64(&serverStats.TotalReadsProcessed),
			atomic.LoadUint64(&serverStats.TotalWritesProcessed),
			getPubsubChannelsCount(db),
			getPubsubPatternsCount(db),
			0,   // latest_fork_usec - N/A in Go
			0,   // total_forks
			0,   // total_blocking_keys
			0,   // total_blocking_keys_on_keys
			0,   // current_cow_size
			0,   // current_cow_size_age
			0.0, // current_fork_perc
			0,   // migrate_cached_sockets - TODO
			0,   // slave_expires_tracked_keys - TODO
			0,   // active_defrag_hits - N/A
			0,   // active_defrag_misses - N/A
			0,   // active_defrag_key_hits - N/A
			0,   // active_defrag_key_misses - N/A
			0,   // eventloop_cycles
			0,   // eventloop_duration_sum
			0,   // eventloop_duration_max
		)
		return []byte(s)
	case "cluster":
		if getGodisRunningMode() == config.ClusterMode {
			s := "# Cluster\r\n" +
				"cluster_enabled:1\r\n" +
				"cluster_state:ok\r\n" +
				"cluster_slots_assigned:16384\r\n" +
				"cluster_slots_ok:16384\r\n" +
				"cluster_slots_pfail:0\r\n" +
				"cluster_slots_fail:0\r\n" +
				"cluster_known_nodes:1\r\n" +
				"cluster_size:1\r\n" +
				"cluster_current_epoch:0\r\n" +
				"cluster_my_epoch:0\r\n" +
				"cluster_stats_messages_sent:0\r\n" +
				"cluster_stats_messages_received:0\r\n"
			return []byte(s)
		}
		s := fmt.Sprintf("# Cluster\r\n"+
			"cluster_enabled:%s\r\n",
			"0",
		)
		return []byte(s)
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
	case "errorstats":
		return []byte(genErrorStatsInfo())
	case "modules":
		return []byte("# Modules\r\n")
	case "latency":
		return []byte("# Latency\r\nlatency_monitor_threshold:0\r\n")
	case "keyspace":
		dbCount := config.Properties.Databases
		var serv []byte
		for i := 0; i < dbCount; i++ {
			keys, expiresKeys, _ := db.GetDBSize(i)
			if keys != 0 {
				ttlSampleAverage, _ := db.GetAvgTTL(i, 20)
				subexpiry, _ := db.CountSubexpiry(i)
				serv = append(serv, getDbSize(i, keys, expiresKeys, ttlSampleAverage, subexpiry)...)
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
		"aof_base_size:%d\r\n"+
		"aof_pending_rewrite:%d\r\n"+
		"aof_buffer_length:%d\r\n"+
		"aof_rewrite_buffer_length:%d\r\n"+
		"aof_pending_bio_fsync:%d\r\n"+
		"aof_delayed_fsync:%d\r\n"+
		"rdb_last_cow_size:%d\r\n"+
		"aof_last_cow_size:%d\r\n",
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
		0,
		0,
		0,
		0,
		0,
		0, // rdb_last_cow_size
		0, // aof_last_cow_size
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

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("# Replication\r\n"+
		"role:%s\r\n", role))

	if role == "slave" && db.slaveStatus != nil {
		db.slaveStatus.mutex.Lock()
		host := db.slaveStatus.masterHost
		port := db.slaveStatus.masterPort
		lastRecv := db.slaveStatus.lastRecvTime
		masterConn := db.slaveStatus.masterConn
		slaveOffset := db.slaveStatus.replOffset
		db.slaveStatus.mutex.Unlock()

		linkStatus := "down"
		lastIO := int64(-1)
		if masterConn != nil && !lastRecv.IsZero() {
			linkStatus = "up"
			lastIO = int64(time.Since(lastRecv).Seconds())
			if lastIO < 0 {
				lastIO = 0
			}
		} else if masterConn != nil {
			linkStatus = "up"
		}
		ro := 0
		if config.Properties.ReplicaReadOnly {
			ro = 1
		}
		sb.WriteString(fmt.Sprintf(
			"master_host:%s\r\n"+
				"master_port:%d\r\n"+
				"master_link_status:%s\r\n"+
				"master_last_io_seconds_ago:%d\r\n"+
				"master_sync_in_progress:0\r\n"+
				"slave_read_only:%d\r\n"+
				"slave_repl_offset:%d\r\n",
			host, port, linkStatus, lastIO, ro, slaveOffset,
		))
	}

	sb.WriteString(fmt.Sprintf(
		"connected_slaves:%d\r\n"+
			"master_replid:%s\r\n"+
			"master_replid2:%s\r\n"+
			"master_repl_offset:%d\r\n"+
			"second_repl_offset:%d\r\n"+
			"repl_backlog_active:%d\r\n"+
			"repl_backlog_size:%d\r\n"+
			"repl_backlog_first_byte_offset:%d\r\n"+
			"repl_backlog_histlen:%d\r\n"+
			"instantaneous_input_repl_kbps:%.2f\r\n"+
			"instantaneous_output_repl_kbps:%.2f\r\n",
		slaves,
		config.Properties.RunID,
		"",
		replOffset,
		-1,
		replBacklogActive,
		backlogSize,
		backlogFirstOffset,
		backlogHistLen,
		0.0,
		0.0,
	))
	return sb.String()
}

// genCPUInfo generates CPU section for INFO
func genCPUInfo() string {
	userSec, sysSec := stats.GetProcessCPUTime()
	return fmt.Sprintf("# CPU\r\n"+
		"used_cpu_sys:%.2f\r\n"+
		"used_cpu_user:%.2f\r\n"+
		"used_cpu_sys_children:%.2f\r\n"+
		"used_cpu_user_children:%.2f\r\n"+
		"used_cpu_sys_main_thread:%.2f\r\n"+
		"used_cpu_user_main_thread:%.2f\r\n",
		sysSec,
		userSec,
		0.0,
		0.0,
		sysSec,
		userSec,
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
				"cmdstat_%s:calls=%d,usec=%d,usec_per_call=%.2f,rejected_calls=%d,failed_calls=%d\r\n",
				cmdName,
				stat.calls,
				stat.usec,
				stat.usecPerCall,
				stat.rejectedKeys,
				stat.failedCalls,
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
	return GetBlockedListClientsCount() + GetBlockedStreamClientsCount() + GetBlockedZSetClientsCount()
}

func getServerHz() int {
	if config.Properties == nil || config.Properties.Hz <= 0 {
		return 10
	}
	return config.Properties.Hz
}

func getInstantaneousOpsPerSec() int64 {
	now := time.Now().Unix()
	sec := atomic.LoadInt64(&opsWindowSec)
	if sec == now {
		// Prefer completed previous second; fall back to in-progress count.
		if last := atomic.LoadInt64(&opsLastSecCount); last > 0 {
			return last
		}
		return atomic.LoadInt64(&opsWindowCount)
	}
	return atomic.LoadInt64(&opsLastSecCount)
}

func getPubsubChannelsCount(db *Server) int64 {
	if db == nil || db.hub == nil {
		return 0
	}
	return int64(db.hub.NumChannels())
}

func getPubsubPatternsCount(db *Server) int64 {
	if db == nil || db.hub == nil {
		return 0
	}
	return int64(db.hub.NumPat())
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

// getRedisMode returns Redis INFO redis_mode (standalone|cluster).
func getRedisMode() string {
	if config.Properties != nil && config.Properties.ClusterEnable {
		return "cluster"
	}
	return "standalone"
}

// getGodisRuninngTime return the running time of godis
func getGodisRuninngTime() time.Duration {
	return time.Since(config.EachTimeServerInfo.StartUpTime) / time.Second
}

func getDbSize(dbIndex, keys, expiresKeys int, ttl int64, subexpiry int) []byte {
	s := fmt.Sprintf("db%d:keys=%d,expires=%d,avg_ttl=%d,subexpiry=%d\r\n",
		dbIndex, keys, expiresKeys, ttl, subexpiry)
	return []byte(s)
}
