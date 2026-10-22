package database

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// validSaveParams reports whether CONFIG SET save value is Redis-acceptable.
// Empty disables autosave; otherwise requires even-length non-negative int pairs
// with at least one non-(0,0) rule (Redis rejects "0 0" and odd/non-int tokens).
func validSaveParams(s string) bool {
	fields := strings.Fields(strings.TrimSpace(s))
	if len(fields) == 0 {
		return true
	}
	if len(fields)%2 != 0 {
		return false
	}
	any := false
	for i := 0; i < len(fields); i += 2 {
		sec, err1 := strconv.ParseInt(fields[i], 10, 64)
		chg, err2 := strconv.ParseInt(fields[i+1], 10, 64)
		if err1 != nil || err2 != nil || sec < 0 || chg < 0 {
			return false
		}
		if !(sec == 0 && chg == 0) {
			any = true
		}
	}
	return any
}

// execConfig handles CONFIG command
func (server *Server) execConfig(args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'config' command")
	}

	subCmd := strings.ToUpper(string(args[0]))

	switch subCmd {
	case "GET":
		if len(args) < 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'config|get' command")
		}
		return execConfigGet(args[1:])
	case "SET":
		if len(args) < 3 || len(args)%2 != 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'config|set' command")
		}
		return server.execConfigSet(args[1:])
	case "RESETSTAT":
		if len(args) != 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'config|resetstat' command")
		}
		resetServerStats()
		return protocol.MakeOkReply()
	case "REWRITE":
		if len(args) != 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'config|rewrite' command")
		}
		if reply := rewriteConfigFile(); reply != nil {
			return reply
		}
		return protocol.MakeOkReply()
	case "HELP":
		return execConfigHelp(args[1:])
	default:
		return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for '%s'. Try CONFIG HELP.", subCmd))
	}
}

func execConfigHelp(args [][]byte) redis.Reply {
	if len(args) != 0 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'config|help' command")
	}
	return protocol.MakeMultiBulkReply([][]byte{
		[]byte("CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"),
		[]byte("GET <pattern>"),
		[]byte("    Return parameters matching the glob-like <pattern> and their values."),
		[]byte("SET <directive> <value>"),
		[]byte("    Set the configuration <directive> to <value>."),
		[]byte("RESETSTAT"),
		[]byte("    Reset statistics reported by the INFO command."),
		[]byte("REWRITE"),
		[]byte("    Rewrite the configuration file."),
		[]byte("HELP"),
		[]byte("    Print this help."),
	})
}

// execConfigGet handles CONFIG GET.
// Returns MapReply so RESP3 connections get % maps while RESP2 still sees a flat array.
func execConfigGet(parameters [][]byte) redis.Reply {
	m := protocol.MakeMapReply()
	for _, param := range parameters {
		paramStr := strings.ToLower(string(param))
		matches := getConfigMatches(paramStr)
		for _, match := range matches {
			m.Put(match.key, protocol.MakeBulkReply([]byte(match.value)))
		}
	}
	return m
}

type configPair struct {
	key   string
	value string
}

func getConfigMatches(pattern string) []configPair {
	matches := make([]configPair, 0)

	configs := []configPair{
		{"databases", strconv.Itoa(config.Properties.Databases)},
		{"disable-thp", "yes"},
		{"enable-debug-command", "no"},
		{"enable-module-command", "no"},
		{"enable-protected-configs", "no"},
		{"socket-mark-id", "0"},
		{"unixsocket", ""},
		{"unixsocketperm", "0"},
		{"syslog-enabled", "no"},
		{"syslog-ident", "redis"},
		{"syslog-facility", "local0"},
		{"cluster-port", "0"},
		{"cluster-config-file", "nodes.conf"},
		{"supervised", "no"},
		{"port", strconv.Itoa(config.Properties.Port)},
		{"bind", config.Properties.Bind},
		{"requirepass", config.Properties.RequirePass},
		{"appendonly", boolToString(config.Properties.AppendOnly)},
		{"appendfilename", config.Properties.AppendFilename},
		{"appenddirname", getAppendDirname()},
		{"appendfsync", config.Properties.AppendFsync},
		{"aof-use-rdb-preamble", boolToString(config.Properties.AofUseRdbPreamble)},
		{"aof-disable-auto-gc", boolToString(getAofDisableAutoGC())},
		{"rdb-del-sync-files", boolToString(getRdbDelSyncFiles())},
		{"masterauth", config.Properties.MasterAuth},
		{"masteruser", getMasterUser()},
		{"slave-announce-ip", config.Properties.SlaveAnnounceIP},
		{"slave-announce-port", strconv.Itoa(config.Properties.SlaveAnnouncePort)},
		{"replica-announce-ip", config.Properties.SlaveAnnounceIP},
		{"replica-announce-port", strconv.Itoa(config.Properties.SlaveAnnouncePort)},
		{"announce-host", config.Properties.AnnounceHost},
		{"lua-time-limit", strconv.FormatInt(config.Properties.LuaTimeLimit, 10)},
		{"dir", configDir()},
		{"dbfilename", config.Properties.RDBFilename},
		{"rdbfilename", config.Properties.RDBFilename}, // alias
		{"maxclients", strconv.Itoa(config.Properties.MaxClients)},
		{"maxmemory", strconv.FormatInt(config.Properties.Maxmemory, 10)},
		{"maxmemory-policy", config.Properties.MaxmemoryPolicy},
		{"timeout", strconv.Itoa(config.Properties.Timeout)},
		{"tcp-keepalive", strconv.Itoa(config.Properties.TCPKeepAlive)},
		{"loglevel", config.Properties.LogLevel},
		{"log-format", config.Properties.LogFormat},
		{"logfile", config.Properties.LogFile},
		{"protected-mode", boolToString(config.Properties.ProtectedMode)},
		{"daemonize", boolToString(config.Properties.Daemonize)},
		{"pidfile", config.Properties.PidFile},
		{"aclfile", config.Properties.AclFile},
		{"replica-read-only", boolToString(config.Properties.ReplicaReadOnly)},
		{"slave-read-only", boolToString(config.Properties.ReplicaReadOnly)},
		{"lazyfree-lazy-eviction", boolToString(config.Properties.LazyfreeLazyEviction)},
		{"lazyfree-lazy-expire", boolToString(getLazyfreeLazyExpire())},
		{"lazyfree-lazy-server-del", boolToString(getLazyfreeLazyServerDel())},
		{"lazyfree-lazy-user-del", boolToString(getLazyfreeLazyUserDel())},
		{"lazyfree-lazy-user-flush", boolToString(getLazyfreeLazyUserFlush())},
		{"replica-lazy-flush", boolToString(getReplicaLazyFlush())},
		{"slave-lazy-flush", boolToString(getReplicaLazyFlush())},
		{"aof-load-truncated", boolToString(getAofLoadTruncated())},
		{"jemalloc-bg-thread", boolToString(getJemallocBgThread())},
		{"activerehashing", boolToString(getActiveRehashing())},
		{"sanitize-dump-payload", boolToString(getSanitizeDumpPayload())},
		{"ignore-warnings", getIgnoreWarnings()},
		{"replica-announced", boolToString(getReplicaAnnounced())},
		{"set-proc-title", boolToString(getSetProcTitle())},
		{"always-show-logo", boolToString(getAlwaysShowLogo())},
		{"lua-replicate-commands", boolToString(getLuaReplicateCommands())},
		{"client-query-buffer-limit", strconv.FormatInt(getClientQueryBufferLimit(), 10)},
		{"client-output-buffer-limit", getClientOutputBufferLimit()},
		{"min-replicas-to-write", strconv.Itoa(getMinReplicasToWrite())},
		{"min-replicas-max-lag", strconv.Itoa(getMinReplicasMaxLag())},
		{"cluster-require-full-coverage", boolToString(getClusterRequireFullCoverage())},
		{"cluster-node-timeout", strconv.FormatInt(getClusterNodeTimeout(), 10)},
		{"cluster-migration-barrier", strconv.Itoa(getClusterMigrationBarrier())},
		{"cluster-allow-reads-when-down", boolToString(getClusterAllowReadsWhenDown())},
		{"stop-writes-on-bgsave-error", boolToString(getStopWritesOnBgsaveError())},
		{"rdbcompression", boolToString(getRDBCompression())},
		{"rdbchecksum", boolToString(getRDBChecksum())},
		{"no-appendfsync-on-rewrite", boolToString(getNoAppendFsyncOnRewrite())},
		{"auto-aof-rewrite-percentage", strconv.Itoa(getAutoAofRewritePercentage())},
		{"auto-aof-rewrite-min-size", strconv.FormatInt(getAutoAofRewriteMinSize(), 10)},
		{"io-threads", strconv.Itoa(getIOThreads())},
		{"io-threads-do-reads", boolToString(getIOThreadsDoReads())},
		{"repl-diskless-sync", boolToString(getReplDisklessSync())},
		{"repl-diskless-sync-delay", strconv.Itoa(getReplDisklessSyncDelay())},
		{"repl-diskless-sync-max-replicas", strconv.Itoa(getReplDisklessSyncMaxReplicas())},
		{"maxmemory-samples", strconv.Itoa(getMaxmemorySamples())},
		{"tracking-table-max-keys", strconv.FormatInt(getTrackingTableMaxKeys(), 10)},
		{"repl-backlog-ttl", strconv.Itoa(getReplBacklogTTL())},
		{"replica-ignore-maxmemory", boolToString(getReplicaIgnoreMaxmemory())},
		{"slave-ignore-maxmemory", boolToString(getReplicaIgnoreMaxmemory())},
		{"aof-rewrite-incremental-fsync", boolToString(getAofRewriteIncrementalFsync())},
		{"rdb-save-incremental-fsync", boolToString(getRdbSaveIncrementalFsync())},
		{"latency-monitor-threshold", strconv.FormatInt(getLatencyMonitorThreshold(), 10)},
		{"acl-pubsub-default", getAclPubsubDefault()},
		{"repl-ping-replica-period", strconv.Itoa(getReplPingReplicaPeriod())},
		{"lfu-log-factor", strconv.Itoa(getLFULogFactor())},
		{"lfu-decay-time", strconv.Itoa(getLFUDecayTime())},
		{"maxmemory-eviction-tenacity", strconv.Itoa(getMaxmemoryEvictionTenacity())},
		{"list-compress-depth", strconv.Itoa(getListCompressDepth())},
		{"aof-timestamp-enabled", boolToString(getAofTimestampEnabled())},
		{"repl-disable-tcp-nodelay", boolToString(getReplDisableTCPNodelay())},
		{"latency-tracking", boolToString(getLatencyTracking())},
		{"crash-log-enabled", boolToString(getCrashLogEnabled())},
		{"crash-memcheck-enabled", boolToString(getCrashMemcheckEnabled())},
		{"repl-diskless-load", getReplDisklessLoad()},
		{"cluster-preferred-endpoint-type", getClusterPreferredEndpointType()},
		{"cluster-link-sendbuf-limit", strconv.FormatInt(getClusterLinkSendbufLimit(), 10)},
		{"cluster-announce-hostname", getClusterAnnounceHostname()},
		{"cluster-announce-human-nodename", getClusterAnnounceHumanNodename()},
		{"shutdown-timeout", strconv.Itoa(getShutdownTimeout())},
		{"shutdown-on-sigint", getShutdownOnSigint()},
		{"shutdown-on-sigterm", getShutdownOnSigterm()},
		{"locale-collate", getLocaleCollate()},
		{"latency-tracking-info-percentiles", getLatencyTrackingInfoPercentiles()},
		{"active-defrag-ignore-bytes", strconv.FormatInt(getActiveDefragIgnoreBytes(), 10)},
		{"active-defrag-threshold-lower", strconv.Itoa(getActiveDefragThresholdLower())},
		{"active-defrag-threshold-upper", strconv.Itoa(getActiveDefragThresholdUpper())},
		{"active-defrag-cycle-min", strconv.Itoa(getActiveDefragCycleMin())},
		{"active-defrag-cycle-max", strconv.Itoa(getActiveDefragCycleMax())},
		{"active-defrag-max-scan-fields", strconv.FormatInt(getActiveDefragMaxScanFields(), 10)},
		{"oom-score-adj-values", getOOMScoreAdjValues()},
		{"propagation-error-behavior", getPropagationErrorBehavior()},
		{"hide-user-data-from-log", boolToString(getHideUserDataFromLog())},
		{"cluster-replica-no-failover", boolToString(getClusterReplicaNoFailover())},
		{"cluster-slave-no-failover", boolToString(getClusterReplicaNoFailover())},
		{"cluster-allow-pubsubshard-when-down", boolToString(getClusterAllowPubsubshardWhenDown())},
		{"proc-title-template", getProcTitleTemplate()},
		{"active-expire-effort", strconv.Itoa(getActiveExpireEffort())},
		{"tls-cert-file", getTLSCertFile()},
		{"tls-key-file", getTLSKeyFile()},
		{"tls-ca-cert-file", getTLSCACertFile()},
		{"tls-protocols", getTLSProtocols()},
		{"tls-ciphers", getTLSCiphers()},
		{"tls-auth-clients", getTLSAuthClients()},
		{"tls-replication", boolToString(getTLSReplication())},
		{"tls-cluster", boolToString(getTLSCluster())},
		{"tls-session-caching", boolToString(getTLSSessionCaching())},
		{"tls-session-cache-size", strconv.Itoa(getTLSSessionCacheSize())},
		{"tls-session-cache-timeout", strconv.Itoa(getTLSSessionCacheTimeout())},
		{"tls-prefer-server-ciphers", boolToString(getTLSPreferServerCiphers())},
		{"cluster-announce-tls-port", strconv.Itoa(getClusterAnnounceTLSPort())},
		{"tls-port", strconv.Itoa(getTLSPort())},
		{"tls-dh-params-file", getTLSDhParamsFile()},
		{"tls-ciphersuites", getTLSCiphersuites()},
		{"tls-client-cert-file", getTLSClientCertFile()},
		{"tls-client-key-file", getTLSClientKeyFile()},
		{"tls-key-file-pass", getTLSKeyFilePass()},
		{"tls-client-key-file-pass", getTLSClientKeyFilePass()},
		{"maxmemory-clients", getMaxmemoryClients()},
		{"cluster-allow-replica-migration", boolToString(getClusterAllowReplicaMigration())},
		{"cluster-replica-validity-factor", strconv.Itoa(getClusterReplicaValidityFactor())},
		{"cluster-slave-validity-factor", strconv.Itoa(getClusterReplicaValidityFactor())},
		{"hash-max-listpack-entries", strconv.Itoa(getHashMaxListpackEntries())},
		{"hash-max-ziplist-entries", strconv.Itoa(getHashMaxListpackEntries())},
		{"list-max-listpack-size", strconv.Itoa(getListMaxListpackSize())},
		{"list-max-ziplist-size", strconv.Itoa(getListMaxListpackSize())},
		{"set-max-intset-entries", strconv.Itoa(getSetMaxIntsetEntries())},
		{"zset-max-listpack-entries", strconv.Itoa(getZSetMaxListpackEntries())},
		{"zset-max-ziplist-entries", strconv.Itoa(getZSetMaxListpackEntries())},
		{"zset-max-listpack-value", strconv.Itoa(getZSetMaxListpackValue())},
		{"zset-max-ziplist-value", strconv.Itoa(getZSetMaxListpackValue())},
		{"stream-node-max-bytes", strconv.FormatInt(getStreamNodeMaxBytes(), 10)},
		{"stream-idmp-duration", strconv.FormatInt(getStreamIdmpDuration(), 10)},
		{"stream-idmp-maxsize", strconv.Itoa(getStreamIdmpMaxSize())},
		{"hll-sparse-max-bytes", strconv.Itoa(getHLLSparseMaxBytes())},
		{"cluster-announce-ip", getClusterAnnounceIP()},
		{"cluster-announce-port", strconv.Itoa(getClusterAnnouncePort())},
		{"cluster-announce-bus-port", strconv.Itoa(getClusterAnnounceBusPort())},
		{"stream-node-max-entries", strconv.FormatInt(getStreamNodeMaxEntries(), 10)},
		{"hash-max-listpack-value", strconv.Itoa(getHashMaxListpackValue())},
		{"hash-max-ziplist-value", strconv.Itoa(getHashMaxListpackValue())},
		{"set-max-listpack-entries", strconv.Itoa(getSetMaxListpackEntries())},
		{"oom-score-adj", getOOMScoreAdj()},
		{"replicaof", getReplicaOf()},
		{"slaveof", getReplicaOf()},
		{"replica-serve-stale-data", boolToString(getReplicaServeStaleData())},
		{"slave-serve-stale-data", boolToString(getReplicaServeStaleData())},
		{"replica-priority", strconv.Itoa(getReplicaPriority())},
		{"slave-priority", strconv.Itoa(getReplicaPriority())},
		{"set-max-listpack-value", strconv.Itoa(getSetMaxListpackValue())},
		{"proto-max-bulk-len", strconv.FormatInt(config.Properties.ProtoMaxBulkLen, 10)},
		{"save", config.Properties.Save},
		{"tcp-backlog", strconv.Itoa(config.Properties.TCPBacklog)},
		{"hz", strconv.Itoa(getServerHz())},
		{"notify-keyspace-events", config.Properties.NotifyKeyspaceEvents},
		{"activedefrag", boolToString(config.Properties.ActiveDefrag)},
		{"busy-reply-threshold", strconv.FormatInt(getBusyReplyThreshold(), 10)},
		{"dynamic-hz", boolToString(getDynamicHz())},
		{"repl-backlog-size", strconv.FormatInt(getReplBacklogSizeConfig(), 10)},
		{"slowlog-log-slower-than", strconv.FormatInt(config.Properties.SlowLogSlowerThan, 10)},
		{"slowlog-max-len", strconv.Itoa(config.Properties.SlowLogMaxLen)},
		{"acllog-max-len", strconv.Itoa(config.Properties.AclLogMaxLen)},
		{"cluster-enabled", boolToString(config.Properties.ClusterEnable)},
		{"cluster-as-seed", boolToString(config.Properties.ClusterAsSeed)},
		{"cluster-seed", config.Properties.ClusterSeed},
		{"raft-listen-address", config.Properties.RaftListenAddr},
		{"raft-advertise-address", config.Properties.RaftAdvertiseAddr},
		{"master-in-cluster", config.Properties.MasterInCluster},
		{"repl-timeout", strconv.Itoa(config.Properties.ReplTimeout)},
		{"use-gnet", boolToString(config.Properties.UseGnet)},
		{"search-backend", config.Properties.SearchBackend},
		{"vector-backend", config.Properties.VectorBackend},
		{"metrics-addr", config.Properties.MetricsAddr},
		{"search-sqlite-path", config.Properties.SearchSQLitePath},
		{"sqlite-mmap-size", strconv.FormatInt(config.Properties.SqliteMmapSize, 10)},
	}

	// Append the Redis 8.0 search-* namespace (mirrors FT.CONFIG values).
	configs = append(configs, searchKebabPairs()...)

	for _, cfg := range configs {
		if patternMatch(pattern, cfg.key) {
			matches = append(matches, cfg)
		}
	}

	return matches
}

func patternMatch(pattern, str string) bool {
	if pattern == str {
		return true
	}
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		prefix := pattern[:len(pattern)-1]
		return strings.HasPrefix(str, prefix)
	}
	if strings.HasPrefix(pattern, "*") {
		suffix := pattern[1:]
		return strings.HasSuffix(str, suffix)
	}
	return false
}

func boolToString(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func getReplBacklogSizeConfig() int64 {
	if config.Properties != nil && config.Properties.ReplBacklogSize > 0 {
		return config.Properties.ReplBacklogSize
	}
	return 10 * 1024 * 1024 // align with maxBacklogSize
}

func getBusyReplyThreshold() int64 {
	if config.Properties != nil && config.Properties.BusyReplyThreshold > 0 {
		return config.Properties.BusyReplyThreshold
	}
	return 5000 // Redis default
}

func getDynamicHz() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.DynamicHz
}

func getLazyfreeLazyExpire() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.LazyfreeLazyExpire
}

func getLazyfreeLazyServerDel() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.LazyfreeLazyServerDel
}

func getJemallocBgThread() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.JemallocBgThread
}

func getLazyfreeLazyUserDel() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.LazyfreeLazyUserDel
}

func getLazyfreeLazyUserFlush() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.LazyfreeLazyUserFlush
}

func getReplicaLazyFlush() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.ReplicaLazyFlush
}

func getAofLoadTruncated() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.AofLoadTruncated
}

func getActiveRehashing() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.ActiveRehashing
}

func getSanitizeDumpPayload() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.SanitizeDumpPayload
}

func getIgnoreWarnings() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.IgnoreWarnings
}

func getReplicaAnnounced() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.ReplicaAnnounced
}

func getSetProcTitle() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.SetProcTitle
}

func getAlwaysShowLogo() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.AlwaysShowLogo
}

func getLuaReplicateCommands() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.LuaReplicateCommands
}

func getClientQueryBufferLimit() int64 {
	if config.Properties == nil || config.Properties.ClientQueryBufferLimit <= 0 {
		return 1073741824
	}
	return config.Properties.ClientQueryBufferLimit
}

func getClientOutputBufferLimit() string {
	if config.Properties == nil || config.Properties.ClientOutputBufferLimit == "" {
		return "normal 0 0 0 slave 268435456 67108864 60 pubsub 33554432 8388608 60"
	}
	return config.Properties.ClientOutputBufferLimit
}

func getMinReplicasToWrite() int {
	if config.Properties == nil {
		return 0
	}
	return config.Properties.MinReplicasToWrite
}

func getMinReplicasMaxLag() int {
	if config.Properties == nil || config.Properties.MinReplicasMaxLag <= 0 {
		return 10
	}
	return config.Properties.MinReplicasMaxLag
}

func getClusterRequireFullCoverage() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.ClusterRequireFullCoverage
}

func getClusterNodeTimeout() int64 {
	if config.Properties == nil || config.Properties.ClusterNodeTimeout <= 0 {
		return 15000
	}
	return config.Properties.ClusterNodeTimeout
}

func getClusterMigrationBarrier() int {
	if config.Properties == nil {
		return 1
	}
	return config.Properties.ClusterMigrationBarrier
}

func getClusterAllowReadsWhenDown() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.ClusterAllowReadsWhenDown
}

func getStopWritesOnBgsaveError() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.StopWritesOnBgsaveError
}

func getRDBCompression() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.RDBCompression
}

func getRDBChecksum() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.RDBChecksum
}

func getNoAppendFsyncOnRewrite() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.NoAppendFsyncOnRewrite
}

func getAutoAofRewritePercentage() int {
	if config.Properties == nil {
		return 100
	}
	return config.Properties.AutoAofRewritePercentage
}

func getAutoAofRewriteMinSize() int64 {
	if config.Properties == nil || config.Properties.AutoAofRewriteMinSize <= 0 {
		return 67108864
	}
	return config.Properties.AutoAofRewriteMinSize
}

func getIOThreads() int {
	if config.Properties == nil || config.Properties.IOThreads < 1 {
		return 1
	}
	return config.Properties.IOThreads
}

func getIOThreadsDoReads() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.IOThreadsDoReads
}

func getReplDisklessSync() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.ReplDisklessSync
}

func getReplDisklessSyncDelay() int {
	if config.Properties == nil {
		return 5
	}
	return config.Properties.ReplDisklessSyncDelay
}

func getReplDisklessSyncMaxReplicas() int {
	if config.Properties == nil {
		return 0
	}
	return config.Properties.ReplDisklessSyncMaxReplicas
}

func getMaxmemorySamples() int {
	if config.Properties == nil || config.Properties.MaxmemorySamples < 1 {
		return 5
	}
	return config.Properties.MaxmemorySamples
}

func getTrackingTableMaxKeys() int64 {
	if config.Properties == nil {
		return 1000000
	}
	return config.Properties.TrackingTableMaxKeys
}

func getReplBacklogTTL() int {
	if config.Properties == nil {
		return 3600
	}
	return config.Properties.ReplBacklogTTL
}

func getReplicaIgnoreMaxmemory() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.ReplicaIgnoreMaxmemory
}

func getAofRewriteIncrementalFsync() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.AofRewriteIncrementalFsync
}

func getRdbDelSyncFiles() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.RdbDelSyncFiles
}

func getAofDisableAutoGC() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.AofDisableAutoGC
}

func getAppendDirname() string {
	if config.Properties == nil || config.Properties.AppendDirname == "" {
		return "appendonlydir"
	}
	return config.Properties.AppendDirname
}

func getRdbSaveIncrementalFsync() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.RdbSaveIncrementalFsync
}

func getLatencyMonitorThreshold() int64 {
	if config.Properties == nil {
		return 0
	}
	return config.Properties.LatencyMonitorThreshold
}

func getAclPubsubDefault() string {
	if config.Properties == nil || config.Properties.AclPubsubDefault == "" {
		return "resetchannels"
	}
	return config.Properties.AclPubsubDefault
}

func getReplPingReplicaPeriod() int {
	if config.Properties == nil || config.Properties.ReplPingReplicaPeriod == 0 {
		return 10
	}
	return config.Properties.ReplPingReplicaPeriod
}

func getLFULogFactor() int {
	if config.Properties == nil {
		return 10
	}
	return config.Properties.LFULogFactor
}

func getLFUDecayTime() int {
	if config.Properties == nil {
		return 1
	}
	return config.Properties.LFUDecayTime
}

func getMaxmemoryEvictionTenacity() int {
	if config.Properties == nil {
		return 10
	}
	return config.Properties.MaxmemoryEvictionTenacity
}

func getListCompressDepth() int {
	if config.Properties == nil {
		return 0
	}
	return config.Properties.ListCompressDepth
}

func getAofTimestampEnabled() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.AofTimestampEnabled
}

func getReplDisableTCPNodelay() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.ReplDisableTCPNodelay
}

func getLatencyTracking() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.LatencyTracking
}

func getCrashLogEnabled() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.CrashLogEnabled
}

func getCrashMemcheckEnabled() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.CrashMemcheckEnabled
}

func getReplDisklessLoad() string {
	if config.Properties == nil || config.Properties.ReplDisklessLoad == "" {
		return "disabled"
	}
	return config.Properties.ReplDisklessLoad
}

func getClusterPreferredEndpointType() string {
	if config.Properties == nil || config.Properties.ClusterPreferredEndpointType == "" {
		return "ip"
	}
	return config.Properties.ClusterPreferredEndpointType
}

func getClusterLinkSendbufLimit() int64 {
	if config.Properties == nil {
		return 0
	}
	return config.Properties.ClusterLinkSendbufLimit
}

func getClusterAnnounceHostname() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.ClusterAnnounceHostname
}

func getClusterAnnounceHumanNodename() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.ClusterAnnounceHumanNodename
}

func getShutdownTimeout() int {
	if config.Properties == nil {
		return 0
	}
	return config.Properties.ShutdownTimeout
}

func getShutdownOnSigint() string {
	if config.Properties == nil || config.Properties.ShutdownOnSigint == "" {
		return "default"
	}
	return config.Properties.ShutdownOnSigint
}

func getShutdownOnSigterm() string {
	if config.Properties == nil || config.Properties.ShutdownOnSigterm == "" {
		return "default"
	}
	return config.Properties.ShutdownOnSigterm
}

func getLocaleCollate() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.LocaleCollate
}

func getLatencyTrackingInfoPercentiles() string {
	if config.Properties == nil || config.Properties.LatencyTrackingInfoPercentiles == "" {
		return "50 99 99.9"
	}
	return config.Properties.LatencyTrackingInfoPercentiles
}

func getActiveDefragIgnoreBytes() int64 {
	if config.Properties == nil {
		return 104857600
	}
	return config.Properties.ActiveDefragIgnoreBytes
}

func getActiveDefragThresholdLower() int {
	if config.Properties == nil {
		return 10
	}
	return config.Properties.ActiveDefragThresholdLower
}

func getActiveDefragThresholdUpper() int {
	if config.Properties == nil {
		return 100
	}
	return config.Properties.ActiveDefragThresholdUpper
}

func getActiveDefragCycleMin() int {
	if config.Properties == nil {
		return 1
	}
	return config.Properties.ActiveDefragCycleMin
}

func getActiveDefragCycleMax() int {
	if config.Properties == nil {
		return 25
	}
	return config.Properties.ActiveDefragCycleMax
}

func getActiveDefragMaxScanFields() int64 {
	if config.Properties == nil {
		return 1000
	}
	return config.Properties.ActiveDefragMaxScanFields
}

func getOOMScoreAdjValues() string {
	if config.Properties == nil || config.Properties.OOMScoreAdjValues == "" {
		return "0 200 800"
	}
	return config.Properties.OOMScoreAdjValues
}

func getPropagationErrorBehavior() string {
	if config.Properties == nil || config.Properties.PropagationErrorBehavior == "" {
		return "ignore"
	}
	return config.Properties.PropagationErrorBehavior
}

func getHideUserDataFromLog() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.HideUserDataFromLog
}

func getClusterReplicaNoFailover() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.ClusterReplicaNoFailover
}

func getClusterAllowPubsubshardWhenDown() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.ClusterAllowPubsubshardWhenDown
}

func getProcTitleTemplate() string {
	if config.Properties == nil || config.Properties.ProcTitleTemplate == "" {
		return "{title} {listen-addr} {server-mode}"
	}
	return config.Properties.ProcTitleTemplate
}

func getActiveExpireEffort() int {
	if config.Properties == nil || config.Properties.ActiveExpireEffort == 0 {
		return 1
	}
	return config.Properties.ActiveExpireEffort
}

func getTLSCertFile() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.TLSCertFile
}

func getTLSKeyFile() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.TLSKeyFile
}

func getTLSCACertFile() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.TLSCACertFile
}

func getTLSProtocols() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.TLSProtocols
}

func getTLSCiphers() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.TLSCiphers
}

func getTLSAuthClients() string {
	if config.Properties == nil || config.Properties.TLSAuthClients == "" {
		return "no"
	}
	return config.Properties.TLSAuthClients
}

func getTLSReplication() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.TLSReplication
}

func getTLSCluster() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.TLSCluster
}

func getTLSSessionCaching() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.TLSSessionCaching
}

func getTLSSessionCacheSize() int {
	if config.Properties == nil {
		return 0
	}
	return config.Properties.TLSSessionCacheSize
}

func getTLSSessionCacheTimeout() int {
	if config.Properties == nil {
		return 300
	}
	return config.Properties.TLSSessionCacheTimeout
}

func getTLSPreferServerCiphers() bool {
	if config.Properties == nil {
		return false
	}
	return config.Properties.TLSPreferServerCiphers
}

func getClusterAnnounceTLSPort() int {
	if config.Properties == nil {
		return 0
	}
	return config.Properties.ClusterAnnounceTLSPort
}

func getTLSPort() int {
	if config.Properties == nil {
		return 0
	}
	return config.Properties.TLSPort
}

func getTLSDhParamsFile() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.TLSDhParamsFile
}

func getTLSCiphersuites() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.TLSCiphersuites
}

func getTLSClientCertFile() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.TLSClientCertFile
}

func getTLSClientKeyFile() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.TLSClientKeyFile
}

func getTLSKeyFilePass() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.TLSKeyFilePass
}

func getTLSClientKeyFilePass() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.TLSClientKeyFilePass
}

func getMaxmemoryClients() string {
	if config.Properties == nil || config.Properties.MaxmemoryClients == "" {
		return "0"
	}
	return config.Properties.MaxmemoryClients
}

// validateMaxmemoryClients accepts a non-negative memory integer or a percent ≤100.
func validateMaxmemoryClients(value string) string {
	v := strings.TrimSpace(value)
	if strings.HasSuffix(v, "%") {
		n, err := strconv.Atoi(strings.TrimSuffix(v, "%"))
		if err != nil || n < 0 {
			return "argument must be a memory or percent value"
		}
		if n > 100 {
			return "percentage argument must be less or equal to 100"
		}
		return ""
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return "argument must be a memory or percent value"
	}
	return ""
}

func validateOOMScoreAdjValues(value string) string {
	fields := strings.Fields(value)
	if len(fields) != 3 {
		return "wrong number of arguments"
	}
	for _, f := range fields {
		n, err := strconv.Atoi(f)
		if err != nil || n < -2000 || n > 2000 {
			return "Invalid oom-score-adj-values, elements must be between -2000 and 2000."
		}
	}
	return ""
}

func validateProcTitleTemplate(value string) string {
	if strings.TrimSpace(value) == "" || strings.TrimSpace(value) == "{title}" {
		return "template format is invalid or contains unknown variables"
	}
	known := map[string]struct{}{
		"title": {}, "listen-addr": {}, "server-mode": {},
		"port": {}, "tls-port": {}, "unixsocket": {},
		"config-file": {}, "exec-path": {},
	}
	s := value
	for {
		start := strings.IndexByte(s, '{')
		if start < 0 {
			break
		}
		end := strings.IndexByte(s[start:], '}')
		if end < 0 {
			return "template format is invalid or contains unknown variables"
		}
		end += start
		name := s[start+1 : end]
		if _, ok := known[name]; !ok {
			return "template format is invalid or contains unknown variables"
		}
		s = s[end+1:]
	}
	return ""
}

// validateClientOutputBufferLimit returns Redis-style error suffix, or "" if OK.
func validateClientOutputBufferLimit(value string) string {
	fields := strings.Fields(value)
	if len(fields) == 0 || len(fields)%4 != 0 {
		return "Wrong number of arguments in buffer limit configuration."
	}
	for i := 0; i < len(fields); i += 4 {
		class := strings.ToLower(fields[i])
		if class != "normal" && class != "slave" && class != "replica" && class != "pubsub" {
			return "Invalid client class specified in buffer limit configuration."
		}
		for _, part := range fields[i+1 : i+4] {
			n, err := strconv.ParseInt(part, 10, 64)
			if err != nil || n < 0 {
				return "Error in hard, soft or soft_seconds setting in buffer limit configuration."
			}
		}
	}
	return ""
}

// validateLatencyTrackingInfoPercentiles normalizes space-separated percentiles in [0,100].
func validateLatencyTrackingInfoPercentiles(value string) (string, string) {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return "", ""
	}
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		n, err := strconv.ParseFloat(f, 64)
		if err != nil {
			return "", "Invalid latency-tracking-info-percentiles parameters"
		}
		if n < 0 || n > 100 {
			return "", "latency-tracking-info-percentiles parameters should sit between [0.0,100.0]"
		}
		out = append(out, f)
	}
	return strings.Join(out, " "), ""
}

func getClusterAllowReplicaMigration() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.ClusterAllowReplicaMigration
}

func getClusterReplicaValidityFactor() int {
	if config.Properties == nil {
		return 10
	}
	return config.Properties.ClusterReplicaValidityFactor
}

func getHashMaxListpackEntries() int {
	if config.Properties == nil || config.Properties.HashMaxListpackEntries <= 0 {
		return 512
	}
	return config.Properties.HashMaxListpackEntries
}

func getListMaxListpackSize() int {
	if config.Properties == nil {
		return -2
	}
	return config.Properties.ListMaxListpackSize
}

func getSetMaxIntsetEntries() int {
	if config.Properties == nil || config.Properties.SetMaxIntsetEntries <= 0 {
		return 512
	}
	return config.Properties.SetMaxIntsetEntries
}

func getZSetMaxListpackEntries() int {
	if config.Properties == nil || config.Properties.ZSetMaxListpackEntries <= 0 {
		return 128
	}
	return config.Properties.ZSetMaxListpackEntries
}

func getZSetMaxListpackValue() int {
	if config.Properties == nil || config.Properties.ZSetMaxListpackValue <= 0 {
		return 64
	}
	return config.Properties.ZSetMaxListpackValue
}

func getStreamNodeMaxBytes() int64 {
	if config.Properties == nil || config.Properties.StreamNodeMaxBytes <= 0 {
		return 4096
	}
	return config.Properties.StreamNodeMaxBytes
}

func getHLLSparseMaxBytes() int {
	if config.Properties == nil || config.Properties.HLLSparseMaxBytes <= 0 {
		return 3000
	}
	return config.Properties.HLLSparseMaxBytes
}

func getClusterAnnounceIP() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.ClusterAnnounceIP
}

func getClusterAnnouncePort() int {
	if config.Properties == nil {
		return 0
	}
	return config.Properties.ClusterAnnouncePort
}

func getClusterAnnounceBusPort() int {
	if config.Properties == nil {
		return 0
	}
	return config.Properties.ClusterAnnounceBusPort
}

func getStreamNodeMaxEntries() int64 {
	if config.Properties == nil || config.Properties.StreamNodeMaxEntries <= 0 {
		return 100
	}
	return config.Properties.StreamNodeMaxEntries
}

func getHashMaxListpackValue() int {
	if config.Properties == nil || config.Properties.HashMaxListpackValue <= 0 {
		return 64
	}
	return config.Properties.HashMaxListpackValue
}

func getSetMaxListpackEntries() int {
	if config.Properties == nil || config.Properties.SetMaxListpackEntries <= 0 {
		return 128
	}
	return config.Properties.SetMaxListpackEntries
}

func getSetMaxListpackValue() int {
	if config.Properties == nil || config.Properties.SetMaxListpackValue <= 0 {
		return 64
	}
	return config.Properties.SetMaxListpackValue
}

func getOOMScoreAdj() string {
	if config.Properties == nil || config.Properties.OOMScoreAdj == "" {
		return "no"
	}
	return config.Properties.OOMScoreAdj
}

func getMasterUser() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.MasterUser
}

func validateBindAddresses(value string) string {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return "Failed to bind to specified addresses."
	}
	for _, a := range fields {
		a = strings.TrimPrefix(a, "-")
		if a == "*" || a == "::*" {
			continue
		}
		if net.ParseIP(a) != nil {
			continue
		}
		if _, err := net.LookupHost(a); err != nil {
			return "Failed to bind to specified addresses."
		}
	}
	return ""
}

func getReplicaOf() string {
	if config.Properties == nil {
		return ""
	}
	return config.Properties.ReplicaOf
}

func getReplicaServeStaleData() bool {
	if config.Properties == nil {
		return true
	}
	return config.Properties.ReplicaServeStaleData
}

func getReplicaPriority() int {
	if config.Properties == nil || config.Properties.ReplicaPriority <= 0 {
		return 100
	}
	return config.Properties.ReplicaPriority
}

func configDir() string {
	if config.Properties == nil || config.Properties.Dir == "" {
		return "."
	}
	return config.Properties.Dir
}

func init() {
	registerSpecialCommand("Config", -2, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagLoading, redisFlagStale}, 0, 0, 0)
}

func getStreamIdmpDuration() int64 {
	if config.Properties == nil || config.Properties.StreamIdmpDuration <= 0 {
		return 300
	}
	return config.Properties.StreamIdmpDuration
}

func getStreamIdmpMaxSize() int {
	if config.Properties == nil || config.Properties.StreamIdmpMaxSize <= 0 {
		return 10000
	}
	return config.Properties.StreamIdmpMaxSize
}
