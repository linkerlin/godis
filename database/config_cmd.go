package database

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/logger"
	"github.com/linkerlin/godis/lib/memory"
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
		[]byte("    Return parameters matching the pattern."),
		[]byte("SET <parameter> <value> [<parameter> <value> ...]"),
		[]byte("    Set configuration parameters."),
		[]byte("RESETSTAT"),
		[]byte("    Reset statistics reported by INFO."),
		[]byte("REWRITE"),
		[]byte("    Rewrite the configuration file with the current configuration."),
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
		{"port", strconv.Itoa(config.Properties.Port)},
		{"bind", config.Properties.Bind},
		{"requirepass", config.Properties.RequirePass},
		{"appendonly", boolToString(config.Properties.AppendOnly)},
		{"appendfilename", config.Properties.AppendFilename},
		{"appendfsync", config.Properties.AppendFsync},
		{"aof-use-rdb-preamble", boolToString(config.Properties.AofUseRdbPreamble)},
		{"masterauth", config.Properties.MasterAuth},
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
		{"maxmemory-samples", strconv.Itoa(getMaxmemorySamples())},
		{"tracking-table-max-keys", strconv.FormatInt(getTrackingTableMaxKeys(), 10)},
		{"repl-backlog-ttl", strconv.Itoa(getReplBacklogTTL())},
		{"replica-ignore-maxmemory", boolToString(getReplicaIgnoreMaxmemory())},
		{"aof-rewrite-incremental-fsync", boolToString(getAofRewriteIncrementalFsync())},
		{"cluster-allow-replica-migration", boolToString(getClusterAllowReplicaMigration())},
		{"cluster-replica-validity-factor", strconv.Itoa(getClusterReplicaValidityFactor())},
		{"hash-max-listpack-entries", strconv.Itoa(getHashMaxListpackEntries())},
		{"list-max-listpack-size", strconv.Itoa(getListMaxListpackSize())},
		{"set-max-intset-entries", strconv.Itoa(getSetMaxIntsetEntries())},
		{"zset-max-listpack-entries", strconv.Itoa(getZSetMaxListpackEntries())},
		{"zset-max-listpack-value", strconv.Itoa(getZSetMaxListpackValue())},
		{"stream-node-max-bytes", strconv.FormatInt(getStreamNodeMaxBytes(), 10)},
		{"hll-sparse-max-bytes", strconv.Itoa(getHLLSparseMaxBytes())},
		{"cluster-announce-ip", getClusterAnnounceIP()},
		{"cluster-announce-port", strconv.Itoa(getClusterAnnouncePort())},
		{"cluster-announce-bus-port", strconv.Itoa(getClusterAnnounceBusPort())},
		{"stream-node-max-entries", strconv.FormatInt(getStreamNodeMaxEntries(), 10)},
		{"hash-max-listpack-value", strconv.Itoa(getHashMaxListpackValue())},
		{"set-max-listpack-entries", strconv.Itoa(getSetMaxListpackEntries())},
		{"oom-score-adj", strconv.Itoa(getOOMScoreAdj())},
		{"replicaof", getReplicaOf()},
		{"slaveof", getReplicaOf()},
		{"replica-serve-stale-data", boolToString(getReplicaServeStaleData())},
		{"replica-priority", strconv.Itoa(getReplicaPriority())},
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

func (server *Server) execConfigSet(kvPairs [][]byte) redis.Reply {
	for i := 0; i < len(kvPairs); i += 2 {
		key := strings.ToLower(string(kvPairs[i]))
		value := string(kvPairs[i+1])

		switch key {
		case "requirepass":
			config.Properties.RequirePass = value
			// Sync the default ACL user's password (Redis 6+ semantics): AUTH
			// checks aclEngine first, so requirepass alone would diverge.
			if aclEngine != nil {
				if u, ok := aclEngine.GetUser("default"); ok {
					if value == "" {
						u.ClearPasswords() // nopass
					} else {
						u.SetPassword(value, false)
					}
				}
			}
		case "appendonly":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'appendonly') - argument must be 'yes' or 'no'")
			}
			config.Properties.AppendOnly = b
			if b && server.persister == nil {
				filename := config.Properties.AppendFilename
				if filename == "" {
					filename = "appendonly.aof"
					config.Properties.AppendFilename = filename
				}
				fsync := config.Properties.AppendFsync
				if fsync == "" {
					fsync = "everysec"
				}
				p, err := NewPersister(server, filename, false, fsync)
				if err != nil {
					config.Properties.AppendOnly = false
					return protocol.MakeErrReply("ERR Failed enabling AOF: " + err.Error())
				}
				server.bindPersister(p)
			}
		case "appendfilename":
			if strings.TrimSpace(value) == "" {
				return protocol.MakeErrReply("ERR empty appendfilename")
			}
			config.Properties.AppendFilename = value
		case "appendfsync":
			v := strings.ToLower(value)
			if v != "always" && v != "everysec" && v != "no" {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'appendfsync') - argument(s) must be one of the following: everysec, always, no")
			}
			config.Properties.AppendFsync = v
			if server.persister != nil {
				server.persister.SetFsync(v)
			}
		case "aof-use-rdb-preamble":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'aof-use-rdb-preamble') - argument must be 'yes' or 'no'")
			}
			config.Properties.AofUseRdbPreamble = b
		case "masterauth":
			config.Properties.MasterAuth = value
		case "announce-host":
			config.Properties.AnnounceHost = value
		case "slave-announce-ip", "replica-announce-ip":
			config.Properties.SlaveAnnounceIP = value
		case "slave-announce-port", "replica-announce-port":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 65535 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument must be between 0 and 65535 inclusive")
			}
			config.Properties.SlaveAnnouncePort = n
		case "lua-time-limit":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'lua-time-limit') - argument couldn't be parsed into an integer")
			}
			if n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'lua-time-limit') - argument must be between 0 and 9223372036854775807 inclusive")
			}
			config.Properties.LuaTimeLimit = n
		case "maxclients":
			n64, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'maxclients') - argument couldn't be parsed into an integer")
			}
			if n64 < 1 || n64 > 4294967295 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'maxclients') - argument must be between 1 and 4294967295 inclusive")
			}
			config.Properties.MaxClients = int(n64)
		case "maxmemory":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'maxmemory') - argument must be a memory value")
			}
			config.Properties.Maxmemory = n
			server.syncMemoryConfig()
		case "maxmemory-policy":
			pol := strings.ToLower(value)
			if _, ok := memory.ParseEvictionPolicyStrict(pol); !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'maxmemory-policy') - argument(s) must be one of the following: volatile-lru, volatile-lfu, volatile-random, volatile-ttl, volatile-lrm, allkeys-lru, allkeys-lfu, allkeys-random, allkeys-lrm, noeviction")
			}
			config.Properties.MaxmemoryPolicy = pol
			server.syncMemoryConfig()
		case "timeout":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'timeout') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'timeout') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.Timeout = n
		case "tcp-keepalive":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'tcp-keepalive') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'tcp-keepalive') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.TCPKeepAlive = n
		case "loglevel":
			lv, ok := logger.ParseRedisLogLevel(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'loglevel') - argument(s) must be one of the following: debug, verbose, notice, warning, nothing")
			}
			config.Properties.LogLevel = strings.ToLower(strings.TrimSpace(value))
			if config.Properties.LogLevel == "warn" {
				config.Properties.LogLevel = "warning"
			}
			if config.Properties.LogLevel == "info" {
				config.Properties.LogLevel = "notice"
			}
			logger.SetMinLevel(lv)
		case "logfile":
			if err := logger.ReconfigureOutput(value); err != nil {
				return protocol.MakeErrReply("ERR Failed opening the logfile: " + err.Error())
			}
			config.Properties.LogFile = value
		case "protected-mode":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'protected-mode') - argument must be 'yes' or 'no'")
			}
			config.Properties.ProtectedMode = b
		case "daemonize":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR invalid daemonize value")
			}
			config.Properties.Daemonize = b
		case "pidfile":
			old := config.Properties.PidFile
			if err := config.WritePidFile(value); err != nil {
				return protocol.MakeErrReply("ERR Failed writing pidfile: " + err.Error())
			}
			if old != "" && old != value {
				_ = os.Remove(old)
			}
			config.Properties.PidFile = value
		case "aclfile":
			config.Properties.AclFile = value
		case "replica-read-only", "slave-read-only":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'replica-read-only') - argument must be 'yes' or 'no'")
			}
			config.Properties.ReplicaReadOnly = b
		case "dir":
			if value == "" {
				return protocol.MakeErrReply("ERR invalid dir value")
			}
			if err := os.MkdirAll(value, 0755); err != nil {
				return protocol.MakeErrReply("ERR Failed changing directory: " + err.Error())
			}
			if err := os.MkdirAll(filepath.Join(value, "tmp"), 0755); err != nil {
				return protocol.MakeErrReply("ERR Failed changing directory: " + err.Error())
			}
			config.Properties.Dir = value
		case "dbfilename", "rdbfilename":
			if value == "" {
				return protocol.MakeErrReply("ERR invalid dbfilename value")
			}
			config.Properties.RDBFilename = value
		case "lazyfree-lazy-eviction":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'lazyfree-lazy-eviction') - argument must be 'yes' or 'no'")
			}
			config.Properties.LazyfreeLazyEviction = b
		case "lazyfree-lazy-expire":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'lazyfree-lazy-expire') - argument must be 'yes' or 'no'")
			}
			config.Properties.LazyfreeLazyExpire = b
		case "lazyfree-lazy-server-del":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'lazyfree-lazy-server-del') - argument must be 'yes' or 'no'")
			}
			config.Properties.LazyfreeLazyServerDel = b
		case "jemalloc-bg-thread":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'jemalloc-bg-thread') - argument must be 'yes' or 'no'")
			}
			config.Properties.JemallocBgThread = b
		case "lazyfree-lazy-user-del":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'lazyfree-lazy-user-del') - argument must be 'yes' or 'no'")
			}
			config.Properties.LazyfreeLazyUserDel = b
		case "lazyfree-lazy-user-flush":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'lazyfree-lazy-user-flush') - argument must be 'yes' or 'no'")
			}
			config.Properties.LazyfreeLazyUserFlush = b
		case "replica-lazy-flush":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'replica-lazy-flush') - argument must be 'yes' or 'no'")
			}
			config.Properties.ReplicaLazyFlush = b
		case "aof-load-truncated":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'aof-load-truncated') - argument must be 'yes' or 'no'")
			}
			config.Properties.AofLoadTruncated = b
		case "activerehashing":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'activerehashing') - argument must be 'yes' or 'no'")
			}
			config.Properties.ActiveRehashing = b
		case "sanitize-dump-payload":
			v := strings.ToLower(strings.TrimSpace(value))
			if v == "clients" {
				// Redis tri-state stub: accept "clients" like yes for local sanitize flag.
				config.Properties.SanitizeDumpPayload = true
				break
			}
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'sanitize-dump-payload') - argument(s) must be one of the following: no, yes, clients")
			}
			config.Properties.SanitizeDumpPayload = b
		case "ignore-warnings":
			config.Properties.IgnoreWarnings = value
		case "replica-announced":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'replica-announced') - argument must be 'yes' or 'no'")
			}
			config.Properties.ReplicaAnnounced = b
		case "set-proc-title":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR invalid set-proc-title value")
			}
			config.Properties.SetProcTitle = b
		case "always-show-logo":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR invalid always-show-logo value")
			}
			config.Properties.AlwaysShowLogo = b
		case "lua-replicate-commands":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR invalid lua-replicate-commands value")
			}
			config.Properties.LuaReplicateCommands = b
		case "client-query-buffer-limit":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'client-query-buffer-limit') - argument must be a memory value")
			}
			if n < 1048576 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'client-query-buffer-limit') - argument must be between 1048576 and 9223372036854775807 inclusive")
			}
			config.Properties.ClientQueryBufferLimit = n
		case "client-output-buffer-limit":
			config.Properties.ClientOutputBufferLimit = value
		case "min-replicas-to-write":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'min-replicas-to-write') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'min-replicas-to-write') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.MinReplicasToWrite = n
		case "min-replicas-max-lag":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'min-replicas-max-lag') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'min-replicas-max-lag') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.MinReplicasMaxLag = n
		case "cluster-require-full-coverage":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-require-full-coverage') - argument must be 'yes' or 'no'")
			}
			config.Properties.ClusterRequireFullCoverage = b
		case "cluster-node-timeout":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-node-timeout') - argument couldn't be parsed into an integer")
			}
			if n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-node-timeout') - argument must be between 0 and 9223372036854775807 inclusive")
			}
			config.Properties.ClusterNodeTimeout = n
		case "cluster-migration-barrier":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-migration-barrier') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-migration-barrier') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.ClusterMigrationBarrier = n
		case "cluster-allow-reads-when-down":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-allow-reads-when-down') - argument must be 'yes' or 'no'")
			}
			config.Properties.ClusterAllowReadsWhenDown = b
		case "stop-writes-on-bgsave-error":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'stop-writes-on-bgsave-error') - argument must be 'yes' or 'no'")
			}
			config.Properties.StopWritesOnBgsaveError = b
		case "rdbcompression":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'rdbcompression') - argument must be 'yes' or 'no'")
			}
			config.Properties.RDBCompression = b
		case "rdbchecksum":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR invalid rdbchecksum value")
			}
			config.Properties.RDBChecksum = b
		case "no-appendfsync-on-rewrite":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'no-appendfsync-on-rewrite') - argument must be 'yes' or 'no'")
			}
			config.Properties.NoAppendFsyncOnRewrite = b
		case "auto-aof-rewrite-percentage":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'auto-aof-rewrite-percentage') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'auto-aof-rewrite-percentage') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.AutoAofRewritePercentage = n
		case "auto-aof-rewrite-min-size":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'auto-aof-rewrite-min-size') - argument must be a memory value")
			}
			config.Properties.AutoAofRewriteMinSize = n
		case "io-threads":
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.IOThreads = n
		case "io-threads-do-reads":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR invalid io-threads-do-reads value")
			}
			config.Properties.IOThreadsDoReads = b
		case "repl-diskless-sync":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'repl-diskless-sync') - argument must be 'yes' or 'no'")
			}
			config.Properties.ReplDisklessSync = b
		case "repl-diskless-sync-delay":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'repl-diskless-sync-delay') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'repl-diskless-sync-delay') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.ReplDisklessSyncDelay = n
		case "maxmemory-samples":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'maxmemory-samples') - argument couldn't be parsed into an integer")
			}
			if n < 1 || n > 64 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'maxmemory-samples') - argument must be between 1 and 64 inclusive")
			}
			config.Properties.MaxmemorySamples = n
		case "tracking-table-max-keys":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'tracking-table-max-keys') - argument couldn't be parsed into an integer")
			}
			if n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'tracking-table-max-keys') - argument must be between 0 and 9223372036854775807 inclusive")
			}
			config.Properties.TrackingTableMaxKeys = n
		case "repl-backlog-ttl":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'repl-backlog-ttl') - argument couldn't be parsed into an integer")
			}
			if n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'repl-backlog-ttl') - argument must be between 0 and 9223372036854775807 inclusive")
			}
			config.Properties.ReplBacklogTTL = n
		case "replica-ignore-maxmemory":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'replica-ignore-maxmemory') - argument must be 'yes' or 'no'")
			}
			config.Properties.ReplicaIgnoreMaxmemory = b
		case "aof-rewrite-incremental-fsync":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'aof-rewrite-incremental-fsync') - argument must be 'yes' or 'no'")
			}
			config.Properties.AofRewriteIncrementalFsync = b
		case "cluster-allow-replica-migration":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-allow-replica-migration') - argument must be 'yes' or 'no'")
			}
			config.Properties.ClusterAllowReplicaMigration = b
		case "cluster-replica-validity-factor":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-replica-validity-factor') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-replica-validity-factor') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.ClusterReplicaValidityFactor = n
		case "hash-max-listpack-entries":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'hash-max-listpack-entries') - argument couldn't be parsed into an integer")
			}
			if n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'hash-max-listpack-entries') - argument must be between 0 and 9223372036854775807 inclusive")
			}
			config.Properties.HashMaxListpackEntries = n
		case "list-max-listpack-size":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'list-max-listpack-size') - argument couldn't be parsed into an integer")
			}
			config.Properties.ListMaxListpackSize = n
		case "set-max-intset-entries":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'set-max-intset-entries') - argument couldn't be parsed into an integer")
			}
			if n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'set-max-intset-entries') - argument must be between 0 and 9223372036854775807 inclusive")
			}
			config.Properties.SetMaxIntsetEntries = n
		case "zset-max-listpack-entries":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'zset-max-listpack-entries') - argument couldn't be parsed into an integer")
			}
			if n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'zset-max-listpack-entries') - argument must be between 0 and 9223372036854775807 inclusive")
			}
			config.Properties.ZSetMaxListpackEntries = n
		case "zset-max-listpack-value":
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'zset-max-listpack-value') - argument must be a memory value")
			}
			config.Properties.ZSetMaxListpackValue = n
		case "stream-node-max-bytes":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'stream-node-max-bytes') - argument must be a memory value")
			}
			config.Properties.StreamNodeMaxBytes = n
		case "hll-sparse-max-bytes":
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'hll-sparse-max-bytes') - argument must be a memory value")
			}
			config.Properties.HLLSparseMaxBytes = n
		case "cluster-announce-ip":
			config.Properties.ClusterAnnounceIP = value
		case "cluster-announce-port":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-announce-port') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 65535 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-announce-port') - argument must be between 0 and 65535 inclusive")
			}
			config.Properties.ClusterAnnouncePort = n
		case "cluster-announce-bus-port":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-announce-bus-port') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 65535 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-announce-bus-port') - argument must be between 0 and 65535 inclusive")
			}
			config.Properties.ClusterAnnounceBusPort = n
		case "stream-node-max-entries":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'stream-node-max-entries') - argument couldn't be parsed into an integer")
			}
			if n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'stream-node-max-entries') - argument must be between 0 and 9223372036854775807 inclusive")
			}
			config.Properties.StreamNodeMaxEntries = n
		case "hash-max-listpack-value":
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'hash-max-listpack-value') - argument must be a memory value")
			}
			config.Properties.HashMaxListpackValue = n
		case "set-max-listpack-entries":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'set-max-listpack-entries') - argument couldn't be parsed into an integer")
			}
			if n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'set-max-listpack-entries') - argument must be between 0 and 9223372036854775807 inclusive")
			}
			config.Properties.SetMaxListpackEntries = n
		case "oom-score-adj":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.OOMScoreAdj = n
		case "replicaof", "slaveof":
			v := strings.TrimSpace(value)
			if strings.EqualFold(v, "no one") || v == "" {
				config.Properties.ReplicaOf = ""
			} else {
				parts := strings.Fields(v)
				if len(parts) != 2 {
					return protocol.MakeErrReply("ERR Invalid syntax for replicaof (need 'host port' or 'no one')")
				}
				if _, err := strconv.Atoi(parts[1]); err != nil {
					return protocol.MakeErrReply("ERR Invalid master port for replicaof")
				}
				config.Properties.ReplicaOf = parts[0] + " " + parts[1]
			}
		case "replica-serve-stale-data":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'replica-serve-stale-data') - argument must be 'yes' or 'no'")
			}
			config.Properties.ReplicaServeStaleData = b
		case "replica-priority":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'replica-priority') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'replica-priority') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.ReplicaPriority = n
		case "proto-max-bulk-len":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - argument must be a memory value")
			}
			if n < 1048576 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - argument must be between 1048576 and 9223372036854775807 inclusive")
			}
			config.Properties.ProtoMaxBulkLen = n
		case "save":
			if !validSaveParams(value) {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'save') - Invalid save parameters")
			}
			config.Properties.Save = value
		case "tcp-backlog":
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.TCPBacklog = n
		case "hz":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'hz') - argument couldn't be parsed into an integer")
			}
			if n < 1 || n > 500 {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.Hz = n
		case "slowlog-log-slower-than":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'slowlog-log-slower-than') - argument couldn't be parsed into an integer")
			}
			config.Properties.SlowLogSlowerThan = n
			if server.slogLogger != nil {
				server.slogLogger.SetThreshold(n)
			}
		case "slowlog-max-len":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'slowlog-max-len') - argument couldn't be parsed into an integer")
			}
			if n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'slowlog-max-len') - argument must be between 0 and 9223372036854775807 inclusive")
			}
			config.Properties.SlowLogMaxLen = n
			if server.slogLogger != nil {
				server.slogLogger.SetMaxLen(n)
			}
		case "acllog-max-len":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'acllog-max-len') - argument couldn't be parsed into an integer")
			}
			if n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'acllog-max-len') - argument must be between 0 and 9223372036854775807 inclusive")
			}
			config.Properties.AclLogMaxLen = n
			trimACLLogToMax(n)
		case "repl-timeout":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'repl-timeout') - argument couldn't be parsed into an integer")
			}
			if n < 1 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'repl-timeout') - argument must be between 1 and 2147483647 inclusive")
			}
			config.Properties.ReplTimeout = n
		case "use-gnet":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR invalid use-gnet value")
			}
			config.Properties.UseGnet = b
		case "search-backend":
			v := strings.ToLower(strings.TrimSpace(value))
			if v != "native" && v != "sqlite" {
				return protocol.MakeErrReply("ERR Invalid argument '" + value + "' for CONFIG SET 'search-backend'")
			}
			config.Properties.SearchBackend = v
		case "vector-backend":
			v := strings.ToLower(strings.TrimSpace(value))
			if v != "native" && v != "sqlite" {
				return protocol.MakeErrReply("ERR Invalid argument '" + value + "' for CONFIG SET 'vector-backend'")
			}
			config.Properties.VectorBackend = v
		case "metrics-addr":
			config.Properties.MetricsAddr = value
		case "search-sqlite-path":
			config.Properties.SearchSQLitePath = value
		case "sqlite-mmap-size":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.SqliteMmapSize = n
		case "notify-keyspace-events":
			// Redis 8.10 allowed class set (see CONFIG SET error Use '…').
			const notifyClasses = "Ag$lshzxeKEtmdnocaSTIV"
			for i := 0; i < len(value); i++ {
				if !strings.ContainsRune(notifyClasses, rune(value[i])) {
					return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'notify-keyspace-events') - Invalid event class character. Use 'Ag$lshzxeKEtmdnocaSTIV'.")
				}
			}
			config.Properties.NotifyKeyspaceEvents = value
		case "activedefrag":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'activedefrag') - argument must be 'yes' or 'no'")
			}
			config.Properties.ActiveDefrag = b
		case "busy-reply-threshold":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'busy-reply-threshold') - argument couldn't be parsed into an integer")
			}
			if n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'busy-reply-threshold') - argument must be between 0 and 9223372036854775807 inclusive")
			}
			config.Properties.BusyReplyThreshold = n
		case "dynamic-hz":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'dynamic-hz') - argument must be 'yes' or 'no'")
			}
			config.Properties.DynamicHz = b
		case "repl-backlog-size":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'repl-backlog-size') - argument must be a memory value")
			}
			if n < 1 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'repl-backlog-size') - argument must be between 1 and 9223372036854775807 inclusive")
			}
			config.Properties.ReplBacklogSize = n
		default:
			// Redis 8.0 search-* config namespace (replaces FT.CONFIG).
			if strings.HasPrefix(key, "search-") {
				if !setSearchKebab(key, value) {
					return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid argument '%s' for CONFIG SET '%s'", value, key))
				}
				continue
			}
			return protocol.MakeErrReply(fmt.Sprintf("ERR Unsupported CONFIG parameter: %s", key))
		}
	}
	return protocol.MakeOkReply()
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

func getOOMScoreAdj() int {
	if config.Properties == nil {
		return 0
	}
	return config.Properties.OOMScoreAdj
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
