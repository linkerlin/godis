// CONFIG SET executor: the big per-key switch and its immutable-key rejection.
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
	"github.com/linkerlin/godis/tcp"
)

func (server *Server) execConfigSet(kvPairs [][]byte) redis.Reply {
	for i := 0; i < len(kvPairs); i += 2 {
		key := strings.ToLower(string(kvPairs[i]))
		value := string(kvPairs[i+1])

		switch key {
		case "requirepass":
			config.Properties.RequirePass = value
			// Sync the default ACL user's password (Redis 6+ semantics): AUTH
			// checks aclEngine first, so requirepass alone would diverge.
			if aclEngine.Load() != nil {
				if u, ok := aclEngine.Load().GetUser("default"); ok {
					if value == "" {
						u.ClearPasswords() // nopass
					} else {
						u.SetPassword(value, false)
					}
				}
			}
		case "masteruser":
			config.Properties.MasterUser = value
		case "port":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'port') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 65535 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'port') - argument must be between 0 and 65535 inclusive")
			}
			// Stub: update reported port only; do not rebind listeners.
			config.Properties.Port = n
		case "bind":
			if errMsg := validateBindAddresses(value); errMsg != "" {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'bind') - " + errMsg)
			}
			// Stub: update reported bind only; do not rebind listeners.
			config.Properties.Bind = value
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
			tcp.SetMaxClients(int(n64))
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
		case "log-format":
			format := strings.ToLower(strings.TrimSpace(value))
			if format != "text" && format != "json" {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'log-format') - argument must be 'text' or 'json'")
			}
			if err := logger.SetFormat(format); err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'log-format') - " + err.Error())
			}
			config.Properties.LogFormat = format
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
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument must be 'yes' or 'no'")
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
		case "replica-lazy-flush", "slave-lazy-flush":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument must be 'yes' or 'no'")
			}
			config.Properties.ReplicaLazyFlush = b
		case "aof-load-truncated":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'aof-load-truncated') - argument must be 'yes' or 'no'")
			}
			config.Properties.AofLoadTruncated = b
		case "rdb-del-sync-files":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'rdb-del-sync-files') - argument must be 'yes' or 'no'")
			}
			config.Properties.RdbDelSyncFiles = b
		case "aof-disable-auto-gc":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'aof-disable-auto-gc') - argument must be 'yes' or 'no'")
			}
			config.Properties.AofDisableAutoGC = b
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
			if errMsg := validateClientOutputBufferLimit(value); errMsg != "" {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'client-output-buffer-limit') - " + errMsg)
			}
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
		case "repl-diskless-sync-max-replicas":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'repl-diskless-sync-max-replicas') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'repl-diskless-sync-max-replicas') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.ReplDisklessSyncMaxReplicas = n
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
		case "replica-ignore-maxmemory", "slave-ignore-maxmemory":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument must be 'yes' or 'no'")
			}
			config.Properties.ReplicaIgnoreMaxmemory = b
		case "aof-rewrite-incremental-fsync":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'aof-rewrite-incremental-fsync') - argument must be 'yes' or 'no'")
			}
			config.Properties.AofRewriteIncrementalFsync = b
		case "rdb-save-incremental-fsync":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'rdb-save-incremental-fsync') - argument must be 'yes' or 'no'")
			}
			config.Properties.RdbSaveIncrementalFsync = b
		case "latency-monitor-threshold":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'latency-monitor-threshold') - argument couldn't be parsed into an integer")
			}
			if n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'latency-monitor-threshold') - argument must be between 0 and 9223372036854775807 inclusive")
			}
			config.Properties.LatencyMonitorThreshold = n
		case "acl-pubsub-default":
			v := strings.ToLower(value)
			if v != "allchannels" && v != "resetchannels" {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'acl-pubsub-default') - argument(s) must be one of the following: allchannels, resetchannels")
			}
			config.Properties.AclPubsubDefault = v
		case "repl-ping-replica-period":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'repl-ping-replica-period') - argument couldn't be parsed into an integer")
			}
			if n < 1 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'repl-ping-replica-period') - argument must be between 1 and 2147483647 inclusive")
			}
			config.Properties.ReplPingReplicaPeriod = n
		case "lfu-log-factor":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'lfu-log-factor') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'lfu-log-factor') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.LFULogFactor = n
		case "lfu-decay-time":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'lfu-decay-time') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'lfu-decay-time') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.LFUDecayTime = n
		case "maxmemory-eviction-tenacity":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'maxmemory-eviction-tenacity') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 100 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'maxmemory-eviction-tenacity') - argument must be between 0 and 100 inclusive")
			}
			config.Properties.MaxmemoryEvictionTenacity = n
		case "list-compress-depth":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'list-compress-depth') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'list-compress-depth') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.ListCompressDepth = n
		case "aof-timestamp-enabled":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'aof-timestamp-enabled') - argument must be 'yes' or 'no'")
			}
			config.Properties.AofTimestampEnabled = b
		case "repl-disable-tcp-nodelay":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'repl-disable-tcp-nodelay') - argument must be 'yes' or 'no'")
			}
			config.Properties.ReplDisableTCPNodelay = b
		case "latency-tracking":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'latency-tracking') - argument must be 'yes' or 'no'")
			}
			config.Properties.LatencyTracking = b
		case "crash-log-enabled":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'crash-log-enabled') - argument must be 'yes' or 'no'")
			}
			config.Properties.CrashLogEnabled = b
		case "crash-memcheck-enabled":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'crash-memcheck-enabled') - argument must be 'yes' or 'no'")
			}
			config.Properties.CrashMemcheckEnabled = b
		case "repl-diskless-load":
			v := strings.ToLower(value)
			switch v {
			case "disabled", "on-empty-db", "swapdb", "flushdb":
				config.Properties.ReplDisklessLoad = v
			default:
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'repl-diskless-load') - argument(s) must be one of the following: disabled, on-empty-db, swapdb, flushdb")
			}
		case "cluster-preferred-endpoint-type":
			v := strings.ToLower(value)
			switch v {
			case "ip", "hostname", "unknown-endpoint":
				config.Properties.ClusterPreferredEndpointType = v
			default:
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-preferred-endpoint-type') - argument(s) must be one of the following: ip, hostname, unknown-endpoint")
			}
		case "cluster-link-sendbuf-limit":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-link-sendbuf-limit') - argument must be a memory value")
			}
			config.Properties.ClusterLinkSendbufLimit = n
		case "cluster-announce-hostname":
			config.Properties.ClusterAnnounceHostname = value
		case "cluster-announce-human-nodename":
			config.Properties.ClusterAnnounceHumanNodename = value
		case "shutdown-timeout":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'shutdown-timeout') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'shutdown-timeout') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.ShutdownTimeout = n
		case "shutdown-on-sigint":
			v := strings.ToLower(value)
			switch v {
			case "default", "save", "nosave", "now", "force":
				config.Properties.ShutdownOnSigint = v
			default:
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'shutdown-on-sigint') - argument(s) must be one of the following: default, save, nosave, now, force")
			}
		case "shutdown-on-sigterm":
			v := strings.ToLower(value)
			switch v {
			case "default", "save", "nosave", "now", "force":
				config.Properties.ShutdownOnSigterm = v
			default:
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'shutdown-on-sigterm') - argument(s) must be one of the following: default, save, nosave, now, force")
			}
		case "locale-collate":
			config.Properties.LocaleCollate = value
		case "latency-tracking-info-percentiles":
			normalized, errMsg := validateLatencyTrackingInfoPercentiles(value)
			if errMsg != "" {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'latency-tracking-info-percentiles') - " + errMsg)
			}
			config.Properties.LatencyTrackingInfoPercentiles = normalized
		case "active-defrag-ignore-bytes":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'active-defrag-ignore-bytes') - argument must be a memory value")
			}
			if n < 1 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'active-defrag-ignore-bytes') - argument must be between 1 and 9223372036854775807 inclusive")
			}
			config.Properties.ActiveDefragIgnoreBytes = n
		case "active-defrag-threshold-lower":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'active-defrag-threshold-lower') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 1000 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'active-defrag-threshold-lower') - argument must be between 0 and 1000 inclusive")
			}
			config.Properties.ActiveDefragThresholdLower = n
		case "active-defrag-threshold-upper":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'active-defrag-threshold-upper') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 1000 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'active-defrag-threshold-upper') - argument must be between 0 and 1000 inclusive")
			}
			config.Properties.ActiveDefragThresholdUpper = n
		case "active-defrag-cycle-min":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'active-defrag-cycle-min') - argument couldn't be parsed into an integer")
			}
			if n < 1 || n > 99 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'active-defrag-cycle-min') - argument must be between 1 and 99 inclusive")
			}
			config.Properties.ActiveDefragCycleMin = n
		case "active-defrag-cycle-max":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'active-defrag-cycle-max') - argument couldn't be parsed into an integer")
			}
			if n < 1 || n > 99 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'active-defrag-cycle-max') - argument must be between 1 and 99 inclusive")
			}
			config.Properties.ActiveDefragCycleMax = n
		case "active-defrag-max-scan-fields":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'active-defrag-max-scan-fields') - argument couldn't be parsed into an integer")
			}
			if n < 1 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'active-defrag-max-scan-fields') - argument must be between 1 and 9223372036854775807 inclusive")
			}
			config.Properties.ActiveDefragMaxScanFields = n
		case "oom-score-adj-values":
			if errMsg := validateOOMScoreAdjValues(value); errMsg != "" {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'oom-score-adj-values') - " + errMsg)
			}
			config.Properties.OOMScoreAdjValues = strings.Join(strings.Fields(value), " ")
		case "propagation-error-behavior":
			v := strings.ToLower(value)
			switch v {
			case "ignore", "panic", "panic-on-replicas":
				config.Properties.PropagationErrorBehavior = v
			default:
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'propagation-error-behavior') - argument(s) must be one of the following: ignore, panic, panic-on-replicas")
			}
		case "hide-user-data-from-log":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'hide-user-data-from-log') - argument must be 'yes' or 'no'")
			}
			config.Properties.HideUserDataFromLog = b
		case "cluster-replica-no-failover", "cluster-slave-no-failover":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument must be 'yes' or 'no'")
			}
			config.Properties.ClusterReplicaNoFailover = b
		case "cluster-allow-pubsubshard-when-down":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-allow-pubsubshard-when-down') - argument must be 'yes' or 'no'")
			}
			config.Properties.ClusterAllowPubsubshardWhenDown = b
		case "proc-title-template":
			if errMsg := validateProcTitleTemplate(value); errMsg != "" {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'proc-title-template') - " + errMsg)
			}
			config.Properties.ProcTitleTemplate = value
		case "active-expire-effort":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'active-expire-effort') - argument couldn't be parsed into an integer")
			}
			if n < 1 || n > 10 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'active-expire-effort') - argument must be between 1 and 10 inclusive")
			}
			config.Properties.ActiveExpireEffort = n
		case "tls-cert-file":
			config.Properties.TLSCertFile = value
		case "tls-key-file":
			config.Properties.TLSKeyFile = value
		case "tls-ca-cert-file":
			config.Properties.TLSCACertFile = value
		case "tls-protocols":
			config.Properties.TLSProtocols = value
		case "tls-ciphers":
			config.Properties.TLSCiphers = value
		case "tls-auth-clients":
			v := strings.ToLower(value)
			switch v {
			case "no", "yes", "optional":
				config.Properties.TLSAuthClients = v
			default:
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'tls-auth-clients') - argument(s) must be one of the following: no, yes, optional")
			}
		case "tls-replication":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'tls-replication') - argument must be 'yes' or 'no'")
			}
			config.Properties.TLSReplication = b
		case "tls-cluster":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'tls-cluster') - argument must be 'yes' or 'no'")
			}
			config.Properties.TLSCluster = b
		case "tls-session-caching":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'tls-session-caching') - argument must be 'yes' or 'no'")
			}
			config.Properties.TLSSessionCaching = b
		case "tls-session-cache-size":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'tls-session-cache-size') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'tls-session-cache-size') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.TLSSessionCacheSize = n
		case "tls-session-cache-timeout":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'tls-session-cache-timeout') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'tls-session-cache-timeout') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.TLSSessionCacheTimeout = n
		case "tls-prefer-server-ciphers":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'tls-prefer-server-ciphers') - argument must be 'yes' or 'no'")
			}
			config.Properties.TLSPreferServerCiphers = b
		case "cluster-announce-tls-port":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-announce-tls-port') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 65535 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-announce-tls-port') - argument must be between 0 and 65535 inclusive")
			}
			config.Properties.ClusterAnnounceTLSPort = n
		case "tls-port":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'tls-port') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 65535 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'tls-port') - argument must be between 0 and 65535 inclusive")
			}
			config.Properties.TLSPort = n
		case "tls-dh-params-file":
			config.Properties.TLSDhParamsFile = value
		case "tls-ciphersuites":
			config.Properties.TLSCiphersuites = value
		case "tls-client-cert-file":
			config.Properties.TLSClientCertFile = value
		case "tls-client-key-file":
			config.Properties.TLSClientKeyFile = value
		case "tls-key-file-pass":
			config.Properties.TLSKeyFilePass = value
		case "tls-client-key-file-pass":
			config.Properties.TLSClientKeyFilePass = value
		case "maxmemory-clients":
			if errMsg := validateMaxmemoryClients(value); errMsg != "" {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'maxmemory-clients') - " + errMsg)
			}
			config.Properties.MaxmemoryClients = value
		case "cluster-allow-replica-migration":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'cluster-allow-replica-migration') - argument must be 'yes' or 'no'")
			}
			config.Properties.ClusterAllowReplicaMigration = b
		case "cluster-replica-validity-factor", "cluster-slave-validity-factor":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.ClusterReplicaValidityFactor = n
		case "hash-max-listpack-entries", "hash-max-ziplist-entries":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument couldn't be parsed into an integer")
			}
			if n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument must be between 0 and 9223372036854775807 inclusive")
			}
			config.Properties.HashMaxListpackEntries = n
		case "list-max-listpack-size", "list-max-ziplist-size":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument couldn't be parsed into an integer")
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
		case "zset-max-listpack-entries", "zset-max-ziplist-entries":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument couldn't be parsed into an integer")
			}
			if n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument must be between 0 and 9223372036854775807 inclusive")
			}
			config.Properties.ZSetMaxListpackEntries = n
		case "zset-max-listpack-value", "zset-max-ziplist-value":
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument must be a memory value")
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
		case "stream-idmp-duration":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'stream-idmp-duration') - argument must be a non-negative integer")
			}
			config.Properties.StreamIdmpDuration = n
		case "stream-idmp-maxsize":
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'stream-idmp-maxsize') - argument must be a non-negative integer")
			}
			config.Properties.StreamIdmpMaxSize = n
		case "hash-max-listpack-value", "hash-max-ziplist-value":
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument must be a memory value")
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
			v := strings.ToLower(value)
			switch v {
			case "no", "yes", "absolute":
				config.Properties.OOMScoreAdj = v
			case "relative":
				// Redis reports relative as yes on CONFIG GET.
				config.Properties.OOMScoreAdj = "yes"
			default:
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'oom-score-adj') - argument(s) must be one of the following: no, yes, relative, absolute")
			}
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
		case "replica-serve-stale-data", "slave-serve-stale-data":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument must be 'yes' or 'no'")
			}
			config.Properties.ReplicaServeStaleData = b
		case "replica-priority", "slave-priority":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument couldn't be parsed into an integer")
			}
			if n < 0 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument '" + key + "') - argument must be between 0 and 2147483647 inclusive")
			}
			config.Properties.ReplicaPriority = n
		case "set-max-listpack-value":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'set-max-listpack-value') - argument couldn't be parsed into an integer")
			}
			if n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'set-max-listpack-value') - argument must be between 0 and 9223372036854775807 inclusive")
			}
			config.Properties.SetMaxListpackValue = n
		case "proto-max-bulk-len":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - argument must be a memory value")
			}
			if n < 1048576 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'proto-max-bulk-len') - argument must be between 1048576 and 9223372036854775807 inclusive")
			}
			config.Properties.ProtoMaxBulkLen = n
			config.SetProtoMaxBulkLen(n)
		case "save":
			if !validSaveParams(value) {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'save') - Invalid save parameters")
			}
			config.Properties.Save = value
			config.SetSaveConfig(value)
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
		case "slowlog-entry-max-argc":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'slowlog-entry-max-argc') - argument couldn't be parsed into an integer")
			}
			if n < 2 || n > 2147483647 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'slowlog-entry-max-argc') - argument must be between 2 and 2147483647 inclusive")
			}
			config.Properties.SlowLogEntryMaxArgc = n
			if server.slogLogger != nil {
				server.slogLogger.SetEntryMaxArgc(n)
			}
		case "slowlog-entry-max-string-len":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'slowlog-entry-max-string-len') - argument couldn't be parsed into an integer")
			}
			if n < 1 {
				return protocol.MakeErrReply("ERR CONFIG SET failed (possibly related to argument 'slowlog-entry-max-string-len') - argument must be between 1 and 9223372036854775807 inclusive")
			}
			config.Properties.SlowLogEntryMaxStringLen = n
			if server.slogLogger != nil {
				server.slogLogger.SetEntryMaxString(int(n))
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
		case "databases", "disable-thp", "enable-debug-command", "enable-module-command",
			"enable-protected-configs", "socket-mark-id", "unixsocket", "unixsocketperm",
			"syslog-enabled", "syslog-ident", "syslog-facility", "cluster-port",
			"cluster-config-file", "supervised", "appenddirname":
			return immutableConfigSetErr(key)
		default:
			// Redis 8.0 search-* config namespace (replaces FT.CONFIG).
			if strings.HasPrefix(key, "search-") {
				if !setSearchKebab(key, value) {
					return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid argument '%s' for CONFIG SET '%s'", value, key))
				}
				continue
			}
			return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown option or number of arguments for CONFIG SET - '%s'", key))
		}
	}
	return protocol.MakeOkReply()
}

func immutableConfigSetErr(key string) redis.Reply {
	return protocol.MakeErrReply(fmt.Sprintf("ERR CONFIG SET failed (possibly related to argument '%s') - can't set immutable config", key))
}
