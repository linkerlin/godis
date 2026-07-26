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
		resetServerStats()
		return protocol.MakeOkReply()
	case "REWRITE":
		if reply := rewriteConfigFile(); reply != nil {
			return reply
		}
		return protocol.MakeOkReply()
	case "HELP":
		return execConfigHelp(args[1:])
	default:
		return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for '%s'", subCmd))
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

// execConfigGet handles CONFIG GET
func execConfigGet(parameters [][]byte) redis.Reply {
	result := make([][]byte, 0)

	for _, param := range parameters {
		paramStr := strings.ToLower(string(param))
		matches := getConfigMatches(paramStr)
		for _, match := range matches {
			result = append(result, []byte(match.key), []byte(match.value))
		}
	}

	return protocol.MakeMultiBulkReply(result)
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
		{"proto-max-bulk-len", strconv.FormatInt(config.Properties.ProtoMaxBulkLen, 10)},
		{"save", config.Properties.Save},
		{"tcp-backlog", strconv.Itoa(config.Properties.TCPBacklog)},
		{"hz", strconv.Itoa(getServerHz())},
		{"notify-keyspace-events", config.Properties.NotifyKeyspaceEvents},
		{"activedefrag", boolToString(config.Properties.ActiveDefrag)},
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
		case "appendonly":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR invalid appendonly value")
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
				return protocol.MakeErrReply("ERR Invalid argument '" + value + "' for CONFIG SET 'appendfsync'")
			}
			config.Properties.AppendFsync = v
			if server.persister != nil {
				server.persister.SetFsync(v)
			}
		case "aof-use-rdb-preamble":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR invalid aof-use-rdb-preamble value")
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
			if err != nil || n < 0 || n > 65535 {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.SlaveAnnouncePort = n
		case "lua-time-limit":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.LuaTimeLimit = n
		case "maxclients":
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.MaxClients = n
		case "maxmemory":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.Maxmemory = n
			server.syncMemoryConfig()
		case "maxmemory-policy":
			pol := strings.ToLower(value)
			if _, ok := memory.ParseEvictionPolicyStrict(pol); !ok {
				return protocol.MakeErrReply("ERR Invalid argument '" + value + "' for CONFIG SET 'maxmemory-policy'")
			}
			config.Properties.MaxmemoryPolicy = pol
			server.syncMemoryConfig()
		case "timeout":
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.Timeout = n
		case "tcp-keepalive":
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.TCPKeepAlive = n
		case "loglevel":
			lv, ok := logger.ParseRedisLogLevel(value)
			if !ok {
				return protocol.MakeErrReply("ERR Invalid argument '" + value + "' for CONFIG SET 'loglevel'")
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
				return protocol.MakeErrReply("ERR invalid protected-mode value")
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
				return protocol.MakeErrReply("ERR invalid replica-read-only value")
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
				return protocol.MakeErrReply("ERR invalid lazyfree-lazy-eviction value")
			}
			config.Properties.LazyfreeLazyEviction = b
		case "proto-max-bulk-len":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil || n < 0 {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.ProtoMaxBulkLen = n
		case "save":
			config.Properties.Save = value
		case "tcp-backlog":
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.TCPBacklog = n
		case "hz":
			n, err := strconv.Atoi(value)
			if err != nil || n < 1 || n > 500 {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.Hz = n
		case "slowlog-log-slower-than":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.SlowLogSlowerThan = n
			if server.slogLogger != nil {
				server.slogLogger.SetThreshold(n)
			}
		case "slowlog-max-len":
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.SlowLogMaxLen = n
			if server.slogLogger != nil {
				server.slogLogger.SetMaxLen(n)
			}
		case "acllog-max-len":
			n, err := strconv.Atoi(value)
			if err != nil || n < 0 {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.AclLogMaxLen = n
			trimACLLogToMax(n)
		case "repl-timeout":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
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
			// Stub: store flags string only; keyspace notifications not implemented.
			config.Properties.NotifyKeyspaceEvents = value
		case "activedefrag":
			ok, b := config.ParseConfigBool(value)
			if !ok {
				return protocol.MakeErrReply("ERR invalid activedefrag value")
			}
			config.Properties.ActiveDefrag = b
		default:
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
