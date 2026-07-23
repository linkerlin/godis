package database

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/interface/redis"
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
		return protocol.MakeOkReply()
	case "REWRITE":
		return protocol.MakeOkReply()
	default:
		return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown subcommand '%s'", subCmd))
	}
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
		{"rdbfilename", config.Properties.RDBFilename},
		{"maxclients", strconv.Itoa(config.Properties.MaxClients)},
		{"maxmemory", strconv.FormatInt(config.Properties.Maxmemory, 10)},
		{"maxmemory-policy", config.Properties.MaxmemoryPolicy},
		{"slowlog-log-slower-than", strconv.FormatInt(config.Properties.SlowLogSlowerThan, 10)},
		{"slowlog-max-len", strconv.Itoa(config.Properties.SlowLogMaxLen)},
		{"acllog-max-len", strconv.Itoa(config.Properties.AclLogMaxLen)},
		{"cluster-enabled", boolToString(config.Properties.ClusterEnable)},
		{"repl-timeout", strconv.Itoa(config.Properties.ReplTimeout)},
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
		case "appendfsync":
			v := strings.ToLower(value)
			if v != "always" && v != "everysec" && v != "no" {
				return protocol.MakeErrReply("ERR Invalid argument '" + value + "' for CONFIG SET 'appendfsync'")
			}
			config.Properties.AppendFsync = v
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
		case "maxmemory-policy":
			config.Properties.MaxmemoryPolicy = value
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

func init() {
	registerSpecialCommand("Config", -2, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagLoading, redisFlagStale}, 0, 0, 0)
}
