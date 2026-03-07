package database

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hdt3213/godis/config"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// execConfig 处理 CONFIG 命令
// CONFIG GET parameter [parameter ...]
// CONFIG SET parameter value [parameter value ...]
// CONFIG RESETSTAT
// CONFIG REWRITE
func execConfig(args [][]byte) redis.Reply {
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
		return execConfigSet(args[1:])
	case "RESETSTAT":
		// 重置统计信息 - 目前只是返回 OK
		return protocol.MakeOkReply()
	case "REWRITE":
		// 重写配置文件 - 目前只是返回 OK
		return protocol.MakeOkReply()
	default:
		return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown subcommand '%s'", subCmd))
	}
}

// execConfigGet 处理 CONFIG GET 命令
func execConfigGet(parameters [][]byte) redis.Reply {
	result := make([][]byte, 0)

	for _, param := range parameters {
		paramStr := strings.ToLower(string(param))
		// 支持通配符模式匹配
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

// getConfigMatches 根据模式获取配置项
func getConfigMatches(pattern string) []configPair {
	matches := make([]configPair, 0)
	
	// 支持的配置项
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
		{"slowlog-log-slower-than", strconv.FormatInt(config.Properties.SlowLogSlowerThan, 10)},
		{"slowlog-max-len", strconv.Itoa(config.Properties.SlowLogMaxLen)},
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

// patternMatch 简单的通配符匹配
func patternMatch(pattern, str string) bool {
	// 完全匹配
	if pattern == str {
		return true
	}
	// * 匹配所有
	if pattern == "*" {
		return true
	}
	// 前缀* 模式
	if strings.HasSuffix(pattern, "*") {
		prefix := pattern[:len(pattern)-1]
		return strings.HasPrefix(str, prefix)
	}
	// 后缀* 模式
	if strings.HasPrefix(pattern, "*") {
		suffix := pattern[1:]
		return strings.HasSuffix(str, suffix)
	}
	return false
}

// execConfigSet 处理 CONFIG SET 命令
func execConfigSet(kvPairs [][]byte) redis.Reply {
	for i := 0; i < len(kvPairs); i += 2 {
		key := strings.ToLower(string(kvPairs[i]))
		value := string(kvPairs[i+1])

		switch key {
		case "requirepass":
			config.Properties.RequirePass = value
		case "slowlog-log-slower-than":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.SlowLogSlowerThan = n
		case "slowlog-max-len":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.SlowLogMaxLen = n
		case "repl-timeout":
			n, err := strconv.Atoi(value)
			if err != nil {
				return protocol.MakeErrReply(fmt.Sprintf("ERR Invalid value for '%s'", key))
			}
			config.Properties.ReplTimeout = n
		default:
			// 其他配置项不支持运行时修改
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
