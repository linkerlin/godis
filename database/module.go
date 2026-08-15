package database

import (
	"strings"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// execModule 处理 MODULE 命令
// MODULE LIST - 列出所有加载的模块
// MODULE LOAD path [arg ...] - 加载模块
// MODULE UNLOAD name - 卸载模块
// MODULE HELP - 获取帮助
func execModule(args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'module' command")
	}

	subCmd := strings.ToUpper(string(args[0]))

	switch subCmd {
	case "LIST":
		if len(args) != 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'module|list' command")
		}
		return execModuleList()
	case "LOAD":
		return execModuleLoad(args[1:])
	case "LOADEX":
		return execModuleLoadEx(args[1:])
	case "UNLOAD":
		return execModuleUnload(args[1:])
	case "HELP":
		return execModuleHelp()
	default:
		return protocol.MakeErrReply("ERR Unknown subcommand or wrong number of arguments for '" + subCmd + "'. Try MODULE HELP.")
	}
}

// execModuleList lists loaded modules.
// RESP3 would be an array of Maps {name, ver}; RESP2 flat field arrays per module.
// Godis has no dynamically loaded modules — returns an empty array.
func execModuleList() redis.Reply {
	return protocol.MakeEmptyMultiBulkReply()
}

// execModuleLoad 加载模块
func execModuleLoad(args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'module|load' command")
	}

	// Godis 不支持动态加载外部模块
	// 所有功能都是内置的
	return protocol.MakeErrReply("ERR Godis does not support dynamic module loading. All features are built-in.")
}

// execModuleLoadEx MODULE LOADEX path [CONFIG name value ...] [ARGS ...] — reject like LOAD.
func execModuleLoadEx(args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'module|loadex' command")
	}
	return protocol.MakeErrReply("ERR Godis does not support dynamic module loading. All features are built-in.")
}

// execModuleUnload 卸载模块
func execModuleUnload(args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'module|unload' command")
	}

	// Godis 不支持卸载模块
	return protocol.MakeErrReply("ERR Godis does not support module unloading.")
}

// execModuleHelp 获取帮助信息
func execModuleHelp() redis.Reply {
	help := []string{
		"MODULE LIST - Return all loaded modules.",
		"MODULE LOAD <path> [arg ...] - Load a module (not supported in Godis).",
		"MODULE LOADEX <path> [CONFIG name value ...] [ARGS ...] - Load a module with configs (not supported in Godis).",
		"MODULE UNLOAD <name> - Unload a module (not supported in Godis).",
		"MODULE HELP - Display this help text.",
	}

	result := make([]redis.Reply, len(help))
	for i, h := range help {
		result[i] = protocol.MakeBulkReply([]byte(h))
	}
	return protocol.MakeMultiRawReply(result)
}

func init() {
	registerSpecialCommand("Module", -2, 0).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
}
