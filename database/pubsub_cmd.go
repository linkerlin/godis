package database

import (
	"strings"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// execPubsub 处理 PUBSUB 命令
// PUBSUB CHANNELS [pattern] - 列出活跃的频道
// PUBSUB NUMSUB [channel ...] - 返回频道的订阅数
// PUBSUB NUMPAT - 返回模式订阅数
// PUBSUB HELP - 获取帮助
func execPubsub(args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'pubsub' command")
	}

	subCmd := strings.ToUpper(string(args[0]))

	switch subCmd {
	case "CHANNELS":
		return execPubsubChannels(args[1:])
	case "NUMSUB":
		return execPubsubNumsub(args[1:])
	case "NUMPAT":
		return execPubsubNumpat()
	case "HELP":
		return execPubsubHelp()
	default:
		return protocol.MakeErrReply("ERR Unknown subcommand or wrong number of arguments for '" + subCmd + "'")
	}
}

// execPubsubChannels 列出活跃的频道
func execPubsubChannels(args [][]byte) redis.Reply {
	// 获取所有订阅频道
	// 注意：由于 hub 结构限制，这里返回空数组
	// 实际实现需要遍历 hub.subs
	return protocol.MakeEmptyMultiBulkReply()
}

// execPubsubNumsub 返回指定频道的订阅数
func execPubsubNumsub(args [][]byte) redis.Reply {
	result := make([][]byte, 0, len(args)*2)
	
	for _, arg := range args {
		channel := string(arg)
		result = append(result, []byte(channel))
		// 由于没有直接访问 hub 的方法，返回 0
		result = append(result, []byte("0"))
	}
	
	return protocol.MakeMultiBulkReply(result)
}

// execPubsubNumpat 返回模式订阅数
func execPubsubNumpat() redis.Reply {
	// 当前不支持模式订阅计数
	return protocol.MakeIntReply(0)
}

// execPubsubHelp 获取帮助信息
func execPubsubHelp() redis.Reply {
	help := []string{
		"PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
		"CHANNELS [<pattern>]",
		"    Return the currently active channels matching the pattern.",
		"NUMSUB [channel [channel ...]]",
		"    Return the number of subscribers for the specified channels.",
		"NUMPAT",
		"    Return the total number of unique pattern subscriptions.",
		"HELP",
		"    Print this help.",
	}

	result := make([]redis.Reply, len(help))
	for i, h := range help {
		result[i] = protocol.MakeBulkReply([]byte(h))
	}
	return protocol.MakeMultiRawReply(result)
}

func init() {
	registerSpecialCommand("Pubsub", -2, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagRandom, redisFlagStale}, 0, 0, 0)
}
