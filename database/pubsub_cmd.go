package database

import (
	"strconv"
	"strings"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/wildcard"
	"github.com/linkerlin/godis/pubsub"
	"github.com/linkerlin/godis/redis/protocol"
)

// execPubsub 处理 PUBSUB 命令
func execPubsub(hub *pubsub.Hub, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'pubsub' command")
	}

	subCmd := strings.ToUpper(string(args[0]))

	switch subCmd {
	case "CHANNELS":
		return execPubsubChannels(hub, args[1:])
	case "NUMSUB":
		return execPubsubNumsub(hub, args[1:])
	case "NUMPAT":
		return execPubsubNumpat(hub)
	case "HELP":
		return execPubsubHelp()
	default:
		return protocol.MakeErrReply("ERR Unknown subcommand or wrong number of arguments for '" + subCmd + "'")
	}
}

func execPubsubChannels(hub *pubsub.Hub, args [][]byte) redis.Reply {
	pattern := "*"
	if len(args) > 0 {
		pattern = string(args[0])
	}
	match, err := wildcard.CompilePattern(pattern)
	if err != nil {
		return protocol.MakeErrReply("ERR illegal wildcard")
	}

	channels := make([][]byte, 0)
	if hub != nil {
		hub.ForEachChannel(func(channel string, n int) bool {
			if n > 0 && (pattern == "*" || match.IsMatch(channel)) {
				channels = append(channels, []byte(channel))
			}
			return true
		})
	}
	return protocol.MakeMultiBulkReply(channels)
}

func execPubsubNumsub(hub *pubsub.Hub, args [][]byte) redis.Reply {
	result := make([][]byte, 0, len(args)*2)
	for _, arg := range args {
		channel := string(arg)
		result = append(result, []byte(channel))
		n := 0
		if hub != nil {
			n = hub.NumSub(channel)
		}
		result = append(result, []byte(strconv.Itoa(n)))
	}
	return protocol.MakeMultiBulkReply(result)
}

func execPubsubNumpat(hub *pubsub.Hub) redis.Reply {
	n := 0
	if hub != nil {
		n = hub.NumPat()
	}
	return protocol.MakeIntReply(int64(n))
}

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
