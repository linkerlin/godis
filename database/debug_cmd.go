package database

import (
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// execDebug handles DEBUG subcommands (minimal Redis-compatible subset).
// DEBUG SLEEP <seconds>
func execDebug(args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'debug' command")
	}
	sub := strings.ToUpper(string(args[0]))
	switch sub {
	case "SLEEP":
		if len(args) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'debug|sleep' command")
		}
		sec, err := strconv.ParseFloat(string(args[1]), 64)
		if err != nil || sec < 0 {
			return protocol.MakeErrReply("ERR invalid sleep time")
		}
		if sec > 0 {
			time.Sleep(time.Duration(sec * float64(time.Second)))
		}
		return protocol.MakeOkReply()
	case "HELP":
		help := []string{
			"DEBUG <subcommand> [<arg> [value] [opt] ...]",
			"Subcommands:",
			"SLEEP <seconds>",
			"    Sleep for the specified number of seconds (float allowed).",
			"HELP",
			"    Print this help.",
		}
		out := make([][]byte, len(help))
		for i, h := range help {
			out[i] = []byte(h)
		}
		return protocol.MakeMultiBulkReply(out)
	default:
		return protocol.MakeErrReply("ERR Unknown DEBUG subcommand or wrong number of arguments for '" + sub + "'. Try DEBUG HELP.")
	}
}

func init() {
	registerSpecialCommand("Debug", -2, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript, redisFlagLoading}, 0, 0, 0)
}
