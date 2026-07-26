package database

import (
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// execTime 处理 TIME 命令
// 返回当前服务器时间的 UNIX 时间戳（秒）和微秒
func execTime(args [][]byte) redis.Reply {
	now := time.Now()
	seconds := now.Unix()
	microseconds := int64(now.Nanosecond()) / 1000

	result := [][]byte{
		[]byte(strconv.FormatInt(seconds, 10)),
		[]byte(strconv.FormatInt(microseconds, 10)),
	}
	return protocol.MakeMultiBulkReply(result)
}

// execLolwut returns a short Redis-compatible easter-egg banner.
// LOLWUT [VERSION version]
func execLolwut(args [][]byte) redis.Reply {
	ver := 0
	if len(args) > 0 {
		if len(args) != 2 || !strings.EqualFold(string(args[0]), "VERSION") {
			return protocol.MakeSyntaxErrReply()
		}
		n, err := strconv.Atoi(string(args[1]))
		if err != nil || n < 0 {
			return protocol.MakeErrReply("ERR Invalid version")
		}
		ver = n
	}
	msg := "Godis ver. redis-compat"
	if ver > 0 {
		msg += " (LOLWUT style " + strconv.Itoa(ver) + ")"
	}
	msg += "\n" +
		"A Go Redis-compatible server.\n" +
		"For more information visit https://github.com/linkerlin/godis\n"
	return protocol.MakeBulkReply([]byte(msg))
}

func init() {
	registerSpecialCommand("Time", 1, 0).
		attachCommandExtra([]string{redisFlagRandom, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("Role", 1, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript, redisFlagLoading, redisFlagStale}, 0, 0, 0)
	registerSpecialCommand("Lolwut", -1, 0).
		attachCommandExtra([]string{redisFlagReadonly, redisFlagLoading, redisFlagStale}, 0, 0, 0)
}
