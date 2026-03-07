package database

import (
	"strconv"
	"time"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
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

func init() {
	registerSpecialCommand("Time", 1, 0).
		attachCommandExtra([]string{redisFlagRandom, redisFlagLoading, redisFlagStale}, 0, 0, 0)
}
