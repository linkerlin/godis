package database

import (
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/wildcard"
	"github.com/linkerlin/godis/pubsub"
	"github.com/linkerlin/godis/redis/protocol"
)

// Global sharded pub/sub hub
var shardedHub = pubsub.NewShardedHub()

// execSSubscribeConn SSUBSCRIBE with real connection.
func execSSubscribeConn(c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ssubscribe' command")
	}
	channels := make([]string, len(args))
	for i, arg := range args {
		channels[i] = string(arg)
	}
	return shardedHub.Subscribe(c, channels)
}

// execSUnsubscribeConn SUNSUBSCRIBE with real connection.
func execSUnsubscribeConn(c redis.Connection, args [][]byte) redis.Reply {
	channels := make([]string, len(args))
	for i, arg := range args {
		channels[i] = string(arg)
	}
	return shardedHub.Unsubscribe(c, channels)
}

// execSPublish publishes to a sharded channel
func execSPublish(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'spublish' command")
	}
	return protocol.MakeIntReply(int64(shardedHub.Publish(string(args[0]), args[1])))
}

// execSChannels lists sharded channels (optional glob pattern).
func execSChannels(args [][]byte) redis.Reply {
	if len(args) > 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'schannels' command")
	}
	var matcher *wildcard.Pattern
	if len(args) == 1 {
		m, err := wildcard.CompilePattern(string(args[0]))
		if err != nil {
			return protocol.MakeErrReply("ERR invalid pattern")
		}
		matcher = m
	}
	names := shardedHub.Channels()
	out := make([][]byte, 0, len(names))
	for _, name := range names {
		if matcher != nil && !matcher.IsMatch(name) {
			continue
		}
		out = append(out, []byte(name))
	}
	return protocol.MakeMultiBulkReply(out)
}

func init() {
	registerSpecialCommand("SSubscribe", -2, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagPubSub, redisFlagNoScript}, 0, 0, 0)
	registerSpecialCommand("SUnsubscribe", -1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagPubSub, redisFlagNoScript}, 0, 0, 0)
	registerCommand("SPublish", execSPublish, nil, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerSpecialCommand("SChannels", -1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
}
