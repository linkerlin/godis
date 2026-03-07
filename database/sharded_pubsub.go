package database

import (
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/pubsub"
	"github.com/hdt3213/godis/redis/protocol"
)

// Global sharded pub/sub hub
var shardedHub = pubsub.NewShardedHub()

// execSSubscribe subscribes to sharded channels
// SSUBSCRIBE channel [channel ...]
func execSSubscribe(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'ssubscribe' command")
	}
	
	channels := make([]string, len(args))
	for i, arg := range args {
		channels[i] = string(arg)
	}
	
	// Get current connection from context (simplified)
	// In real implementation, connection should be passed
	return shardedHub.Subscribe(nil, channels)
}

// execSUnsubscribe unsubscribes from sharded channels
// SUNSUBSCRIBE [channel [channel ...]]
func execSUnsubscribe(db *DB, args [][]byte) redis.Reply {
	channels := make([]string, len(args))
	for i, arg := range args {
		channels[i] = string(arg)
	}
	
	return shardedHub.Unsubscribe(nil, channels)
}

// execSPublish publishes to a sharded channel
// SPUBLISH channel message
func execSPublish(db *DB, args [][]byte) redis.Reply {
	if len(args) != 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'spublish' command")
	}
	
	channel := string(args[0])
	message := args[1]
	
	receivers := shardedHub.Publish(channel, message)
	
	return protocol.MakeIntReply(int64(receivers))
}

// execSChannel returns sharded channel info
// SCHANNELS [pattern]
func execSChannels(args [][]byte) redis.Reply {
	// Simplified: return empty list
	// In real implementation, would list sharded channels
	return protocol.MakeEmptyMultiBulkReply()
}

// execSUnsubscribeCmd is the command handler
func execSUnsubscribeCmd(db *DB, args [][]byte) redis.Reply {
	return execSUnsubscribe(db, args)
}

// execSChannelsCmd is the command handler  
func execSChannelsCmd(db *DB, args [][]byte) redis.Reply {
	return execSChannels(args)
}

func init() {
	// Note: These commands would need proper connection handling
	// For now, they're registered but simplified
	registerCommand("SSubscribe", execSSubscribe, nil, nil, -2, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagPubSub}, 0, 0, 0)
	registerCommand("SUnsubscribe", execSUnsubscribeCmd, nil, nil, -1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagPubSub}, 0, 0, 0)
	registerCommand("SPublish", execSPublish, nil, nil, 3, flagWrite).
		attachCommandExtra([]string{redisFlagWrite}, 0, 0, 0)
	registerCommand("SChannels", execSChannelsCmd, nil, nil, -1, flagAdmin).
		attachCommandExtra([]string{redisFlagAdmin}, 0, 0, 0)
}


