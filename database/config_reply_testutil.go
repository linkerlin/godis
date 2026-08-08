package database

import (
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// configReplyValue returns the value for key from a CONFIG GET MapReply or MultiBulkReply.
func configReplyValue(r redis.Reply, key string) (string, bool) {
	switch m := r.(type) {
	case *protocol.MapReply:
		v, ok := m.Data[key]
		if !ok {
			return "", false
		}
		if b, ok := v.(*protocol.BulkReply); ok {
			return string(b.Arg), true
		}
		return string(v.ToBytes()), true
	case *protocol.MultiBulkReply:
		for i := 0; i+1 < len(m.Args); i += 2 {
			if string(m.Args[i]) == key {
				return string(m.Args[i+1]), true
			}
		}
	}
	return "", false
}

// configReplyEntries returns the number of key/value pairs in a CONFIG GET reply.
func configReplyEntries(r redis.Reply) int {
	switch m := r.(type) {
	case *protocol.MapReply:
		return len(m.Data)
	case *protocol.MultiBulkReply:
		return len(m.Args) / 2
	default:
		return 0
	}
}
