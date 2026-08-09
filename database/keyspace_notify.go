package database

import (
	"fmt"
	"strings"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/pubsub"
)

// notifyKeyspaceEvent publishes Redis-style keyspace/keyevent notifications
// when CONFIG notify-keyspace-events is non-empty.
//
// Supported flag letters (subset):
//
//	K — __keyspace@<db>__:<key>  → event
//	E — __keyevent@<db>__:<event> → key
//	g — generic (del, expire, rename, …)
//	$ — string (set, …)
//	l/h/s/z — list/hash/set/zset
//	x — expired
//	e — evicted
//	A — all of the above we implement
func notifyKeyspaceEvent(db *DB, event, key string) {
	if db == nil || db.server == nil || db.server.hub == nil {
		return
	}
	flags := ""
	if config.Properties != nil {
		flags = config.Properties.NotifyKeyspaceEvents
	}
	if flags == "" || !keyspaceFlagsAllow(flags, event) {
		return
	}
	dbIndex := db.index
	if strings.ContainsAny(flags, "KA") {
		channel := fmt.Sprintf("__keyspace@%d__:%s", dbIndex, key)
		_ = pubsub.Publish(db.server.hub, [][]byte{[]byte(channel), []byte(event)})
	}
	if strings.ContainsAny(flags, "EA") {
		channel := fmt.Sprintf("__keyevent@%d__:%s", dbIndex, event)
		_ = pubsub.Publish(db.server.hub, [][]byte{[]byte(channel), []byte(key)})
	}
}

func keyspaceFlagsAllow(flags, event string) bool {
	if strings.Contains(flags, "A") {
		return true
	}
	switch event {
	case "expired":
		return strings.ContainsAny(flags, "gx")
	case "evicted":
		return strings.ContainsAny(flags, "ge")
	case "del", "expire", "persist", "rename_from", "rename_to":
		return strings.Contains(flags, "g")
	case "set", "setrange", "incrby", "append":
		return strings.Contains(flags, "$")
	case "lpush", "rpush", "lpop", "rpop", "lset", "ltrim":
		return strings.Contains(flags, "l")
	case "hset", "hdel", "hincrby", "hexpire", "hexpired", "hpersist":
		return strings.Contains(flags, "h")
	case "sadd", "srem", "spop":
		return strings.Contains(flags, "s")
	case "zadd", "zrem", "zincrby":
		return strings.Contains(flags, "z")
	default:
		return strings.Contains(flags, "g")
	}
}
