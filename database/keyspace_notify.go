package database

import (
	"fmt"
	"strconv"
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
	case "hset", "hdel", "hincrby", "hincrbyfloat", "hexpire", "hpersist":
		return strings.Contains(flags, "h")
	case "hexpired":
		return strings.ContainsAny(flags, "hx")
	case "sadd", "srem", "spop":
		return strings.Contains(flags, "s")
	case "zadd", "zrem", "zincrby":
		return strings.Contains(flags, "z")
	default:
		return strings.Contains(flags, "g")
	}
}

// notifySubkeyEvent publishes Redis 8.8 subkey notifications for a key-level
// event that names the affected subkeys (hash fields).
//
// Channel formats (see Redis "subkey notifications" docs):
//
//	S — __subkeyspace@<db>__:<key>              → <event>|<len>:<subkey>[,...]
//	T — __subkeyevent@<db>__:<event>            → <len>:<key>|<len>:<subkey>[,...]
//	I — __subkeyspaceitem@<db>__:<key>\n<subkey> → <event>
//	V — __subkeyspaceevent@<db>__:<event>|<key> → <len>:<subkey>[,...]
//
// Subkey notifications are independent from the key-level K/E flags and from
// the A alias; they are enabled only by the S/T/I/V characters.
func notifySubkeyEvent(db *DB, event, key string, subkeys []string) {
	if db == nil || db.server == nil || db.server.hub == nil || len(subkeys) == 0 {
		return
	}
	flags := ""
	if config.Properties != nil {
		flags = config.Properties.NotifyKeyspaceEvents
	}
	if !strings.ContainsAny(flags, "STIV") {
		return
	}
	subkeys = dedupeStrings(subkeys)
	if len(subkeys) == 0 {
		return
	}

	encoded := encodeSubkeys(subkeys)
	publish := func(channel, payload string) {
		_ = pubsub.Publish(db.server.hub, [][]byte{[]byte(channel), []byte(payload)})
	}
	dbIndex := db.index
	// Events containing '|' cannot be represented unambiguously in the two
	// event-prefixed channel formats.
	eventHasPipe := strings.Contains(event, "|")

	if strings.Contains(flags, "S") && !eventHasPipe {
		publish(fmt.Sprintf("__subkeyspace@%d__:%s", dbIndex, key), event+"|"+encoded)
	}
	if strings.Contains(flags, "T") {
		publish(fmt.Sprintf("__subkeyevent@%d__:%s", dbIndex, event),
			strconv.Itoa(len(key))+":"+key+"|"+encoded)
	}
	if strings.Contains(flags, "I") && !strings.Contains(key, "\n") {
		for _, subkey := range subkeys {
			publish(fmt.Sprintf("__subkeyspaceitem@%d__:%s\n%s", dbIndex, key, subkey), event)
		}
	}
	if strings.Contains(flags, "V") && !eventHasPipe {
		publish(fmt.Sprintf("__subkeyspaceevent@%d__:%s|%s", dbIndex, event, key), encoded)
	}
}

func encodeSubkeys(subkeys []string) string {
	var b strings.Builder
	for i, subkey := range subkeys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(len(subkey)))
		b.WriteByte(':')
		b.WriteString(subkey)
	}
	return b.String()
}

func dedupeStrings(in []string) []string {
	if len(in) < 2 {
		return in
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
