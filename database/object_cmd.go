package database

import (
	"strconv"
	"strings"
	"time"

	"github.com/hdt3213/godis/datastruct/dict"
	"github.com/hdt3213/godis/datastruct/list"
	"github.com/hdt3213/godis/datastruct/set"
	"github.com/hdt3213/godis/datastruct/sortedset"
	"github.com/hdt3213/godis/datastruct/stream"
	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// execObject inspects the internals of Redis objects associated with keys
// OBJECT subcommand [arguments [arguments ...]]
func execObject(db *DB, args [][]byte) redis.Reply {
	if len(args) < 2 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'object' command")
	}

	subCmd := strings.ToUpper(string(args[0]))

	switch subCmd {
	case "REFCOUNT":
		return execObjectRefCount(db, string(args[1]))
	case "ENCODING":
		return execObjectEncoding(db, string(args[1]))
	case "IDLETIME":
		return execObjectIdleTime(db, string(args[1]))
	case "FREQ":
		return execObjectFreq(db, string(args[1]))
	case "HELP":
		return execObjectHelp()
	default:
		return protocol.MakeErrReply("ERR Unknown subcommand or wrong number of arguments for '" + subCmd + "'")
	}
}

// execObjectRefCount returns the number of references of the value associated with the specified key
func execObjectRefCount(db *DB, key string) redis.Reply {
	// Simplified: always return 1 (no shared objects)
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeIntReply(1)
}

// execObjectEncoding returns the internal encoding of the Redis object
func execObjectEncoding(db *DB, key string) redis.Reply {
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeNullBulkReply()
	}

	encoding := getObjectEncoding(entity.Data)
	return protocol.MakeBulkReply([]byte(encoding))
}

// getObjectEncoding returns the encoding name for a given data type
func getObjectEncoding(data interface{}) string {
	switch data.(type) {
	case []byte:
		return "raw"
	case list.List:
		return "quicklist"
	case *dict.ConcurrentDict:
		return "hashtable"
	case *set.Set:
		return "hashtable"
	case *sortedset.SortedSet:
		return "skiplist"
	case *stream.Stream:
		return "stream"
	default:
		return "unknown"
	}
}

// execObjectIdleTime returns the idle time of the key
func execObjectIdleTime(db *DB, key string) redis.Reply {
	// Simplified: return 0 (no idle time tracking)
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeIntReply(0)
}

// execObjectFreq returns the access frequency of the key (LFU)
func execObjectFreq(db *DB, key string) redis.Reply {
	// Simplified: return 0 (no LFU tracking)
	_, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeIntReply(0)
}

// execObjectHelp returns help information
func execObjectHelp() redis.Reply {
	help := []string{
		"OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
		"ENCODING <key>",
		"    Return the encoding of the key.",
		"FREQ <key>",
		"    Return the access frequency of the key.",
		"IDLETIME <key>",
		"    Return the time since the last access.",
		"REFCOUNT <key>",
		"    Return the number of references of the value.",
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
	registerCommand("Object", execObject, readFirstKey, nil, -2, flagReadOnly).
		attachCommandExtra([]string{redisFlagReadonly}, 2, 2, 1)
}

// Avoid unused imports
var _ = time.Now
var _ = strconv.Itoa
