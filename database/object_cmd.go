package database

import (
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/datastruct/dict"
	"github.com/linkerlin/godis/datastruct/json"
	"github.com/linkerlin/godis/datastruct/list"
	"github.com/linkerlin/godis/datastruct/probabilistic"
	"github.com/linkerlin/godis/datastruct/set"
	"github.com/linkerlin/godis/datastruct/sortedset"
	"github.com/linkerlin/godis/datastruct/stream"
	"github.com/linkerlin/godis/datastruct/timeseries"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
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
	case *dict.ConcurrentDict, *dict.SimpleDict, *dict.ExpireDict:
		return "hashtable"
	case dict.Dict:
		return "hashtable"
	case *set.Set:
		return "hashtable"
	case *sortedset.SortedSet:
		return "skiplist"
	case *stream.Stream:
		return "stream"
	case *json.JSONValue:
		return "json"
	case *probabilistic.BloomFilter:
		return "bloomfilter"
	case *probabilistic.CuckooFilter:
		return "cuckoo"
	case *probabilistic.CountMinSketch:
		return "cms"
	case *probabilistic.TopK:
		return "topk"
	case *probabilistic.TDigest:
		return "tdigest"
	case *timeseries.TimeSeries:
		return "timeseries"
	default:
		return "unknown"
	}
}

// execObjectIdleTime returns the idle time of the key
func execObjectIdleTime(db *DB, key string) redis.Reply {
	raw, ok := db.data.GetWithLock(key)
	if !ok {
		return protocol.MakeNullBulkReply()
	}
	_ = raw
	if db.IsExpired(key) {
		return protocol.MakeNullBulkReply()
	}
	idle := int64(0)
	if db.evictionManager != nil {
		idle = db.evictionManager.IdleSeconds(key)
	}
	return protocol.MakeIntReply(idle)
}

// execObjectFreq returns the access frequency of the key (LFU)
func execObjectFreq(db *DB, key string) redis.Reply {
	raw, ok := db.data.GetWithLock(key)
	if !ok {
		return protocol.MakeNullBulkReply()
	}
	_ = raw
	if db.IsExpired(key) {
		return protocol.MakeNullBulkReply()
	}
	freq := int64(0)
	if db.evictionManager != nil {
		freq = db.evictionManager.Freq(key)
	}
	return protocol.MakeIntReply(freq)
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
