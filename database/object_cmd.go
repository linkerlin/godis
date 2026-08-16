package database

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/config"
	"github.com/linkerlin/godis/datastruct/dict"
	"github.com/linkerlin/godis/datastruct/hll"
	"github.com/linkerlin/godis/datastruct/json"
	"github.com/linkerlin/godis/datastruct/list"
	"github.com/linkerlin/godis/datastruct/probabilistic"
	"github.com/linkerlin/godis/datastruct/set"
	"github.com/linkerlin/godis/datastruct/sortedset"
	"github.com/linkerlin/godis/datastruct/stream"
	"github.com/linkerlin/godis/datastruct/timeseries"
	"github.com/linkerlin/godis/datastruct/vector"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// execObject inspects the internals of Redis objects associated with keys
// OBJECT subcommand [arguments [arguments ...]]
func execObject(db *DB, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'object' command")
	}

	subCmd := strings.ToUpper(string(args[0]))

	// Only the key-taking subcommands require a second argument; an unknown
	// subcommand must surface the "Try OBJECT HELP." error regardless of arity
	// (matching Redis).
	switch subCmd {
	case "ENCODING":
		if len(args) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'object|encoding' command")
		}
		return execObjectEncoding(db, string(args[1]))
	case "IDLETIME":
		if len(args) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'object|idletime' command")
		}
		return execObjectIdleTime(db, string(args[1]))
	case "FREQ":
		if len(args) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'object|freq' command")
		}
		return execObjectFreq(db, string(args[1]))
	case "REFCOUNT":
		if len(args) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'object|refcount' command")
		}
		return execObjectRefCount(db, string(args[1]))
	case "HELP":
		if len(args) != 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'object|help' command")
		}
		return execObjectHelp()
	default:
		return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for '%s'. Try OBJECT HELP.", subCmd))
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
	switch v := data.(type) {
	case []byte:
		// Redis: HLL strings report encoding "hyperloglog" (not raw).
		if hll.IsHLLString(v) {
			return "hyperloglog"
		}
		if _, err := strconv.ParseInt(string(v), 10, 64); err == nil {
			return "int"
		}
		// Redis embstr for short strings (OBJ_ENCODING_EMBSTR_SIZE_LIMIT = 44).
		if len(v) <= 44 {
			return "embstr"
		}
		return "raw"
	case list.List:
		if v.Len() <= 128 {
			return "listpack"
		}
		return "quicklist"
	case *dict.ConcurrentDict, *dict.SimpleDict, *dict.ExpireDict:
		if d, ok := data.(dict.Dict); ok && d.Len() <= 128 {
			return "listpack"
		}
		return "hashtable"
	case dict.Dict:
		if v.Len() <= 128 {
			return "listpack"
		}
		return "hashtable"
	case *set.Set:
		if v.Len() <= 512 && setAllIntegerMembers(v) {
			return "intset"
		}
		// Redis 7+: small non-intset sets use listpack (set-max-listpack-entries=128).
		if v.Len() <= 128 {
			return "listpack"
		}
		return "hashtable"
	case *sortedset.SortedSet:
		if v.Len() <= 128 {
			return "listpack"
		}
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
	case *vector.VectorSet:
		return "vectorset"
	default:
		return "unknown"
	}
}

func setAllIntegerMembers(s *set.Set) bool {
	if s == nil || s.Len() == 0 {
		return true
	}
	ok := true
	s.ForEach(func(member string) bool {
		if _, err := strconv.ParseInt(member, 10, 64); err != nil {
			ok = false
			return false
		}
		return true
	})
	return ok
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
	// Missing key → null even when LFU is not selected (Redis 8.x).
	raw, ok := db.data.GetWithLock(key)
	if !ok {
		return protocol.MakeNullBulkReply()
	}
	_ = raw
	if db.IsExpired(key) {
		return protocol.MakeNullBulkReply()
	}
	pol := ""
	if config.Properties != nil {
		pol = strings.ToLower(config.Properties.MaxmemoryPolicy)
	}
	if pol != "allkeys-lfu" && pol != "volatile-lfu" {
		return protocol.MakeErrReply("ERR An LFU maxmemory policy is not selected, access frequency not tracked. Please note that when switching between policies at runtime LRU and LFU data will take some time to adjust.")
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
		"    Return the kind of internal representation used in order to store the value",
		"    associated with a <key>.",
		"FREQ <key>",
		"    Return the access frequency index of the <key>. The returned integer is",
		"    proportional to the logarithm of the recent access frequency of the key.",
		"IDLETIME <key>",
		"    Return the idle time of the <key>, that is the approximated number of",
		"    seconds elapsed since the last access to the key.",
		"REFCOUNT <key>",
		"    Return the number of references of the value associated with the specified",
		"    <key>.",
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
