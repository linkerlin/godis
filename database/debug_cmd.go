package database

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/linkerlin/godis/interface/database"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/utils"
	"github.com/linkerlin/godis/lib/wildcard"
	"github.com/linkerlin/godis/redis/protocol"
)

// activeExpireEnabled gates the timewheel active-expiry callback. It defaults
// to on; DEBUG SET-ACTIVE-EXPIRE 0 disables active expiry (lazy deletion via
// IsExpired still runs), mirroring Redis's activeExpireCycle toggle.
var activeExpireEnabled atomic.Bool

func init() { activeExpireEnabled.Store(true) }

// execDebug handles DEBUG subcommands (minimal Redis-compatible subset).
func execDebug(server *Server, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'debug' command")
	}
	sub := strings.ToUpper(string(args[0]))
	switch sub {
	case "SLEEP":
		if len(args) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'debug|sleep' command")
		}
		sec, err := strconv.ParseFloat(string(args[1]), 64)
		if err != nil || sec < 0 {
			return protocol.MakeErrReply("ERR invalid sleep time")
		}
		if sec > 0 {
			time.Sleep(time.Duration(sec * float64(time.Second)))
		}
		return protocol.MakeOkReply()
	case "OBJECT":
		if len(args) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'debug|object' command")
		}
		return execDebugObject(server, c, string(args[1]))
	case "SET-ACTIVE-EXPIRE":
		// Real implementation: toggles the timewheel active-expiry callback.
		if len(args) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'debug|set-active-expire' command")
		}
		v, err := strconv.Atoi(string(args[1]))
		if err != nil || (v != 0 && v != 1) {
			return protocol.MakeErrReply("ERR argument must be 0 or 1")
		}
		activeExpireEnabled.Store(v == 1)
		return protocol.MakeOkReply()
	case "RELOAD":
		// Stub: RDB save+reload round-trip is not performed; data already resident.
		return protocol.MakeOkReply()
	case "CHANGE-REPL-ID":
		// Real: rotate the replication ID so replicas can no longer partial
		// resync (masterTryPartialSyncWithSlave compares replId) and must
		// full-resync on their next PSYNC.
		server.masterStatus.mu.Lock()
		server.masterStatus.replId = utils.RandHexString(40)
		server.masterStatus.mu.Unlock()
		return protocol.MakeOkReply()
	case "JMAP":
		// Stub: JVM-only in real Redis (no-op there too); accepted for compatibility.
		return protocol.MakeOkReply()
	case "FLUSHALL":
		// Stub: intentionally does NOT flush data to avoid destructive surprises.
		return protocol.MakeOkReply()
	case "DIGEST":
		if len(args) != 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'debug|digest' command")
		}
		return protocol.MakeStatusReply(computeServerDigest(server))
	case "DIGEST-VALUE":
		if len(args) < 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'debug|digest-value' command")
		}
		return execDebugDigestValue(server, c, args[1:])
	case "STRINGMATCH-LEN":
		if len(args) != 3 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'debug|stringmatch-len' command")
		}
		pattern, err := wildcard.CompilePattern(string(args[1]))
		if err != nil {
			return protocol.MakeIntReply(0)
		}
		if pattern.IsMatch(string(args[2])) {
			return protocol.MakeIntReply(1)
		}
		return protocol.MakeIntReply(0)
	case "HELP":
		help := []string{
			"DEBUG <subcommand> [<arg> [value] [opt] ...]",
			"Subcommands:",
			"SLEEP <seconds>",
			"    Sleep for the specified number of seconds (float allowed).",
			"OBJECT <key>",
			"    Show low-level info about a key (simplified).",
			"SET-ACTIVE-EXPIRE <0|1>",
			"    Enable/disable the active expiry callback (lazy expiry always runs).",
			"RELOAD",
			"    Stub: accepted, no effect.",
			"CHANGE-REPL-ID",
			"    Rotate the replication ID (replicas must full-resync).",
			"JMAP",
			"    Stub: accepted, no effect.",
			"FLUSHALL",
			"    Stub: accepted, does not flush data.",
			"DIGEST",
			"    Return a SHA1 digest over all keys in all databases.",
			"DIGEST-VALUE <key> [<key> ...]",
			"    Return a SHA1 digest of each key's value, nil for missing keys.",
			"STRINGMATCH-LEN <pattern> <string>",
			"    Return 1 if pattern matches string, else 0.",
			"HELP",
			"    Print this help.",
		}
		out := make([][]byte, len(help))
		for i, h := range help {
			out[i] = []byte(h)
		}
		return protocol.MakeMultiBulkReply(out)
	default:
		return protocol.MakeErrReply("ERR Unknown DEBUG subcommand or wrong number of arguments for '" + sub + "'. Try DEBUG HELP.")
	}
}

// entityDigestBytes renders a DataEntity's payload for digesting. Binary-safe
// for []byte values; other types use fmt %v (digest stability across types is
// best-effort — Redis uses its own serializers).
func entityDigestBytes(entity *database.DataEntity) []byte {
	if entity == nil {
		return nil
	}
	if b, ok := entity.Data.([]byte); ok {
		return b
	}
	if s, ok := entity.Data.(string); ok {
		return []byte(s)
	}
	return []byte(fmt.Sprintf("%v", entity.Data))
}

// computeServerDigest returns a SHA1 over every key name and payload in every
// database (DEBUG DIGEST semantics).
func computeServerDigest(server *Server) string {
	h := sha1.New()
	if server == nil {
		return hex.EncodeToString(h.Sum(nil))
	}
	for i := range server.dbSet {
		holder := server.dbSet[i]
		if holder == nil {
			continue
		}
		v := holder.Load()
		if v == nil {
			continue
		}
		db := v.(*DB)
		db.data.ForEach(func(key string, val interface{}) bool {
			h.Write([]byte(key))
			h.Write([]byte{0})
			if entity, ok := val.(*database.DataEntity); ok {
				h.Write(entityDigestBytes(entity))
			}
			h.Write([]byte{0})
			return true
		})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func execDebugDigestValue(server *Server, c redis.Connection, keys [][]byte) redis.Reply {
	dbIndex := 0
	if c != nil {
		dbIndex = c.GetDBIndex()
	}
	db, errReply := server.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}
	replies := make([]redis.Reply, len(keys))
	for i, key := range keys {
		entity, exists := db.GetEntity(string(key))
		if !exists {
			replies[i] = protocol.MakeNullBulkReply()
			continue
		}
		h := sha1.New()
		h.Write([]byte(string(key)))
		h.Write([]byte{0})
		h.Write(entityDigestBytes(entity))
		replies[i] = protocol.MakeStatusReply(hex.EncodeToString(h.Sum(nil)))
	}
	return protocol.MakeMultiRawReply(replies)
}

func execDebugObject(server *Server, c redis.Connection, key string) redis.Reply {
	dbIndex := 0
	if c != nil {
		dbIndex = c.GetDBIndex()
	}
	db, errReply := server.selectDB(dbIndex)
	if errReply != nil {
		return errReply
	}
	entity, exists := db.GetEntity(key)
	if !exists {
		return protocol.MakeErrReply("ERR no such key")
	}
	encoding := "raw"
	serLen := 0
	switch v := entity.Data.(type) {
	case []byte:
		encoding = "embstr"
		serLen = len(v)
	case string:
		encoding = "embstr"
		serLen = len(v)
	default:
		encoding = "hashtable"
		serLen = 0
	}
	msg := fmt.Sprintf(
		"Value at:0x0 refcount:1 encoding:%s serializedlength:%d lru:0 lru_seconds_idle:0",
		encoding, serLen,
	)
	return protocol.MakeStatusReply(msg)
}

func init() {
	registerSpecialCommand("Debug", -2, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagNoScript, redisFlagLoading}, 0, 0, 0)
}
