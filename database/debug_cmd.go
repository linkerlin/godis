package database

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/lib/wildcard"
	"github.com/linkerlin/godis/redis/protocol"
)

// zeroDigest is a harmless stand-in for a SHA1 digest used by DEBUG DIGEST /
// DIGEST-VALUE stubs (real Redis computes this over the dataset for
// consistency checks; we don't implement that here).
var zeroDigest = strings.Repeat("0", 40)

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
		// Stub: active expire cycle toggling is not implemented; accept and no-op.
		if len(args) != 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'debug|set-active-expire' command")
		}
		return protocol.MakeOkReply()
	case "RELOAD":
		// Stub: RDB save+reload round-trip is not performed; data already resident.
		return protocol.MakeOkReply()
	case "CHANGE-REPL-ID":
		// Stub: replication ID is not mutated.
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
		return protocol.MakeStatusReply(zeroDigest)
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
			"    Stub: accepted, no effect.",
			"RELOAD",
			"    Stub: accepted, no effect.",
			"CHANGE-REPL-ID",
			"    Stub: accepted, no effect.",
			"JMAP",
			"    Stub: accepted, no effect.",
			"FLUSHALL",
			"    Stub: accepted, does not flush data.",
			"DIGEST",
			"    Stub: returns a fixed all-zero digest.",
			"DIGEST-VALUE <key> [<key> ...]",
			"    Stub: returns a fixed digest per existing key, nil for missing keys.",
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
		if _, exists := db.GetEntity(string(key)); exists {
			replies[i] = protocol.MakeStatusReply(zeroDigest)
		} else {
			replies[i] = protocol.MakeNullBulkReply()
		}
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
