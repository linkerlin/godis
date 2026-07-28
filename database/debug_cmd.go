package database

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

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
	case "HELP":
		help := []string{
			"DEBUG <subcommand> [<arg> [value] [opt] ...]",
			"Subcommands:",
			"SLEEP <seconds>",
			"    Sleep for the specified number of seconds (float allowed).",
			"OBJECT <key>",
			"    Show low-level info about a key (simplified).",
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
