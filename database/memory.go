package database

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"

	godisjson "github.com/linkerlin/godis/datastruct/json"
	"github.com/linkerlin/godis/datastruct/list"
	"github.com/linkerlin/godis/datastruct/set"
	"github.com/linkerlin/godis/datastruct/sortedset"
	"github.com/linkerlin/godis/datastruct/stream"
	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// execMemory 处理 MEMORY 命令
func execMemory(server *Server, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'memory' command")
	}

	subCmd := strings.ToUpper(string(args[0]))

	switch subCmd {
	case "USAGE":
		return execMemoryUsage(server, c, args[1:])
	case "STATS":
		return execMemoryStats()
	case "PURGE":
		runtime.GC()
		return protocol.MakeOkReply()
	case "DOCTOR":
		return protocol.MakeBulkReply([]byte("I'm fine, no issues to report."))
	case "MALLOC-STATS":
		return protocol.MakeBulkReply([]byte("Stats not available in Go runtime"))
	default:
		return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for '%s'", subCmd))
	}
}

func execMemoryUsage(server *Server, c redis.Connection, args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'memory|usage' command")
	}

	key := string(args[0])
	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return protocol.MakeErrReply("ERR syntax error")
		}
		option := strings.ToUpper(string(args[i]))
		if _, err := strconv.Atoi(string(args[i+1])); err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		if option != "SAMPLES" {
			return protocol.MakeErrReply("ERR syntax error")
		}
	}

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
		return &protocol.NullBulkReply{}
	}
	return protocol.MakeIntReply(estimateEntityBytes(key, entity.Data))
}

func estimateEntityBytes(key string, data interface{}) int64 {
	size := int64(len(key) + 64)
	switch v := data.(type) {
	case []byte:
		size += int64(len(v))
	case *godisjson.JSONValue:
		if b, err := v.ToBytes(); err == nil {
			size += int64(len(b))
		}
	case *set.Set:
		size += int64(v.Len() * 16)
	case *sortedset.SortedSet:
		size += int64(v.Len() * 24)
	case list.List:
		size += int64(v.Len() * 16)
	case *stream.Stream:
		size += int64(v.Len() * 64)
	default:
		size += 128
	}
	return size
}

func execMemoryStats() redis.Reply {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	stats := []redis.Reply{
		protocol.MakeBulkReply([]byte("peak.allocated")),
		protocol.MakeIntReply(int64(m.TotalAlloc)),
		protocol.MakeBulkReply([]byte("total.allocated")),
		protocol.MakeIntReply(int64(m.Alloc)),
		protocol.MakeBulkReply([]byte("total.system")),
		protocol.MakeIntReply(int64(m.Sys)),
		protocol.MakeBulkReply([]byte("keys.count")),
		protocol.MakeIntReply(int64(m.HeapObjects)),
		protocol.MakeBulkReply([]byte("heap.allocated")),
		protocol.MakeIntReply(int64(m.HeapAlloc)),
		protocol.MakeBulkReply([]byte("heap.system")),
		protocol.MakeIntReply(int64(m.HeapSys)),
		protocol.MakeBulkReply([]byte("heap.free")),
		protocol.MakeIntReply(int64(m.HeapIdle)),
		protocol.MakeBulkReply([]byte("gc.runs")),
		protocol.MakeIntReply(int64(m.NumGC)),
		protocol.MakeBulkReply([]byte("gc.used_cpu")),
		protocol.MakeIntReply(int64(m.GCCPUFraction * 100000)),
	}

	return protocol.MakeMultiRawReply(stats)
}

func init() {
	registerSpecialCommand("Memory", -2, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagRandom, redisFlagStale}, 0, 0, 0)
}
