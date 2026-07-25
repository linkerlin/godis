package database

import (
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
	"strings"

	"github.com/linkerlin/godis/datastruct/dict"
	godisjson "github.com/linkerlin/godis/datastruct/json"
	"github.com/linkerlin/godis/datastruct/list"
	"github.com/linkerlin/godis/datastruct/set"
	"github.com/linkerlin/godis/datastruct/sortedset"
	"github.com/linkerlin/godis/datastruct/stream"
	"github.com/linkerlin/godis/datastruct/timeseries"
	"github.com/linkerlin/godis/datastruct/vector"
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
	samples := 5 // Redis default
	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return protocol.MakeErrReply("ERR syntax error")
		}
		option := strings.ToUpper(string(args[i]))
		n, err := strconv.Atoi(string(args[i+1]))
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		if option != "SAMPLES" {
			return protocol.MakeErrReply("ERR syntax error")
		}
		if n < 0 {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		samples = n
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
	return protocol.MakeIntReply(estimateEntityBytes(key, entity.Data, samples))
}

func estimateEntityBytes(key string, data interface{}, samples int) int64 {
	size := int64(len(key) + 64)
	switch v := data.(type) {
	case []byte:
		size += int64(len(v))
	case *godisjson.JSONValue:
		if b, err := v.ToBytes(); err == nil {
			size += int64(len(b))
		}
	case *set.Set:
		size += estimateSetBytes(v, samples)
	case *sortedset.SortedSet:
		size += estimateZSetBytes(v, samples)
	case list.List:
		size += estimateListBytes(v, samples)
	case *stream.Stream:
		size += int64(v.Len() * 64)
	case *dict.ExpireDict:
		size += estimateExpireDictBytes(v, samples)
	case dict.Dict:
		size += estimateDictBytes(v, samples)
	case *timeseries.TimeSeries:
		size += int64(v.Len()*16 + len(v.GetLabels())*16)
	case *vector.VectorSet:
		dim := v.Dimension()
		if dim <= 0 {
			dim = 1
		}
		size += int64(v.Len() * (dim*8 + 32))
	default:
		size += 128
	}
	return size
}

// estimateFromSamples averages sampled element sizes and scales to totalCount.
// samples==0 means sample all elements (Redis semantics).
func estimateFromSamples(totalCount int, samples int, elemSize func(i int) int64) int64 {
	if totalCount <= 0 {
		return 0
	}
	n := samples
	if n <= 0 || n > totalCount {
		n = totalCount
	}
	var sum int64
	if n == totalCount {
		for i := 0; i < totalCount; i++ {
			sum += elemSize(i)
		}
		return sum
	}
	// Sample n distinct indices
	idxs := make([]int, totalCount)
	for i := range idxs {
		idxs[i] = i
	}
	for i := 0; i < n; i++ {
		j := i + rand.Intn(totalCount-i)
		idxs[i], idxs[j] = idxs[j], idxs[i]
		sum += elemSize(idxs[i])
	}
	avg := sum / int64(n)
	return avg * int64(totalCount)
}

func estimateDictBytes(d dict.Dict, samples int) int64 {
	n := d.Len()
	if n == 0 {
		return 0
	}
	take := samples
	if take <= 0 || take > n {
		take = n
	}
	fields := d.RandomDistinctKeys(take)
	var sum int64
	for _, field := range fields {
		sz := int64(len(field) + 16)
		if raw, ok := d.Get(field); ok {
			if b, ok := raw.([]byte); ok {
				sz += int64(len(b))
			}
		}
		sum += sz
	}
	return (sum / int64(len(fields))) * int64(n)
}

func estimateExpireDictBytes(ed *dict.ExpireDict, samples int) int64 {
	n := ed.Len()
	if n == 0 {
		return 0
	}
	take := samples
	if take <= 0 || take > n {
		take = n
	}
	fields := ed.RandomDistinctKeys(take)
	var sum int64
	for _, field := range fields {
		sz := int64(len(field) + 32)
		if raw, ok := ed.Get(field); ok {
			if b, ok := raw.([]byte); ok {
				sz += int64(len(b))
			}
		}
		sum += sz
	}
	return (sum / int64(len(fields))) * int64(n)
}

func estimateSetBytes(s *set.Set, samples int) int64 {
	n := s.Len()
	if n == 0 {
		return 0
	}
	take := samples
	if take <= 0 || take > n {
		take = n
	}
	members := s.RandomDistinctMembers(take)
	var sum int64
	for _, m := range members {
		sum += int64(len(m) + 16)
	}
	return (sum / int64(len(members))) * int64(n)
}

func estimateZSetBytes(z *sortedset.SortedSet, samples int) int64 {
	n := int(z.Len())
	if n == 0 {
		return 0
	}
	take := samples
	if take <= 0 || take > n {
		take = n
	}
	// Sample by rank indices
	return estimateFromSamples(n, take, func(i int) int64 {
		elems := z.RangeByRank(int64(i), int64(i+1), false)
		if len(elems) == 0 {
			return 24
		}
		return int64(len(elems[0].Member) + 24)
	})
}

func estimateListBytes(l list.List, samples int) int64 {
	n := l.Len()
	return estimateFromSamples(n, samples, func(i int) int64 {
		v := l.Get(i)
		if b, ok := v.([]byte); ok {
			return int64(len(b) + 16)
		}
		return 16
	})
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
