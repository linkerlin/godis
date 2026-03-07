package database

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/hdt3213/godis/interface/redis"
	"github.com/hdt3213/godis/redis/protocol"
)

// execMemory 处理 MEMORY 命令
// MEMORY USAGE key [SAMPLES count]
// MEMORY STATS
// MEMORY PURGE
// MEMORY DOCTOR
// MEMORY MALLOC-STATS
func execMemory(args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'memory' command")
	}

	subCmd := strings.ToUpper(string(args[0]))

	switch subCmd {
	case "USAGE":
		return execMemoryUsage(args[1:])
	case "STATS":
		return execMemoryStats()
	case "PURGE":
		// 触发 GC
		runtime.GC()
		return protocol.MakeOkReply()
	case "DOCTOR":
		return protocol.MakeBulkReply([]byte("I'm fine, no issues to report."))
	case "MALLOC-STATS":
		return protocol.MakeBulkReply([]byte("Stats not available in Go runtime"))
	default:
		return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown subcommand '%s'", subCmd))
	}
}

// execMemoryUsage 处理 MEMORY USAGE 命令
func execMemoryUsage(args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'memory|usage' command")
	}

	key := string(args[0])
	samples := 5 // 默认采样数量

	// 解析可选参数
	for i := 1; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return protocol.MakeErrReply("ERR syntax error")
		}
		option := strings.ToUpper(string(args[i]))
		value, err := strconv.Atoi(string(args[i+1]))
		if err != nil {
			return protocol.MakeErrReply("ERR value is not an integer or out of range")
		}
		switch option {
		case "SAMPLES":
			samples = value
		default:
			return protocol.MakeErrReply("ERR syntax error")
		}
	}

	// 返回估算的内存使用量（简化实现）
	// 实际实现需要遍历数据库获取键值大小
	_ = key
	_ = samples

	// 简化返回，实际需要根据键值内容计算
	return protocol.MakeIntReply(0)
}

// execMemoryStats 处理 MEMORY STATS 命令
func execMemoryStats() redis.Reply {
	// 获取运行时内存统计
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// 构建统计信息
	stats := []redis.Reply{
		// 峰值内存
		protocol.MakeBulkReply([]byte("peak.allocated")),
		protocol.MakeIntReply(int64(m.TotalAlloc)),
		
		// 当前分配的内存
		protocol.MakeBulkReply([]byte("total.allocated")),
		protocol.MakeIntReply(int64(m.Alloc)),
		
		// 从系统获取的内存
		protocol.MakeBulkReply([]byte("total.system")),
		protocol.MakeIntReply(int64(m.Sys)),
		
		// 活跃对象数
		protocol.MakeBulkReply([]byte("keys.count")),
		protocol.MakeIntReply(int64(m.HeapObjects)),
		
		// 堆内存
		protocol.MakeBulkReply([]byte("heap.allocated")),
		protocol.MakeIntReply(int64(m.HeapAlloc)),
		
		// 堆系统内存
		protocol.MakeBulkReply([]byte("heap.system")),
		protocol.MakeIntReply(int64(m.HeapSys)),
		
		// 堆空闲
		protocol.MakeBulkReply([]byte("heap.free")),
		protocol.MakeIntReply(int64(m.HeapIdle)),
		
		// GC 次数
		protocol.MakeBulkReply([]byte("gc.runs")),
		protocol.MakeIntReply(int64(m.NumGC)),
		
		// GC CPU 占用
		protocol.MakeBulkReply([]byte("gc.used_cpu")),
		protocol.MakeIntReply(int64(m.GCCPUFraction * 100000)),
	}

	return protocol.MakeMultiRawReply(stats)
}

func init() {
	registerSpecialCommand("Memory", -2, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagRandom, redisFlagStale}, 0, 0, 0)
}
