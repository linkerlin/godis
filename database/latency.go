package database

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/linkerlin/godis/interface/redis"
	"github.com/linkerlin/godis/redis/protocol"
)

// LatencyMonitor 延迟监控器
type LatencyMonitor struct {
	mu        sync.RWMutex
	events    map[string][]*LatencyEvent
	maxEvents int
}

// LatencyEvent 延迟事件
type LatencyEvent struct {
	Timestamp int64
	Duration  int64 // 微秒
}

// 全局延迟监控器
var latencyMonitor = &LatencyMonitor{
	events:    make(map[string][]*LatencyEvent),
	maxEvents: 160, // 每个事件类型最多保存 160 个事件
}

// per-command latency histograms for LATENCY HISTOGRAM (microseconds buckets).
var (
	cmdLatencyMu   sync.Mutex
	cmdLatencyHist = make(map[string]*cmdLatencyBuckets)
)

type cmdLatencyBuckets struct {
	// Redis-ish cumulative: count of samples and sum of micros; coarse buckets.
	count  uint64
	sumUs  uint64
	maxUs  uint64
	bucket [16]uint64 // powers of two: <1us,<2,<4,...,<32768,<inf
}

// RecordCommandLatency records a sample for LATENCY HISTOGRAM.
func RecordCommandLatency(cmdName string, duration time.Duration) {
	us := duration.Microseconds()
	if us < 0 {
		return
	}
	cmdLatencyMu.Lock()
	h := cmdLatencyHist[cmdName]
	if h == nil {
		h = &cmdLatencyBuckets{}
		cmdLatencyHist[cmdName] = h
	}
	h.count++
	h.sumUs += uint64(us)
	if uint64(us) > h.maxUs {
		h.maxUs = uint64(us)
	}
	idx := 0
	v := us
	for idx < len(h.bucket)-1 && v >= 1 {
		v >>= 1
		idx++
	}
	h.bucket[idx]++
	cmdLatencyMu.Unlock()
}

// ResetCommandLatency clears histogram data (optionally filtered by names).
func ResetCommandLatency(names []string) {
	cmdLatencyMu.Lock()
	defer cmdLatencyMu.Unlock()
	if len(names) == 0 {
		cmdLatencyHist = make(map[string]*cmdLatencyBuckets)
		return
	}
	for _, n := range names {
		delete(cmdLatencyHist, n)
	}
}

// RecordLatency 记录延迟事件
func RecordLatency(eventName string, duration time.Duration) {
	latencyMonitor.Record(eventName, duration)
}

// Record 记录延迟事件
func (lm *LatencyMonitor) Record(eventName string, duration time.Duration) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	event := &LatencyEvent{
		Timestamp: time.Now().Unix(),
		Duration:  duration.Microseconds(),
	}

	lm.events[eventName] = append(lm.events[eventName], event)

	// 限制事件数量
	if len(lm.events[eventName]) > lm.maxEvents {
		lm.events[eventName] = lm.events[eventName][1:]
	}
}

// GetEvents 获取指定事件的延迟记录
func (lm *LatencyMonitor) GetEvents(eventName string) []*LatencyEvent {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.events[eventName]
}

// GetAllEvents 获取所有事件类型
func (lm *LatencyMonitor) GetAllEvents() map[string][]*LatencyEvent {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	result := make(map[string][]*LatencyEvent)
	for k, v := range lm.events {
		result[k] = v
	}
	return result
}

// Reset 重置延迟监控
func (lm *LatencyMonitor) Reset() {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.events = make(map[string][]*LatencyEvent)
}

// execLatency 处理 LATENCY 命令
// LATENCY HISTORY event-name
// LATENCY LATEST
// LATENCY DOCTOR
// LATENCY GRAPH event-name
// LATENCY RESET [event-name ...]
// LATENCY HELP
func execLatency(args [][]byte) redis.Reply {
	if len(args) < 1 {
		return protocol.MakeErrReply("ERR wrong number of arguments for 'latency' command")
	}

	subCmd := strings.ToUpper(string(args[0]))

	switch subCmd {
	case "HISTORY":
		if len(args) < 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'latency|history' command")
		}
		return execLatencyHistory(string(args[1]))
	case "LATEST":
		if len(args) != 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'latency|latest' command")
		}
		return execLatencyLatest()
	case "DOCTOR":
		if len(args) != 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'latency|doctor' command")
		}
		return execLatencyDoctor()
	case "GRAPH":
		if len(args) < 2 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'latency|graph' command")
		}
		return execLatencyGraph(string(args[1]))
	case "RESET":
		if len(args) > 1 {
			return execLatencyReset(args[1:])
		}
		latencyMonitor.Reset()
		ResetCommandLatency(nil)
		return protocol.MakeOkReply()
	case "HISTOGRAM":
		return execLatencyHistogram(args[1:])
	case "HELP":
		if len(args) != 1 {
			return protocol.MakeErrReply("ERR wrong number of arguments for 'latency|help' command")
		}
		return execLatencyHelp()
	default:
		return protocol.MakeErrReply(fmt.Sprintf("ERR Unknown subcommand or wrong number of arguments for '%s'. Try LATENCY HELP.", subCmd))
	}
}

// execLatencyHistory 获取指定事件的历史延迟数据
func execLatencyHistory(eventName string) redis.Reply {
	events := latencyMonitor.GetEvents(eventName)
	if len(events) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}

	result := make([]redis.Reply, 0, len(events)*2)
	for _, event := range events {
		result = append(result,
			protocol.MakeIntReply(event.Timestamp),
			protocol.MakeIntReply(event.Duration),
		)
	}

	return protocol.MakeMultiRawReply(result)
}

// execLatencyLatest 获取最新的延迟事件
func execLatencyLatest() redis.Reply {
	allEvents := latencyMonitor.GetAllEvents()
	if len(allEvents) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}

	result := make([]redis.Reply, 0)
	for eventName, events := range allEvents {
		if len(events) == 0 {
			continue
		}
		// 获取最新的事件
		latest := events[len(events)-1]

		// 计算平均延迟
		var total int64
		for _, e := range events {
			total += e.Duration
		}
		avg := total / int64(len(events))

		// 计算最大延迟
		var max int64
		for _, e := range events {
			if e.Duration > max {
				max = e.Duration
			}
		}

		eventReply := []redis.Reply{
			protocol.MakeBulkReply([]byte(eventName)),
			protocol.MakeIntReply(latest.Timestamp),
			protocol.MakeIntReply(latest.Duration),
			protocol.MakeIntReply(max),
			protocol.MakeIntReply(avg),
		}
		result = append(result, protocol.MakeMultiRawReply(eventReply))
	}

	return protocol.MakeMultiRawReply(result)
}

// execLatencyDoctor 获取延迟诊断信息
func execLatencyDoctor() redis.Reply {
	allEvents := latencyMonitor.GetAllEvents()

	var issues []string
	for eventName, events := range allEvents {
		if len(events) == 0 {
			continue
		}
		// 检查是否有高延迟事件（超过 100ms）
		for _, e := range events {
			if e.Duration > 100000 { // 100ms = 100000 微秒
				issues = append(issues, fmt.Sprintf("High latency detected for %s: %d microseconds",
					eventName, e.Duration))
				break
			}
		}
	}

	if len(issues) == 0 {
		return protocol.MakeBulkReply([]byte("No latency issues detected."))
	}

	doctorReport := "Latency issues detected:\n"
	for _, issue := range issues {
		doctorReport += issue + "\n"
	}
	return protocol.MakeBulkReply([]byte(doctorReport))
}

// execLatencyGraph 获取延迟图形表示（简化版）
func execLatencyGraph(eventName string) redis.Reply {
	events := latencyMonitor.GetEvents(eventName)
	if len(events) == 0 {
		return protocol.MakeErrReply(fmt.Sprintf("ERR No samples available for event '%s'", eventName))
	}

	// 简化图形表示
	graph := fmt.Sprintf("Latency graph for '%s' (last %d samples):\n", eventName, len(events))

	// 计算最大和最小值
	var max, min int64 = -1, -1
	for _, e := range events {
		if max == -1 || e.Duration > max {
			max = e.Duration
		}
		if min == -1 || e.Duration < min {
			min = e.Duration
		}
	}

	graph += fmt.Sprintf("max: %d us, min: %d us\n", max, min)

	// 绘制简单直方图
	for _, e := range events {
		bars := int(e.Duration * 50 / max)
		if bars > 50 {
			bars = 50
		}
		barStr := strings.Repeat("#", bars)
		graph += fmt.Sprintf("|%s %d us\n", barStr, e.Duration)
	}

	return protocol.MakeBulkReply([]byte(graph))
}

// execLatencyReset 重置指定事件
func execLatencyReset(eventNames [][]byte) redis.Reply {
	latencyMonitor.mu.Lock()
	for _, name := range eventNames {
		delete(latencyMonitor.events, string(name))
	}
	latencyMonitor.mu.Unlock()

	names := make([]string, 0, len(eventNames))
	for _, name := range eventNames {
		names = append(names, strings.ToLower(string(name)))
	}
	ResetCommandLatency(names)

	return protocol.MakeOkReply()
}

// execLatencyHistogram LATENCY HISTOGRAM [command ...] — per-command latency samples.
// RESP3: Map{cmd → Map{calls, histogram_usec→Map{bound→cum}}}；RESP2 经 Map.ToBytes 扁平化。
func execLatencyHistogram(args [][]byte) redis.Reply {
	cmdLatencyMu.Lock()
	defer cmdLatencyMu.Unlock()

	want := map[string]struct{}{}
	for _, a := range args {
		want[strings.ToLower(string(a))] = struct{}{}
	}

	outer := protocol.MakeMapReply()
	for name, h := range cmdLatencyHist {
		if len(want) > 0 {
			if _, ok := want[name]; !ok {
				continue
			}
		}
		hist := protocol.MakeMapReply()
		var cum uint64
		for i, c := range h.bucket {
			if c == 0 {
				continue
			}
			cum += c
			bound := int64(1) << uint(i)
			hist.Put(strconv.FormatInt(bound, 10), protocol.MakeIntReply(int64(cum)))
		}
		inner := protocol.MakeMapReply()
		inner.Put("calls", protocol.MakeIntReply(int64(h.count)))
		inner.Put("histogram_usec", hist)
		outer.Put(name, inner)
	}
	return outer
}

// execLatencyHelp 获取帮助信息
func execLatencyHelp() redis.Reply {
	help := []string{
		"LATENCY HISTORY <event> - Return time-latency samples for the specified event.",
		"LATENCY LATEST - Return the latest latency samples for all events.",
		"LATENCY DOCTOR - Return a human readable latency analysis report.",
		"LATENCY GRAPH <event> - Return an ASCII latency graph for the specified event.",
		"LATENCY RESET [event ...] - Reset latency data of one or more events.",
		"LATENCY HISTOGRAM [command ...] - Return latency histogram for sampled commands.",
		"LATENCY HELP - Display this help text.",
	}

	result := make([]redis.Reply, len(help))
	for i, h := range help {
		result[i] = protocol.MakeBulkReply([]byte(h))
	}
	return protocol.MakeMultiRawReply(result)
}

func init() {
	registerSpecialCommand("Latency", -2, 0).
		attachCommandExtra([]string{redisFlagAdmin, redisFlagRandom, redisFlagStale}, 0, 0, 0)
}
